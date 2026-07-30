"""Monotonic fail-closed serving suppression over a :class:`StateStore`."""

from __future__ import annotations

import asyncio
import datetime
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from synor import state

from .revocation_model import RevocationSchemaError, json_bytes, json_mapping
from .state_store_lock import state_store_serving_fence, state_store_writer_lock


_SCHEMA_VERSION = 1
_EPOCH_KEY = "revocation/v1/suppression_epoch.json"
_SERVING_FENCE_PREFIX = "revocation/v1/serving_fences/"


class SuppressionCorruptionError(ValueError):
    """Raised when a stored suppression record cannot be trusted."""


class SuppressionGenerationConflict(ValueError):
    """Raised when one generation is reused for different decisions."""


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _utc_text(value: datetime.datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("suppression timestamp must be timezone-aware")
    return value.astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _read_time(value: object) -> datetime.datetime:
    if not isinstance(value, str):
        raise SuppressionCorruptionError("invalid suppression timestamp")
    result: datetime.datetime | None = None
    try:
        result = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        pass
    if result is None:
        raise SuppressionCorruptionError("invalid suppression timestamp") from None
    if result.tzinfo is None or result.utcoffset() is None:
        raise SuppressionCorruptionError("invalid suppression timestamp")
    return result.astimezone(datetime.timezone.utc)


def _validate_digest(name: str, value: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")


def _validate_safe_token(name: str, value: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 256
        or any(
            not (char.isascii() and (char.isalnum() or char in {"-", "_", ".", ":"}))
            for char in value
        )
    ):
        raise ValueError(f"{name} must be an opaque safe token")


def _string_field(value: Mapping[str, object], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str):
        raise SuppressionCorruptionError(f"invalid suppression {name}")
    return item


def _optional_string_field(value: Mapping[str, object], name: str) -> str | None:
    item = value.get(name)
    if item is None:
        return None
    if not isinstance(item, str):
        raise SuppressionCorruptionError(f"invalid suppression {name}")
    return item


@dataclass(frozen=True, slots=True)
class SuppressionRecord:
    source_digest: str
    tenant_digest: str
    policy_id: str
    generation: int
    suppressed: bool
    policy_revision: str
    group_graph_revision: str
    reason: str
    case_id: str | None
    observed_at: datetime.datetime
    verified_authorization: bool
    schema_version: int = _SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            not isinstance(self.schema_version, int)
            or isinstance(self.schema_version, bool)
            or self.schema_version != _SCHEMA_VERSION
        ):
            raise RevocationSchemaError("unsupported suppression schema major version")
        _validate_digest("source_digest", self.source_digest)
        _validate_digest("tenant_digest", self.tenant_digest)
        _validate_safe_token("policy_id", self.policy_id)
        if (
            not isinstance(self.generation, int)
            or isinstance(self.generation, bool)
            or self.generation < 1
        ):
            raise ValueError("suppression generation must be positive")
        if type(self.suppressed) is not bool:
            raise TypeError("suppressed must be a bool")
        if type(self.verified_authorization) is not bool:
            raise TypeError("verified_authorization must be a bool")
        _validate_safe_token("policy_revision", self.policy_revision)
        _validate_safe_token("group_graph_revision", self.group_graph_revision)
        _validate_safe_token("reason", self.reason)
        if self.case_id is not None:
            _validate_safe_token("case_id", self.case_id)
        if not self.suppressed and not self.verified_authorization:
            raise ValueError(
                "lifting suppression requires verified newer authorization"
            )
        object.__setattr__(
            self,
            "observed_at",
            _read_time(_utc_text(self.observed_at)),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "source_digest": self.source_digest,
            "tenant_digest": self.tenant_digest,
            "policy_id": self.policy_id,
            "generation": self.generation,
            "suppressed": self.suppressed,
            "policy_revision": self.policy_revision,
            "group_graph_revision": self.group_graph_revision,
            "reason": self.reason,
            "case_id": self.case_id,
            "observed_at": _utc_text(self.observed_at),
            "verified_authorization": self.verified_authorization,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "SuppressionRecord":
        schema = value.get("schema_version")
        if (
            not isinstance(schema, int)
            or isinstance(schema, bool)
            or schema != _SCHEMA_VERSION
        ):
            raise RevocationSchemaError("unsupported suppression schema major version")
        record: SuppressionRecord | None = None
        try:
            generation = value["generation"]
            suppressed = value["suppressed"]
            verified = value["verified_authorization"]
            if (
                not isinstance(generation, int)
                or isinstance(generation, bool)
                or not isinstance(suppressed, bool)
                or not isinstance(verified, bool)
            ):
                raise SuppressionCorruptionError(
                    "invalid suppression generation or decision"
                )
            record = cls(
                schema_version=_SCHEMA_VERSION,
                source_digest=_string_field(value, "source_digest"),
                tenant_digest=_string_field(value, "tenant_digest"),
                policy_id=_string_field(value, "policy_id"),
                generation=generation,
                suppressed=suppressed,
                policy_revision=_string_field(value, "policy_revision"),
                group_graph_revision=_string_field(value, "group_graph_revision"),
                reason=_string_field(value, "reason"),
                case_id=_optional_string_field(value, "case_id"),
                observed_at=_read_time(value["observed_at"]),
                verified_authorization=verified,
            )
        except (KeyError, TypeError, ValueError):
            pass
        if record is None:
            raise SuppressionCorruptionError("invalid suppression record") from None
        return record


@dataclass(frozen=True, slots=True)
class SuppressionSnapshot:
    """One lock-linearized view of suppression state.

    ``epoch`` advances for every accepted generation written through a
    :class:`StateStoreSuppressionIndex`.  Callers must still compare the
    records in two snapshots: the store replaces records and the epoch in two
    atomic key writes, so record comparison keeps reads fail-closed if a
    process stops between those writes.
    """

    epoch: int
    records: Mapping[str, SuppressionRecord | None]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.epoch, int)
            or isinstance(self.epoch, bool)
            or self.epoch < 0
        ):
            raise SuppressionCorruptionError("invalid suppression epoch")
        records = dict(self.records)
        for source_digest, record in records.items():
            invalid_key = False
            try:
                _validate_digest("source_digest", source_digest)
            except (TypeError, ValueError):
                invalid_key = True
            if invalid_key:
                raise SuppressionCorruptionError(
                    "invalid suppression snapshot key"
                ) from None
            if record is not None and (
                not isinstance(record, SuppressionRecord)
                or record.source_digest != source_digest
            ):
                raise SuppressionCorruptionError(
                    "suppression snapshot record does not match its key"
                )
        object.__setattr__(self, "records", MappingProxyType(records))


class StateStoreSuppressionIndex:
    """Single-process monotonic suppression registry.

    Missing records fail closed.  A strict ingestion flow must write an
    explicit, verified authorization generation before a candidate can be
    served.
    """

    def __init__(self, store: state.StateStore) -> None:
        self._store = store
        self._lock = state_store_writer_lock(store)
        self._serving_fence = state_store_serving_fence(store)

    @staticmethod
    def _key(source_digest: str) -> str:
        _validate_digest("source_digest", source_digest)
        return f"revocation/v1/suppression/{source_digest}.json"

    @staticmethod
    def _serving_fence_key(source_digest: str) -> str:
        _validate_digest("source_digest", source_digest)
        return f"{_SERVING_FENCE_PREFIX}{source_digest}.json"

    @staticmethod
    def _record_from_payload(
        payload: bytes,
        *,
        expected_source_digest: str,
    ) -> SuppressionRecord:
        record: SuppressionRecord | None = None
        try:
            record = SuppressionRecord.from_dict(json_mapping(payload))
        except (RevocationSchemaError, ValueError):
            pass
        if record is None:
            raise SuppressionCorruptionError(
                "stored suppression state is corrupt"
            ) from None
        if record.source_digest != expected_source_digest:
            raise SuppressionCorruptionError(
                "stored suppression record does not match its storage key"
            )
        return record

    async def _get_unlocked(self, source_digest: str) -> SuppressionRecord | None:
        payload = await self._store.get(self._key(source_digest))
        if payload is None:
            return None
        return self._record_from_payload(
            payload,
            expected_source_digest=source_digest,
        )

    @staticmethod
    def _serving_fence_from_payload(
        payload: bytes,
        *,
        expected_source_digest: str,
    ) -> SuppressionRecord:
        fence: SuppressionRecord | None = None
        try:
            fence = SuppressionRecord.from_dict(json_mapping(payload))
        except (RevocationSchemaError, TypeError, ValueError):
            pass
        if fence is None:
            raise SuppressionCorruptionError(
                "stored serving fence is corrupt"
            ) from None
        if fence.source_digest != expected_source_digest:
            raise SuppressionCorruptionError(
                "stored serving fence does not match its storage key"
            )
        if not fence.suppressed or fence.verified_authorization:
            raise SuppressionCorruptionError(
                "stored serving fence is not a pending suppression"
            )
        return fence

    async def _get_durable_fence_unlocked(
        self,
        source_digest: str,
    ) -> SuppressionRecord | None:
        payload = await self._store.get(self._serving_fence_key(source_digest))
        if payload is None:
            return None
        return self._serving_fence_from_payload(
            payload,
            expected_source_digest=source_digest,
        )

    @staticmethod
    def _durable_fence_blocks(
        fence: SuppressionRecord | None,
    ) -> bool:
        return fence is not None

    @staticmethod
    def _record_releases_fence(
        record: SuppressionRecord,
        fence: SuppressionRecord,
    ) -> bool:
        return record.generation > fence.generation or record == fence

    async def _clear_durable_fence_for_record_unlocked(
        self,
        record: SuppressionRecord,
    ) -> None:
        fence = await self._get_durable_fence_unlocked(record.source_digest)
        if fence is not None and self._record_releases_fence(record, fence):
            deleted = await self._store.delete(
                self._serving_fence_key(record.source_digest)
            )
            if not deleted:
                raise SuppressionCorruptionError(
                    "durable serving fence changed during reconciliation"
                )

    async def get(self, source_digest: str) -> SuppressionRecord | None:
        self._key(source_digest)
        if self._serving_fence.contains(source_digest):
            return None
        async with self._lock:
            record = await self._get_unlocked(source_digest)
            fence = await self._get_durable_fence_unlocked(source_digest)
            if self._durable_fence_blocks(fence):
                record = None
        if self._serving_fence.contains(source_digest):
            return None
        return record

    async def _epoch_unlocked(self) -> int:
        payload = await self._store.get(_EPOCH_KEY)
        if payload is None:
            return 0
        epoch: int | None = None
        try:
            value = json_mapping(payload)
            schema = value.get("schema_version")
            raw_epoch = value.get("epoch")
            if (
                not isinstance(schema, int)
                or isinstance(schema, bool)
                or schema != _SCHEMA_VERSION
                or not isinstance(raw_epoch, int)
                or isinstance(raw_epoch, bool)
                or raw_epoch < 0
            ):
                raise SuppressionCorruptionError("invalid suppression epoch")
            epoch = raw_epoch
        except (RevocationSchemaError, TypeError, ValueError):
            pass
        if epoch is None:
            raise SuppressionCorruptionError(
                "stored suppression epoch is corrupt"
            ) from None
        return epoch

    async def current_epoch(self) -> int:
        """Return the current monotonic epoch at a writer-lock boundary."""

        async with self._lock:
            return await self._epoch_unlocked()

    async def snapshot_many(
        self, source_digests: tuple[str, ...]
    ) -> SuppressionSnapshot:
        """Read records and their epoch at one event-loop linearization point."""

        unique_source_digests = tuple(dict.fromkeys(source_digests))
        async with self._lock:
            epoch = await self._epoch_unlocked()
            records = {
                source_digest: await self._get_unlocked(source_digest)
                for source_digest in unique_source_digests
            }
            for source_digest in unique_source_digests:
                fence = await self._get_durable_fence_unlocked(source_digest)
                if self._serving_fence.contains(
                    source_digest
                ) or self._durable_fence_blocks(fence):
                    records[source_digest] = None
        return SuppressionSnapshot(epoch=epoch, records=records)

    async def put(self, record: SuppressionRecord) -> SuppressionRecord:
        """Persist a newer generation, ignoring stale delivery idempotently."""

        if record.suppressed:
            self._serving_fence.fail_closed(
                record.source_digest,
                record.generation,
            )
        accepted = record
        should_write = True
        async with self._lock:
            current = await self._get_unlocked(record.source_digest)
            if current is not None:
                if record.tenant_digest != current.tenant_digest:
                    raise SuppressionGenerationConflict(
                        "one source identity cannot move between tenants"
                    )
                if record.generation < current.generation:
                    accepted = current
                    should_write = False
                if record.generation == current.generation:
                    if (
                        record.source_digest == current.source_digest
                        and record.tenant_digest == current.tenant_digest
                        and record.policy_id == current.policy_id
                        and record.suppressed == current.suppressed
                        and record.policy_revision == current.policy_revision
                        and record.group_graph_revision == current.group_graph_revision
                        and record.reason == current.reason
                        and record.case_id == current.case_id
                        and record.verified_authorization
                        == current.verified_authorization
                    ):
                        accepted = current
                        should_write = False
                    else:
                        raise SuppressionGenerationConflict(
                            "one suppression generation has conflicting decisions"
                        )
            if should_write:
                epoch = await self._epoch_unlocked()
                await self._store.put(
                    self._key(record.source_digest),
                    json_bytes(record.to_dict()),
                )
                await self._store.put(
                    _EPOCH_KEY,
                    json_bytes(
                        {
                            "schema_version": _SCHEMA_VERSION,
                            "epoch": epoch + 1,
                        }
                    ),
                )
            await self._clear_durable_fence_for_record_unlocked(accepted)
            remaining_fence = await self._get_durable_fence_unlocked(
                record.source_digest
            )
        pending_generation = self._serving_fence.pending_generation(
            record.source_digest
        )
        if (
            remaining_fence is None
            and pending_generation is not None
            and (
                accepted.generation > pending_generation
                or (accepted.generation == pending_generation and accepted.suppressed)
            )
        ):
            self._serving_fence.clear_through(
                record.source_digest,
                accepted.generation,
            )
        return accepted

    def fail_closed(self, source_digest: str, generation: int) -> None:
        """Deny serving synchronously before durable suppression I/O begins."""

        self._key(source_digest)
        if (
            not isinstance(generation, int)
            or isinstance(generation, bool)
            or generation < 1
        ):
            raise ValueError("suppression generation must be positive")
        self._serving_fence.fail_closed(source_digest, generation)

    async def persist_fail_closed(
        self,
        *,
        source_digest: str,
        tenant_digest: str,
        policy_id: str,
        generation: int,
        policy_revision: str,
        group_graph_revision: str,
        reason: str,
        case_id: str,
        observed_at: datetime.datetime,
    ) -> SuppressionRecord:
        """Persist a monotonic denial before accepting a revocation observation."""

        self.fail_closed(source_digest, generation)
        candidate = SuppressionRecord(
            source_digest=source_digest,
            tenant_digest=tenant_digest,
            policy_id=policy_id,
            generation=generation,
            suppressed=True,
            policy_revision=policy_revision,
            group_graph_revision=group_graph_revision,
            reason=reason,
            case_id=case_id,
            observed_at=observed_at,
            verified_authorization=False,
        )
        async with self._lock:
            current = await self._get_durable_fence_unlocked(source_digest)
            if current is None:
                await self._store.put(
                    self._serving_fence_key(source_digest),
                    json_bytes(candidate.to_dict()),
                )
                return candidate
            if current.tenant_digest != candidate.tenant_digest:
                raise SuppressionGenerationConflict(
                    "one source identity cannot move between tenants"
                )
            if candidate.generation < current.generation:
                return current
            if candidate.generation == current.generation:
                if candidate == current:
                    return current
                raise SuppressionGenerationConflict(
                    "one serving-fence generation has conflicting decisions"
                )
            await self._store.put(
                self._serving_fence_key(source_digest),
                json_bytes(candidate.to_dict()),
            )
            return candidate

    async def durable_state(
        self,
        source_digest: str,
    ) -> SuppressionRecord | None:
        """Read durable state without consulting or clearing the serving fence."""

        async with self._lock:
            return await self._get_unlocked(source_digest)

    async def clear_fail_closed(self, source_digest: str) -> None:
        """Clear a pending fence through semantically confirmed durable state."""

        self._key(source_digest)
        async with self._lock:
            current = await self._get_unlocked(source_digest)
            if current is None:
                return
            await self._clear_durable_fence_for_record_unlocked(current)
            remaining_fence = await self._get_durable_fence_unlocked(source_digest)
        pending_generation = self._serving_fence.pending_generation(source_digest)
        if (
            remaining_fence is None
            and pending_generation is not None
            and (
                current.generation > pending_generation
                or (current.generation == pending_generation and current.suppressed)
            )
        ):
            self._serving_fence.clear_through(
                source_digest,
                current.generation,
            )

    async def suppress(
        self,
        *,
        source_digest: str,
        tenant_digest: str,
        policy_id: str,
        generation: int,
        policy_revision: str,
        group_graph_revision: str,
        reason: str,
        case_id: str,
        observed_at: datetime.datetime | None = None,
    ) -> SuppressionRecord:
        return await self.put(
            SuppressionRecord(
                source_digest=source_digest,
                tenant_digest=tenant_digest,
                policy_id=policy_id,
                generation=generation,
                suppressed=True,
                policy_revision=policy_revision,
                group_graph_revision=group_graph_revision,
                reason=reason,
                case_id=case_id,
                observed_at=observed_at or _utc_now(),
                verified_authorization=False,
            )
        )

    async def authorize(
        self,
        *,
        source_digest: str,
        tenant_digest: str,
        policy_id: str,
        generation: int,
        policy_revision: str,
        group_graph_revision: str,
        observed_at: datetime.datetime | None = None,
    ) -> SuppressionRecord:
        """Supersede old suppression only after authorization was verified."""

        return await self.put(
            SuppressionRecord(
                source_digest=source_digest,
                tenant_digest=tenant_digest,
                policy_id=policy_id,
                generation=generation,
                suppressed=False,
                policy_revision=policy_revision,
                group_graph_revision=group_graph_revision,
                reason="authorization_verified",
                case_id=None,
                observed_at=observed_at or _utc_now(),
                verified_authorization=True,
            )
        )

    async def is_suppressed(self, source_digest: str) -> bool:
        record = await self.get(source_digest)
        return record is None or record.suppressed

    async def is_suppressed_many(
        self, source_digests: tuple[str, ...]
    ) -> Mapping[str, bool]:
        """Batch lookup contract consumed by the strict retrieval guard."""

        results = await asyncio.gather(
            *(self.get(source_digest) for source_digest in source_digests)
        )
        return {
            source_digest: record is None or record.suppressed
            for source_digest, record in zip(source_digests, results, strict=True)
        }

    async def records(self) -> tuple[SuppressionRecord, ...]:
        records: list[SuppressionRecord] = []
        for key in await self._store.list("revocation/v1/suppression/"):
            payload = await self._store.get(key)
            if payload is None:
                continue
            invalid_record = False
            try:
                prefix = "revocation/v1/suppression/"
                suffix = ".json"
                if not key.startswith(prefix) or not key.endswith(suffix):
                    raise SuppressionCorruptionError("invalid suppression storage key")
                source_digest = key[len(prefix) : -len(suffix)]
                if self._key(source_digest) != key:
                    raise SuppressionCorruptionError("invalid suppression storage key")
                records.append(
                    self._record_from_payload(
                        payload,
                        expected_source_digest=source_digest,
                    )
                )
            except (RevocationSchemaError, ValueError):
                invalid_record = True
            if invalid_record:
                raise SuppressionCorruptionError(
                    "stored suppression state is corrupt"
                ) from None
        return tuple(
            sorted(
                records,
                key=lambda record: (record.source_digest, record.generation),
            )
        )


SuppressionIndex = StateStoreSuppressionIndex
