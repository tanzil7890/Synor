"""Recoverable single-process, single-event-loop ledger over ``StateStore``.

The adapter deliberately does not claim multi-process compare-and-swap
or cross-event-loop semantics. One event-loop-bound lock serializes writers
that share the same store facade. Immutable events and receipts are written
before the mutable case summary, and :meth:`repair` rebuilds summaries after
an interrupted second write.
"""

from __future__ import annotations

import datetime
import hashlib
from dataclasses import dataclass
from typing import Mapping, Protocol, cast

from synor import state

from .revocation_model import (
    AssuranceLevel,
    EffectOperation,
    InvalidRevocationTransition,
    RevocationCase,
    RevocationReceipt,
    RevocationSchemaError,
    RevocationPolicyDecision,
    RevocationStage,
    VerificationOutcome,
    json_bytes,
    json_mapping,
    transition_case,
)
from .state_store_lock import state_store_writer_lock


_SCHEMA_VERSION = 1
_EVIDENCED_SUCCESS_STAGES = frozenset(
    {
        RevocationStage.VERIFIED,
        RevocationStage.RETAINED_ISOLATED,
        RevocationStage.CLOSED,
    }
)


class RevocationLedgerError(RuntimeError):
    """Base class for trustworthy ledger failures."""


class RevocationLedgerConflict(RevocationLedgerError):
    """Raised for divergent reuse of an immutable identifier or version."""


class RevocationLedgerCorruption(RevocationLedgerError):
    """Raised when the immutable event/receipt stream is inconsistent."""


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _utc_text(value: datetime.datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("ledger timestamp must be timezone-aware")
    return value.astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def _read_time(value: object) -> datetime.datetime:
    if not isinstance(value, str):
        raise RevocationLedgerCorruption("invalid ledger timestamp")
    try:
        result = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        result = None
    if result is None:
        # The parser exception can retain the raw persisted timestamp. Raise
        # after leaving the handler so corruption reports have no secret-
        # bearing implicit context.
        raise RevocationLedgerCorruption("invalid ledger timestamp") from None
    if result.tzinfo is None or result.utcoffset() is None:
        raise RevocationLedgerCorruption("invalid ledger timestamp")
    return result.astimezone(datetime.timezone.utc)


def _string_field(value: Mapping[str, object], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str):
        raise RevocationLedgerCorruption(f"invalid event {name}")
    return item


def _event_id(case: RevocationCase) -> str:
    payload = b"synor-revocation-event-v1\x00" + json_bytes(case.to_dict())
    return f"event1_{hashlib.sha256(payload).hexdigest()}"


@dataclass(frozen=True, slots=True)
class RevocationEvent:
    event_id: str
    case_id: str
    sequence: int
    occurred_at: datetime.datetime
    case: RevocationCase
    schema_version: int = _SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            not isinstance(self.schema_version, int)
            or isinstance(self.schema_version, bool)
            or self.schema_version != _SCHEMA_VERSION
        ):
            raise RevocationSchemaError("unsupported event schema major version")
        if self.event_id != _event_id(self.case):
            raise RevocationLedgerCorruption("event identifier does not match case")
        if self.case_id != self.case.case_id:
            raise RevocationLedgerCorruption("event case identifier mismatch")
        if isinstance(self.sequence, bool) or self.sequence != self.case.version:
            raise RevocationLedgerCorruption(
                "event sequence does not match case version"
            )
        object.__setattr__(
            self,
            "occurred_at",
            _read_time(_utc_text(self.occurred_at)),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "case_id": self.case_id,
            "sequence": self.sequence,
            "occurred_at": _utc_text(self.occurred_at),
            "case": self.case.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "RevocationEvent":
        schema = value.get("schema_version")
        if (
            not isinstance(schema, int)
            or isinstance(schema, bool)
            or schema != _SCHEMA_VERSION
        ):
            raise RevocationSchemaError("unsupported event schema major version")
        case_value = value.get("case")
        if not isinstance(case_value, dict):
            raise RevocationLedgerCorruption("event case snapshot is invalid")
        sequence = value.get("sequence")
        if not isinstance(sequence, int) or isinstance(sequence, bool):
            raise RevocationLedgerCorruption("event sequence is invalid")
        return cls(
            schema_version=_SCHEMA_VERSION,
            event_id=_string_field(value, "event_id"),
            case_id=_string_field(value, "case_id"),
            sequence=sequence,
            occurred_at=_read_time(value["occurred_at"]),
            case=RevocationCase.from_dict(cast(Mapping[str, object], case_value)),
        )


def _decode_event(payload: bytes, error_message: str) -> RevocationEvent:
    try:
        event = RevocationEvent.from_dict(json_mapping(payload))
    except (KeyError, ValueError):
        event = None
    if event is None:
        # Parser and Enum ValueErrors retain rejected values. Normalizing
        # outside the handler keeps raw persisted metadata out of exception
        # strings, chains, and formatted tracebacks.
        raise RevocationLedgerCorruption(error_message) from None
    return event


def _decode_case(payload: bytes, error_message: str) -> RevocationCase:
    try:
        case = RevocationCase.from_dict(json_mapping(payload))
    except (KeyError, ValueError):
        case = None
    if case is None:
        raise RevocationLedgerCorruption(error_message) from None
    return case


def _decode_receipt(payload: bytes, error_message: str) -> RevocationReceipt:
    try:
        receipt = RevocationReceipt.from_dict(json_mapping(payload))
    except (KeyError, ValueError):
        receipt = None
    if receipt is None:
        raise RevocationLedgerCorruption(error_message) from None
    return receipt


@dataclass(frozen=True, slots=True)
class LedgerRepairReport:
    cases_rebuilt: int
    events_validated: int
    receipt_heads_rebuilt: int


class RevocationLedger(Protocol):
    async def append_case(self, case: RevocationCase) -> RevocationEvent:
        """Append an immutable case transition and refresh its summary."""

    async def get_case(self, case_id: str) -> RevocationCase | None:
        """Read the derived mutable summary."""

    async def list_cases(
        self, *, stage: RevocationStage | None = None
    ) -> tuple[RevocationCase, ...]:
        """List derived case summaries."""

    async def append_receipt(self, receipt: RevocationReceipt) -> RevocationReceipt:
        """Append one hash-linked evidence receipt."""

    async def list_receipts(self, case_id: str) -> tuple[RevocationReceipt, ...]:
        """Read receipts in verified chain order."""

    async def repair(self) -> LedgerRepairReport:
        """Rebuild every case summary from immutable events."""


class StateStoreRevocationLedger:
    """Event-first local ledger for exactly one writer process."""

    def __init__(self, store: state.StateStore) -> None:
        self._store = store
        self._lock = state_store_writer_lock(store)

    @staticmethod
    def _case_key(case_id: str) -> str:
        return f"revocation/v1/cases/{case_id}.json"

    @staticmethod
    def _event_prefix(case_id: str) -> str:
        return f"revocation/v1/events/{case_id}/"

    @classmethod
    def _event_key(cls, event: RevocationEvent) -> str:
        return (
            cls._event_prefix(event.case_id)
            + f"{event.sequence:020d}-{event.event_id}.json"
        )

    @staticmethod
    def _receipt_prefix(case_id: str) -> str:
        return f"revocation/v1/receipts/{case_id}/"

    @staticmethod
    def _receipt_head_key(case_id: str) -> str:
        return f"revocation/v1/receipt_heads/{case_id}.json"

    @classmethod
    def _receipt_key(cls, receipt: RevocationReceipt) -> str:
        return cls._receipt_prefix(receipt.case_id) + f"{receipt.receipt_id}.json"

    async def _events_for_case(self, case_id: str) -> tuple[RevocationEvent, ...]:
        events: list[RevocationEvent] = []
        for key in await self._store.list(self._event_prefix(case_id)):
            payload = await self._store.get(key)
            if payload is None:
                raise RevocationLedgerCorruption(
                    "immutable event disappeared while reading"
                )
            event = _decode_event(payload, "immutable event is corrupt")
            if event.case_id != case_id:
                raise RevocationLedgerCorruption("event stored under wrong case")
            if key != self._event_key(event):
                raise RevocationLedgerCorruption(
                    "event key does not match immutable event content"
                )
            events.append(event)
        ordered = tuple(sorted(events, key=lambda event: event.sequence))
        self._validate_event_chain(ordered)
        return ordered

    @staticmethod
    def _validate_event_chain(events: tuple[RevocationEvent, ...]) -> None:
        if not events:
            return
        if events[0].sequence != 1:
            raise RevocationLedgerCorruption("case event stream does not start at one")
        if events[0].case.stage is not RevocationStage.OBSERVED:
            raise RevocationLedgerCorruption(
                "case event stream does not start at observed"
            )
        previous: RevocationEvent | None = None
        seen: set[int] = set()
        for event in events:
            if event.sequence in seen:
                raise RevocationLedgerCorruption(
                    "case event stream contains a duplicate sequence"
                )
            seen.add(event.sequence)
            if previous is not None:
                if event.sequence != previous.sequence + 1:
                    raise RevocationLedgerCorruption(
                        "case event stream contains a sequence gap"
                    )
                try:
                    expected = transition_case(
                        previous.case,
                        event.case.stage,
                        safe_error_code=event.case.safe_error_code,
                    )
                except InvalidRevocationTransition:
                    expected = None
                if expected is None:
                    raise RevocationLedgerCorruption(
                        "case event stream contains an illegal transition"
                    ) from None
                if expected != event.case:
                    raise RevocationLedgerCorruption(
                        "case event snapshot diverges from prior version"
                    )
            previous = event

    async def append_case(self, case: RevocationCase) -> RevocationEvent:
        async with self._lock:
            events = await self._events_for_case(case.case_id)
            if case.stage in _EVIDENCED_SUCCESS_STAGES:
                await self._validate_terminal_evidence(case)
            event = RevocationEvent(
                event_id=_event_id(case),
                case_id=case.case_id,
                sequence=case.version,
                occurred_at=_utc_now(),
                case=case,
            )
            event_key = self._event_key(event)
            existing_payload = await self._store.get(event_key)
            if existing_payload is not None:
                existing = _decode_event(
                    existing_payload,
                    "existing immutable event is corrupt",
                )
                if existing.case != case:
                    raise RevocationLedgerConflict(
                        "event identifier was reused with different content"
                    )
                # A delayed retry of an older immutable event must never
                # regress the mutable summary below the stream tip.
                summary = events[-1].case if events else case
                await self._store.put(
                    self._case_key(case.case_id),
                    json_bytes(summary.to_dict()),
                )
                return existing

            if not events:
                if case.version != 1 or case.stage is not RevocationStage.OBSERVED:
                    raise RevocationLedgerConflict(
                        "a new case must begin at observed version one"
                    )
            else:
                latest = events[-1]
                if case.version != latest.sequence + 1:
                    raise RevocationLedgerConflict(
                        "case version is not the next immutable sequence"
                    )
                try:
                    expected = transition_case(
                        latest.case,
                        case.stage,
                        safe_error_code=case.safe_error_code,
                    )
                except InvalidRevocationTransition:
                    expected = None
                if expected is None:
                    raise RevocationLedgerConflict(
                        "case transition is not legal"
                    ) from None
                if expected != case:
                    raise RevocationLedgerConflict(
                        "case transition changed immutable case attributes"
                    )

            # Event first; summary second.  A crash between these writes is
            # repaired by retrying this method or by calling repair().
            await self._store.put(event_key, json_bytes(event.to_dict()))
            await self._store.put(
                self._case_key(case.case_id),
                json_bytes(case.to_dict()),
            )
            return event

    async def get_case(self, case_id: str) -> RevocationCase | None:
        payload = await self._store.get(self._case_key(case_id))
        if payload is None:
            if await self._events_for_case(case_id):
                raise RevocationLedgerCorruption(
                    "case summary is missing behind its immutable event stream"
                )
            return None
        case = _decode_case(payload, "case summary is corrupt")
        if case.case_id != case_id:
            raise RevocationLedgerCorruption("case summary stored under wrong case")
        events = await self._events_for_case(case_id)
        if not events or events[-1].case != case:
            raise RevocationLedgerCorruption(
                "case summary does not match the immutable event-stream tip"
            )
        if (
            case.stage in _EVIDENCED_SUCCESS_STAGES
            and not await self._case_has_terminal_evidence(case)
        ):
            raise RevocationLedgerCorruption(
                "successful case lacks terminal evidence for every obligation"
            )
        return case

    async def list_cases(
        self, *, stage: RevocationStage | None = None
    ) -> tuple[RevocationCase, ...]:
        cases: list[RevocationCase] = []
        for key in await self._store.list("revocation/v1/cases/"):
            payload = await self._store.get(key)
            if payload is None:
                continue
            case = _decode_case(payload, "case summary is corrupt")
            if key != self._case_key(case.case_id):
                raise RevocationLedgerCorruption(
                    "case summary key does not match its content"
                )
            events = await self._events_for_case(case.case_id)
            if not events or events[-1].case != case:
                raise RevocationLedgerCorruption(
                    "case summary does not match the immutable event-stream tip"
                )
            if (
                case.stage in _EVIDENCED_SUCCESS_STAGES
                and not await self._case_has_terminal_evidence(case)
            ):
                raise RevocationLedgerCorruption(
                    "successful case lacks terminal evidence for every obligation"
                )
            if stage is None or case.stage is stage:
                cases.append(case)
        return tuple(sorted(cases, key=lambda case: (case.observed_at, case.case_id)))

    async def _receipt_chain(
        self,
        case_id: str,
        *,
        verify_head: bool = True,
    ) -> tuple[RevocationReceipt, ...]:
        receipts: list[RevocationReceipt] = []
        for key in await self._store.list(self._receipt_prefix(case_id)):
            payload = await self._store.get(key)
            if payload is None:
                raise RevocationLedgerCorruption(
                    "immutable receipt disappeared while reading"
                )
            receipt = _decode_receipt(payload, "immutable receipt is corrupt")
            if receipt.case_id != case_id:
                raise RevocationLedgerCorruption("receipt stored under wrong case")
            if key != self._receipt_key(receipt):
                raise RevocationLedgerCorruption(
                    "receipt key does not match immutable receipt content"
                )
            receipts.append(receipt)
        chain = self._order_receipt_chain(receipts)
        if verify_head:
            await self._verify_receipt_head(case_id, chain)
        return chain

    @staticmethod
    def _receipt_head(
        chain: tuple[RevocationReceipt, ...],
    ) -> dict[str, object]:
        if not chain:
            raise ValueError("an empty receipt chain has no durable head")
        return {
            "schema_version": _SCHEMA_VERSION,
            "count": len(chain),
            "tip_digest": chain[-1].evidence_digest(),
        }

    async def _read_receipt_head(self, case_id: str) -> tuple[int, str] | None:
        payload = await self._store.get(self._receipt_head_key(case_id))
        if payload is None:
            return None
        try:
            value = json_mapping(payload)
        except RevocationSchemaError:
            value = None
        if value is None:
            raise RevocationLedgerCorruption("receipt head is corrupt") from None
        schema = value.get("schema_version")
        if (
            not isinstance(schema, int)
            or isinstance(schema, bool)
            or schema != _SCHEMA_VERSION
        ):
            raise RevocationLedgerCorruption("receipt head schema is unsupported")
        count = value.get("count")
        tip_digest = value.get("tip_digest")
        if (
            not isinstance(count, int)
            or isinstance(count, bool)
            or count < 1
            or not isinstance(tip_digest, str)
            or len(tip_digest) != 64
            or any(char not in "0123456789abcdef" for char in tip_digest)
        ):
            raise RevocationLedgerCorruption("receipt head is corrupt")
        return count, tip_digest

    async def _verify_receipt_head(
        self,
        case_id: str,
        chain: tuple[RevocationReceipt, ...],
    ) -> None:
        head = await self._read_receipt_head(case_id)
        if not chain:
            if head is not None:
                raise RevocationLedgerCorruption(
                    "receipt head exists without immutable receipts"
                )
            return
        if head is None:
            raise RevocationLedgerCorruption(
                "receipt chain is missing its durable head"
            )
        expected = (len(chain), chain[-1].evidence_digest())
        if head != expected:
            raise RevocationLedgerCorruption(
                "receipt chain does not match its durable head"
            )

    @staticmethod
    def _validate_receipt_head_prefix(
        head: tuple[int, str] | None,
        chain: tuple[RevocationReceipt, ...],
    ) -> None:
        if head is None:
            return
        count, tip_digest = head
        if count > len(chain):
            raise RevocationLedgerCorruption("durable receipt head proves receipt loss")
        if chain[count - 1].evidence_digest() != tip_digest:
            raise RevocationLedgerCorruption(
                "durable receipt head diverges from the receipt chain"
            )

    @staticmethod
    def _order_receipt_chain(
        receipts: list[RevocationReceipt],
    ) -> tuple[RevocationReceipt, ...]:
        if not receipts:
            return ()
        by_previous: dict[str | None, list[RevocationReceipt]] = {}
        for receipt in receipts:
            by_previous.setdefault(receipt.previous_receipt_digest, []).append(receipt)
        roots = by_previous.get(None, [])
        if len(roots) != 1:
            raise RevocationLedgerCorruption(
                "receipt chain must contain exactly one root"
            )
        ordered: list[RevocationReceipt] = []
        current = roots[0]
        seen: set[str] = set()
        while True:
            digest = current.evidence_digest()
            if digest in seen:
                raise RevocationLedgerCorruption("receipt chain contains a cycle")
            seen.add(digest)
            ordered.append(current)
            children = by_previous.get(digest, [])
            if not children:
                break
            if len(children) != 1:
                raise RevocationLedgerCorruption("receipt chain forks")
            current = children[0]
        if len(ordered) != len(receipts):
            raise RevocationLedgerCorruption(
                "receipt chain contains a missing or reordered link"
            )
        return tuple(ordered)

    async def append_receipt(self, receipt: RevocationReceipt) -> RevocationReceipt:
        async with self._lock:
            chain = await self._receipt_chain(receipt.case_id, verify_head=False)
            current_head = await self._read_receipt_head(receipt.case_id)
            if not chain and current_head is not None:
                raise RevocationLedgerCorruption(
                    "receipt head exists without immutable receipts"
                )
            if chain and current_head is not None:
                self._validate_receipt_head_prefix(current_head, chain)
            events = await self._events_for_case(receipt.case_id)
            if not events:
                raise RevocationLedgerConflict(
                    "receipt requires an existing immutable revocation case"
                )
            key = self._receipt_key(receipt)
            existing_payload = await self._store.get(key)
            if existing_payload is not None:
                existing = _decode_receipt(
                    existing_payload,
                    "existing immutable receipt is corrupt",
                )
                if existing != receipt:
                    raise RevocationLedgerConflict(
                        "receipt identifier was reused with different content"
                    )
                await self._store.put(
                    self._receipt_head_key(receipt.case_id),
                    json_bytes(self._receipt_head(chain)),
                )
                return existing

            expected_previous = chain[-1].evidence_digest() if chain else None
            if receipt.previous_receipt_digest != expected_previous:
                raise RevocationLedgerConflict(
                    "receipt does not extend the current hash-chain tip"
                )
            case = events[-1].case
            if not self._receipt_matches_case(receipt, case):
                raise RevocationLedgerConflict(
                    "receipt does not match its revocation case obligation"
                )
            await self._store.put(key, receipt.canonical_bytes())
            await self._store.put(
                self._receipt_head_key(receipt.case_id),
                json_bytes(self._receipt_head((*chain, receipt))),
            )
            return receipt

    async def list_receipts(self, case_id: str) -> tuple[RevocationReceipt, ...]:
        return await self._receipt_chain(case_id)

    async def _validate_terminal_evidence(self, case: RevocationCase) -> None:
        if not await self._case_has_terminal_evidence(case):
            raise RevocationLedgerConflict(
                "successful case transition lacks terminal evidence for every "
                "target obligation"
            )

    async def _case_has_terminal_evidence(self, case: RevocationCase) -> bool:
        if not case.expected_targets:
            return False
        if case.stage is RevocationStage.VERIFIED and case.policy_decision not in {
            RevocationPolicyDecision.DESTROY,
            RevocationPolicyDecision.RESTRICT,
        }:
            return False
        if (
            case.stage is RevocationStage.RETAINED_ISOLATED
            and case.policy_decision is not RevocationPolicyDecision.PRESERVE_ON_HOLD
        ):
            return False
        receipts = await self._receipt_chain(case.case_id)
        latest_satisfaction: dict[str, bool] = {}
        for receipt in receipts:
            if not self._receipt_matches_case(receipt, case):
                raise RevocationLedgerCorruption(
                    "persisted receipt does not match its revocation case"
                )
            assurance = AssuranceLevel(receipt.assurance_level)
            outcome = VerificationOutcome(receipt.observed_outcome)
            stage = RevocationStage(receipt.stage)
            operation = EffectOperation(receipt.operation_kind)
            query_verified = (
                operation is EffectOperation.DELETE
                and case.policy_decision
                in {
                    RevocationPolicyDecision.DESTROY,
                    RevocationPolicyDecision.RESTRICT,
                }
                and assurance
                in {
                    AssuranceLevel.QUERY_VERIFIED,
                    AssuranceLevel.ERASURE_ATTESTED,
                }
                and outcome is VerificationOutcome.ABSENT
                and stage is RevocationStage.VERIFIED
            )
            retained_isolated = (
                operation is EffectOperation.ISOLATE
                and case.policy_decision is RevocationPolicyDecision.PRESERVE_ON_HOLD
                and assurance is AssuranceLevel.RETAINED_ISOLATED
                and outcome is VerificationOutcome.RETAINED_ISOLATED
                and stage is RevocationStage.RETAINED_ISOLATED
            )
            latest_satisfaction[receipt.obligation_id] = (
                query_verified or retained_isolated
            )
        return all(
            latest_satisfaction.get(obligation_id, False)
            for obligation_id in case.expected_targets
        )

    @staticmethod
    def _receipt_matches_case(
        receipt: RevocationReceipt,
        case: RevocationCase,
    ) -> bool:
        allowed_operations = {
            RevocationPolicyDecision.DESTROY: frozenset({EffectOperation.DELETE}),
            RevocationPolicyDecision.RESTRICT: frozenset(
                {EffectOperation.RESTRICT, EffectOperation.DELETE}
            ),
            RevocationPolicyDecision.PRESERVE_ON_HOLD: frozenset(
                {EffectOperation.ISOLATE}
            ),
            RevocationPolicyDecision.INVESTIGATE_AMBIGUOUS: frozenset(),
        }
        return (
            receipt.case_id == case.case_id
            and receipt.source_digest == case.source_digest
            and receipt.reason == case.reason.value
            and receipt.policy_decision == case.policy_decision.value
            and receipt.obligation_id in case.expected_targets
            and EffectOperation(receipt.operation_kind)
            in allowed_operations[case.policy_decision]
        )

    async def repair(self) -> LedgerRepairReport:
        async with self._lock:
            event_case_ids: set[str] = set()
            for key in await self._store.list("revocation/v1/events/"):
                parts = key.split("/")
                if len(parts) != 5:
                    raise RevocationLedgerCorruption("invalid immutable event key")
                event_case_ids.add(parts[3])

            summary_case_ids: set[str] = set()
            for key in await self._store.list("revocation/v1/cases/"):
                parts = key.split("/")
                if (
                    len(parts) != 4
                    or not parts[3].endswith(".json")
                    or not parts[3][:-5]
                ):
                    raise RevocationLedgerCorruption("invalid case summary key")
                case_id = parts[3][:-5]
                if key != self._case_key(case_id):
                    raise RevocationLedgerCorruption("invalid case summary key")
                summary_case_ids.add(case_id)

            orphan_summaries = summary_case_ids - event_case_ids
            if orphan_summaries:
                raise RevocationLedgerCorruption(
                    "case summary has no immutable event stream"
                )
            case_ids = event_case_ids | summary_case_ids

            rebuilt = 0
            validated = 0
            receipt_heads_rebuilt = 0
            for case_id in sorted(case_ids):
                events = await self._events_for_case(case_id)
                if not events:
                    raise RevocationLedgerCorruption(
                        "case has no immutable event stream"
                    )
                validated += len(events)
                latest = events[-1].case
                if (
                    latest.stage in _EVIDENCED_SUCCESS_STAGES
                    and not await self._case_has_terminal_evidence(latest)
                ):
                    raise RevocationLedgerCorruption(
                        "successful event stream lacks terminal obligation evidence"
                    )
                try:
                    existing = await self.get_case(case_id)
                except RevocationLedgerCorruption:
                    existing = None
                if existing != latest:
                    await self._store.put(
                        self._case_key(case_id),
                        json_bytes(latest.to_dict()),
                    )
                    rebuilt += 1

            receipt_case_ids: set[str] = set()
            for key in await self._store.list("revocation/v1/receipts/"):
                parts = key.split("/")
                if len(parts) != 5:
                    raise RevocationLedgerCorruption("invalid immutable receipt key")
                receipt_case_ids.add(parts[3])
            for key in await self._store.list("revocation/v1/receipt_heads/"):
                parts = key.split("/")
                if (
                    len(parts) != 4
                    or not parts[3].endswith(".json")
                    or not parts[3][:-5]
                ):
                    raise RevocationLedgerCorruption("invalid receipt head key")
                case_id = parts[3][:-5]
                if key != self._receipt_head_key(case_id):
                    raise RevocationLedgerCorruption("invalid receipt head key")
                receipt_case_ids.add(case_id)
            for case_id in sorted(receipt_case_ids):
                if case_id not in case_ids:
                    raise RevocationLedgerCorruption(
                        "receipt stream has no immutable revocation case"
                    )
                chain = await self._receipt_chain(case_id, verify_head=False)
                if not chain:
                    raise RevocationLedgerCorruption(
                        "receipt head exists without immutable receipts"
                    )
                expected = (len(chain), chain[-1].evidence_digest())
                current = await self._read_receipt_head(case_id)
                self._validate_receipt_head_prefix(current, chain)
                if current is None or current[0] < expected[0]:
                    await self._store.put(
                        self._receipt_head_key(case_id),
                        json_bytes(self._receipt_head(chain)),
                    )
                    receipt_heads_rebuilt += 1
                elif current != expected:
                    raise RevocationLedgerCorruption(
                        "receipt loss or head divergence cannot be repaired"
                    )
            return LedgerRepairReport(
                cases_rebuilt=rebuilt,
                events_validated=validated,
                receipt_heads_rebuilt=receipt_heads_rebuilt,
            )
