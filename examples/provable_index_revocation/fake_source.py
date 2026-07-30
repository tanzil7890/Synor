"""Deterministic governed source fixtures for the local flagship demo."""

from __future__ import annotations

import dataclasses
import hashlib
import uuid

from synor import governance

CONTENT_SENTINEL = "NEBULA-ROADMAP-ALPHA-CONTENT-MUST-NOT-ENTER-EVIDENCE"
CONTROL_CONTENT_SENTINEL = "ORCHARD-CONTROL-DOCUMENT-MUST-NOT-ENTER-EVIDENCE"
PRINCIPAL_ALPHA = "alice@tenant-alpha.example"
PRINCIPAL_BETA = "bob@tenant-beta.example"


def digest(value: str | bytes) -> str:
    payload = value.encode("utf-8") if isinstance(value, str) else value
    return hashlib.sha256(payload).hexdigest()


@dataclasses.dataclass(frozen=True, slots=True)
class DemoDocument:
    identity: governance.SourceIdentity
    tenant_id: str
    policy_id: str
    principal_id: str
    content: bytes

    @property
    def source_revision(self) -> str:
        return f"content-{digest(self.content)}"

    def access(
        self,
        *,
        policy_revision: str,
        allowed: bool,
    ) -> governance.AccessSnapshot:
        rules: tuple[governance.AccessRule, ...] = (
            (
                governance.AccessRule(
                    effect=governance.AccessEffect.GRANT,
                    subject_type="user",
                    subject_id=self.principal_id,
                    role="reader",
                ),
            )
            if allowed
            else ()
        )
        return governance.AccessSnapshot(
            tenant_id=self.tenant_id,
            policy_id=self.policy_id,
            policy_revision=policy_revision,
            policy_digest=governance.canonical_access_digest(
                tenant_id=self.tenant_id,
                policy_id=self.policy_id,
                policy_revision=policy_revision,
                rules=rules,
            ),
            group_graph_revision="groups-v1",
        )

    def observed_item(
        self,
        *,
        event: governance.SourceEventKind,
        policy_revision: str,
        allowed: bool,
        observation_generation: str,
    ) -> governance.GovernedSourceItem[bytes]:
        access = self.access(
            policy_revision=policy_revision,
            allowed=allowed,
        )
        return governance.GovernedSourceItem(
            identity=self.identity,
            resource=self.content,
            source_revision=self.source_revision,
            content_fingerprint=hashlib.sha256(self.content).digest(),
            access=access,
            event=event,
            observation_id=governance.make_observation_id(
                self.identity,
                self.source_revision,
                event,
                access,
                observation_generation=observation_generation,
            ),
        )


@dataclasses.dataclass(frozen=True, slots=True)
class DemoChunk:
    point_id: str
    chunk_digest: str
    start: int
    end: int
    text: str


def deterministic_chunks(
    document: DemoDocument,
    *,
    chunk_size: int = 48,
) -> tuple[DemoChunk, ...]:
    if chunk_size < 1:
        raise ValueError("chunk_size must be positive")
    source_digest = document.identity.evidence_digest()
    chunks: list[DemoChunk] = []
    for start in range(0, len(document.content), chunk_size):
        end = min(start + chunk_size, len(document.content))
        raw = document.content[start:end]
        identity = f"{source_digest}:{start}:{end}"
        chunks.append(
            DemoChunk(
                point_id=str(uuid.uuid5(uuid.NAMESPACE_URL, identity)),
                chunk_digest=digest(identity),
                start=start,
                end=end,
                text=raw.decode("utf-8"),
            )
        )
    return tuple(chunks)


def demo_documents() -> tuple[DemoDocument, DemoDocument]:
    governed = DemoDocument(
        identity=governance.SourceIdentity(
            connector_instance_id="demo-source",
            source_scope_id="tenant-alpha-drive",
            item_id="stable-document-alpha",
        ),
        tenant_id="tenant-alpha",
        policy_id="policy-alpha",
        principal_id=PRINCIPAL_ALPHA,
        content=(
            f"{CONTENT_SENTINEL}. The launch checklist is deterministic. "
            "Access changes must revoke every derived chunk without changing "
            "these source bytes."
        ).encode(),
    )
    control = DemoDocument(
        identity=governance.SourceIdentity(
            connector_instance_id="demo-source",
            source_scope_id="tenant-beta-drive",
            item_id="stable-document-beta",
        ),
        tenant_id="tenant-beta",
        policy_id="policy-beta",
        principal_id=PRINCIPAL_BETA,
        content=(
            f"{CONTROL_CONTENT_SENTINEL}. This independent tenant remains "
            "retrievable while tenant alpha is revoked."
        ).encode(),
    )
    return governed, control


__all__ = [
    "CONTENT_SENTINEL",
    "CONTROL_CONTENT_SENTINEL",
    "DemoChunk",
    "DemoDocument",
    "PRINCIPAL_ALPHA",
    "PRINCIPAL_BETA",
    "demo_documents",
    "deterministic_chunks",
    "digest",
]
