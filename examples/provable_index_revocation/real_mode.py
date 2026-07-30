"""Operator-gated Google Drive and Qdrant configuration for the demo."""

from __future__ import annotations

import dataclasses
import os
import pathlib

import synor as syn
from synor import retrieval
from synor.connectors import google_drive, qdrant


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required for real mode")
    return value


def _csv(name: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in _required(name).split(",") if item.strip())


@dataclasses.dataclass(frozen=True, slots=True)
class RealComponents:
    source: google_drive.GovernedGoogleDriveSource
    target: qdrant.CertifiedQdrantTarget


def configure_real_components(
    *,
    state_store: syn.StateStore,
    suppression_lookup: retrieval.MonotonicSuppressionLookup,
) -> RealComponents:
    """Validate configuration and build lazy clients without certifying them."""

    credentials = pathlib.Path(_required("GOOGLE_DRIVE_CREDENTIALS"))
    if not credentials.is_file():
        raise ValueError("GOOGLE_DRIVE_CREDENTIALS must name a readable file")
    verifier = qdrant.SuppressionBackedQdrantVerifier(suppression_lookup)
    source = google_drive.GovernedGoogleDriveSource(
        service_account_credential_path=str(credentials),
        root_folder_ids=_csv("GOOGLE_DRIVE_ROOT_IDS"),
        shared_drive_ids=tuple(
            item.strip()
            for item in os.getenv("GOOGLE_DRIVE_SHARED_DRIVE_IDS", "").split(",")
            if item.strip()
        ),
        state_store=state_store,
        connector_instance_id=_required("SYNOR_DRIVE_CONNECTOR_ID"),
        source_scope_id=_required("SYNOR_DRIVE_SCOPE_ID"),
        tenant_id=_required("SYNOR_TENANT_ID"),
        policy_revision=_required("SYNOR_POLICY_REVISION"),
        group_graph_revision=_required("SYNOR_GROUP_GRAPH_REVISION"),
    )
    target = qdrant.CertifiedQdrantTarget(
        qdrant.create_client(_required("QDRANT_URL"), timeout=30),
        _required("QDRANT_COLLECTION"),
        lineage_authorizer=verifier.authorize_lineage,
        query_context_verifier=verifier.verify_query_context,
    )
    return RealComponents(source=source, target=target)


__all__ = ["RealComponents", "configure_real_components"]
