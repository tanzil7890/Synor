//! Metadata-only native effect intent and verification state.
//!
//! These records deliberately contain no target payload, source content, raw
//! locator, principal, credential, or remote error text. The engine persists
//! them independently from ordinary target tracking so cleanup evidence
//! survives a successful target-state prune.

use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use synor_utils::fingerprint::Fingerprint;

use crate::prelude::*;

const NATIVE_EFFECT_RECORD_VERSION: u16 = 2;
const NATIVE_EFFECT_OBLIGATION_CURSOR_VERSION: u16 = 1;
const NATIVE_EFFECT_LINEAGE_CURSOR_VERSION: u16 = 1;
const NATIVE_OBLIGATION_SUMMARY_VERSION: u16 = 1;
const MAX_ACTION_ID_BYTES: usize = 256;
const SHA256_HEX_BYTES: usize = 64;

fn default_record_version() -> u16 {
    NATIVE_EFFECT_RECORD_VERSION
}

fn default_obligation_cursor_version() -> u16 {
    NATIVE_EFFECT_OBLIGATION_CURSOR_VERSION
}

fn default_lineage_cursor_version() -> u16 {
    NATIVE_EFFECT_LINEAGE_CURSOR_VERSION
}

pub(crate) fn unix_time_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

fn is_safe_action_id(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(first) if first.is_ascii_alphanumeric())
        && value.len() <= MAX_ACTION_ID_BYTES
        && bytes
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
}

pub(crate) fn is_sha256_hex(value: &str) -> bool {
    value.len() == SHA256_HEX_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// The external postcondition requested by one native effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeEffectOperation {
    #[serde(rename = "delete")]
    Delete,
    #[serde(rename = "isolate")]
    Isolate,
    #[serde(rename = "cleanup")]
    Cleanup,
}

/// The verification contract that must hold before finalization.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeVerificationPolicy {
    /// Compatibility/default value for records without a verified-sink
    /// contract. Strict execution must reject this policy before applying a
    /// governed destructive effect.
    #[default]
    #[serde(rename = "legacy_unverified")]
    LegacyUnverified,
    #[serde(rename = "query_verified")]
    QueryVerified,
}

/// Durable native effect lifecycle.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeEffectStatus {
    #[default]
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "verified")]
    Verified,
    #[serde(rename = "failed")]
    Failed,
    #[serde(rename = "blocked")]
    Blocked,
    #[serde(rename = "completed")]
    Completed,
}

/// Controlled causes for effect creation. No free-form source data is stored.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeEffectCause {
    /// Safe default for records written before cause metadata exists.
    #[default]
    #[serde(rename = "undeclared")]
    Undeclared,
    #[serde(rename = "explicit")]
    Explicit,
    #[serde(rename = "orphaned_component")]
    OrphanedComponent,
    #[serde(rename = "provider_missing")]
    ProviderMissing,
}

/// Fixed, privacy-safe failure categories.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NativeEffectErrorCode {
    #[serde(rename = "provider_missing")]
    ProviderMissing,
    #[serde(rename = "legacy_sink_rejected")]
    LegacySinkRejected,
    #[serde(rename = "sink_apply_failed")]
    SinkApplyFailed,
    #[serde(rename = "verification_failed")]
    VerificationFailed,
    #[serde(rename = "verification_timeout")]
    VerificationTimeout,
    #[serde(rename = "final_commit_failed")]
    FinalCommitFailed,
    #[serde(rename = "cleanup_failed")]
    CleanupFailed,
    #[serde(rename = "unsupported_capability")]
    UnsupportedCapability,
    #[serde(rename = "internal_state")]
    InternalState,
}

/// Stable, privacy-safe identity and proof binding for one external effect.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeEffectDescriptor {
    #[serde(rename = "A")]
    pub action_id: String,
    #[serde(rename = "O")]
    pub operation: NativeEffectOperation,
    #[serde(rename = "S")]
    pub source_digest: String,
    #[serde(rename = "G")]
    pub source_generation: u64,
    #[serde(rename = "T")]
    pub target_locator_digest: String,
}

impl NativeEffectDescriptor {
    pub fn validate(&self) -> Result<()> {
        if !is_safe_action_id(&self.action_id) {
            client_bail!("native effect action ID is not a bounded safe token");
        }
        if self.source_generation == 0 {
            client_bail!("native effect source generation must be at least one");
        }
        if !is_sha256_hex(&self.source_digest) {
            client_bail!("native effect source digest must be lowercase SHA-256 hex");
        }
        if !is_sha256_hex(&self.target_locator_digest) {
            client_bail!("native effect target locator digest must be lowercase SHA-256 hex");
        }
        Ok(())
    }
}

/// Persisted native effect intent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeEffectIntent {
    #[serde(rename = "V", default = "default_record_version")]
    pub record_version: u16,
    #[serde(rename = "D")]
    pub descriptor: NativeEffectDescriptor,
    /// Engine-owned evidence identity. The connector-visible action ID stays
    /// in the descriptor for receipt correlation; this separate ID lets
    /// completed evidence remain immutable across repeated app lifecycles.
    /// Missing on v1 records, where the descriptor action ID was also the
    /// evidence key.
    #[serde(rename = "I", default, skip_serializing_if = "Option::is_none")]
    pub evidence_id: Option<String>,
    /// Opaque fingerprint that lets engine code correlate this effect with
    /// ordinary tracking without serializing a target path or payload here.
    #[serde(rename = "L")]
    pub tracking_locator: Fingerprint,
    #[serde(rename = "P", default)]
    pub verification_policy: NativeVerificationPolicy,
    #[serde(rename = "C", default)]
    pub cause: NativeEffectCause,
    #[serde(rename = "S", default)]
    pub status: NativeEffectStatus,
    /// Unix epoch milliseconds. Zero decodes as "unknown" for forward/backward
    /// compatible records that predate timestamp metadata.
    #[serde(rename = "T", default)]
    pub created_at_unix_ms: u64,
    #[serde(rename = "U", default)]
    pub updated_at_unix_ms: u64,
    #[serde(rename = "A", default)]
    pub attempt_count: u64,
    #[serde(rename = "E", default, skip_serializing_if = "Option::is_none")]
    pub last_error_code: Option<NativeEffectErrorCode>,
}

impl NativeEffectIntent {
    pub fn new(
        descriptor: NativeEffectDescriptor,
        tracking_locator: Fingerprint,
        verification_policy: NativeVerificationPolicy,
    ) -> Result<Self> {
        descriptor.validate()?;
        let now = unix_time_millis();
        Ok(Self {
            record_version: NATIVE_EFFECT_RECORD_VERSION,
            descriptor,
            evidence_id: None,
            tracking_locator,
            verification_policy,
            cause: NativeEffectCause::Explicit,
            status: NativeEffectStatus::Pending,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            attempt_count: 0,
            last_error_code: None,
        })
    }

    pub fn with_cause(mut self, cause: NativeEffectCause) -> Self {
        self.cause = cause;
        self
    }

    pub fn validate(&self) -> Result<()> {
        self.descriptor.validate()?;
        if self
            .evidence_id
            .as_deref()
            .is_some_and(|evidence_id| !is_safe_action_id(evidence_id))
        {
            client_bail!("native effect evidence ID is not a bounded safe token");
        }
        if self.record_version > NATIVE_EFFECT_RECORD_VERSION {
            client_bail!("native effect record version is newer than this binary");
        }
        if self.created_at_unix_ms != 0
            && self.updated_at_unix_ms != 0
            && self.updated_at_unix_ms < self.created_at_unix_ms
        {
            client_bail!("native effect timestamps are inconsistent");
        }
        Ok(())
    }

    pub(crate) fn proof_contract_matches(&self, other: &Self) -> bool {
        self.descriptor == other.descriptor
            && self.tracking_locator == other.tracking_locator
            && self.verification_policy == other.verification_policy
    }

    pub(crate) fn evidence_id(&self) -> &str {
        self.evidence_id
            .as_deref()
            .unwrap_or(&self.descriptor.action_id)
    }

    pub(crate) fn with_evidence_id(mut self, evidence_id: String) -> Result<Self> {
        if !is_safe_action_id(&evidence_id) {
            client_bail!("native effect evidence ID is not a bounded safe token");
        }
        self.evidence_id = Some(evidence_id);
        self.validate()?;
        Ok(self)
    }

    pub(crate) fn start_attempt(&mut self) -> Result<()> {
        self.attempt_count = self
            .attempt_count
            .checked_add(1)
            .ok_or_else(|| internal_error!("native effect attempt count overflow"))?;
        self.record_version = NATIVE_EFFECT_RECORD_VERSION;
        self.status = NativeEffectStatus::Pending;
        self.updated_at_unix_ms = unix_time_millis().max(self.created_at_unix_ms);
        self.last_error_code = None;
        Ok(())
    }

    pub(crate) fn mark_blocked(&mut self, error_code: NativeEffectErrorCode) -> Result<()> {
        self.attempt_count = self
            .attempt_count
            .checked_add(1)
            .ok_or_else(|| internal_error!("native effect attempt count overflow"))?;
        self.record_version = NATIVE_EFFECT_RECORD_VERSION;
        self.status = NativeEffectStatus::Blocked;
        self.updated_at_unix_ms = unix_time_millis().max(self.created_at_unix_ms);
        self.last_error_code = Some(error_code);
        if error_code == NativeEffectErrorCode::ProviderMissing {
            self.cause = NativeEffectCause::ProviderMissing;
        }
        Ok(())
    }

    pub(crate) fn mark_verified(&mut self) {
        self.record_version = NATIVE_EFFECT_RECORD_VERSION;
        self.status = NativeEffectStatus::Verified;
        self.updated_at_unix_ms = unix_time_millis().max(self.created_at_unix_ms);
        self.last_error_code = None;
    }

    pub(crate) fn mark_failed(&mut self, error_code: NativeEffectErrorCode) {
        self.record_version = NATIVE_EFFECT_RECORD_VERSION;
        self.status = NativeEffectStatus::Failed;
        self.updated_at_unix_ms = unix_time_millis().max(self.created_at_unix_ms);
        self.last_error_code = Some(error_code);
    }

    pub(crate) fn mark_completed(&mut self) {
        self.record_version = NATIVE_EFFECT_RECORD_VERSION;
        self.status = NativeEffectStatus::Completed;
        self.updated_at_unix_ms = unix_time_millis().max(self.created_at_unix_ms);
        self.last_error_code = None;
    }
}

/// Aggregate effect status counts exposed to safe inspection/reporting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeEffectCounts {
    pub pending: u64,
    pub verified: u64,
    pub failed: u64,
    pub blocked: u64,
    pub completed: u64,
}

/// Transactionally maintained app-wide totals used by strict completion. The
/// underlying evidence and tombstones remain the source of truth and are
/// still scanned before destructive administration; this additive record is
/// rebuilt atomically when an older schema is first opened by a supporting
/// binary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct NativeObligationSummary {
    #[serde(rename = "V", default = "default_obligation_summary_version")]
    pub record_version: u16,
    #[serde(rename = "E", default)]
    pub effect_counts: NativeEffectCounts,
    #[serde(rename = "T", default)]
    pub query_verified_tombstones: u64,
}

fn default_obligation_summary_version() -> u16 {
    NATIVE_OBLIGATION_SUMMARY_VERSION
}

impl NativeObligationSummary {
    pub(crate) fn empty() -> Self {
        Self {
            record_version: NATIVE_OBLIGATION_SUMMARY_VERSION,
            ..Self::default()
        }
    }

    pub(crate) fn validate(self) -> Result<()> {
        if self.record_version == 0 {
            internal_bail!("native obligation summary has an invalid schema version");
        }
        if self.record_version > NATIVE_OBLIGATION_SUMMARY_VERSION {
            client_bail!("native obligation summary is newer than this binary");
        }
        Ok(())
    }
}

/// Result of an explicit, archive-backed completed-evidence compaction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeEffectCompactionResult {
    pub requested: u64,
    pub deleted: u64,
    pub protected: u64,
    pub already_absent: u64,
}

/// Metadata captured for one app while preparing a downgrade copy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeEffectAppSnapshot {
    pub app_name: String,
    pub schema_version: Option<u32>,
    pub effects: Vec<NativeEffectIntent>,
}

/// Result of preparing a separate, pre-native-compatible database copy.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NativeEffectDowngradeResult {
    pub apps: Vec<NativeEffectAppSnapshot>,
    pub removed_schema_markers: u64,
    pub removed_effects: u64,
    pub removed_obligation_cursors: u64,
    pub removed_lineage_cursors: u64,
    pub removed_live_generation_keys: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct NativeEffectDowngradeStripResult {
    pub removed_schema_markers: u64,
    pub removed_effects: u64,
    pub removed_obligation_cursors: u64,
    pub removed_lineage_cursors: u64,
    pub removed_live_generation_keys: u64,
}

/// Persisted allocation cursor for repeated cleanup obligations at the same
/// tracking locator and source generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct NativeEffectObligationCursor {
    #[serde(rename = "V", default = "default_obligation_cursor_version")]
    pub record_version: u16,
    #[serde(rename = "L")]
    pub tracking_locator: Fingerprint,
    #[serde(rename = "G")]
    pub source_generation: u64,
    #[serde(rename = "E")]
    pub current_epoch: u64,
}

/// Retained cursor for ordinary verified effects at one opaque tracking
/// locator. It binds every unresolved retry to one immutable proof contract
/// and allocates a successor evidence epoch only after completion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct NativeEffectLineageCursor {
    #[serde(rename = "V", default = "default_lineage_cursor_version")]
    pub record_version: u16,
    #[serde(rename = "L")]
    pub tracking_locator: Fingerprint,
    #[serde(rename = "E")]
    pub current_epoch: u64,
    #[serde(rename = "I")]
    pub current_evidence_id: String,
}

impl NativeEffectLineageCursor {
    pub(crate) fn new(
        tracking_locator: Fingerprint,
        current_epoch: u64,
        current_evidence_id: String,
    ) -> Result<Self> {
        let cursor = Self {
            record_version: NATIVE_EFFECT_LINEAGE_CURSOR_VERSION,
            tracking_locator,
            current_epoch,
            current_evidence_id,
        };
        cursor.validate()?;
        Ok(cursor)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.record_version > NATIVE_EFFECT_LINEAGE_CURSOR_VERSION {
            client_bail!("native effect lineage cursor is newer than this binary");
        }
        if self.current_epoch == 0 {
            internal_bail!("native effect lineage epoch must be at least one");
        }
        if !is_safe_action_id(&self.current_evidence_id) {
            internal_bail!("native effect lineage evidence ID is invalid");
        }
        Ok(())
    }
}

impl NativeEffectObligationCursor {
    pub(crate) fn new(tracking_locator: Fingerprint, source_generation: u64) -> Result<Self> {
        let cursor = Self {
            record_version: NATIVE_EFFECT_OBLIGATION_CURSOR_VERSION,
            tracking_locator,
            source_generation,
            current_epoch: 1,
        };
        cursor.validate()?;
        Ok(cursor)
    }

    pub(crate) fn validate(self) -> Result<()> {
        if self.record_version > NATIVE_EFFECT_OBLIGATION_CURSOR_VERSION {
            client_bail!("native effect obligation cursor is newer than this binary");
        }
        if self.source_generation == 0 {
            client_bail!("native effect obligation generation must be at least one");
        }
        if self.current_epoch == 0 {
            internal_bail!("native effect obligation epoch must be at least one");
        }
        Ok(())
    }
}

impl NativeEffectCounts {
    pub fn has_unresolved(self) -> bool {
        self.pending != 0 || self.verified != 0 || self.failed != 0 || self.blocked != 0
    }

    pub(crate) fn add(&mut self, status: NativeEffectStatus) -> Result<()> {
        let count = match status {
            NativeEffectStatus::Pending => &mut self.pending,
            NativeEffectStatus::Verified => &mut self.verified,
            NativeEffectStatus::Failed => &mut self.failed,
            NativeEffectStatus::Blocked => &mut self.blocked,
            NativeEffectStatus::Completed => &mut self.completed,
        };
        *count = count
            .checked_add(1)
            .ok_or_else(|| internal_error!("native effect count overflow"))?;
        Ok(())
    }

    pub(crate) fn remove(&mut self, status: NativeEffectStatus) -> Result<()> {
        let count = match status {
            NativeEffectStatus::Pending => &mut self.pending,
            NativeEffectStatus::Verified => &mut self.verified,
            NativeEffectStatus::Failed => &mut self.failed,
            NativeEffectStatus::Blocked => &mut self.blocked,
            NativeEffectStatus::Completed => &mut self.completed,
        };
        *count = count
            .checked_sub(1)
            .ok_or_else(|| internal_error!("native effect count underflow"))?;
        Ok(())
    }
}

/// Deterministic action ID for cleanup blocked before a connector can
/// describe a provider-native action.
pub fn blocked_cleanup_action_id(tracking_locator: Fingerprint, generation: u64) -> String {
    blocked_cleanup_action_id_for_epoch(tracking_locator, generation, 1)
}

pub fn blocked_cleanup_action_id_for_epoch(
    tracking_locator: Fingerprint,
    generation: u64,
    epoch: u64,
) -> String {
    let generation = generation.max(1);
    let epoch = epoch.max(1);
    let (domain, prefix) = if epoch == 1 {
        (
            b"synor/native-blocked-cleanup-action/v1\0".as_slice(),
            "cleanup1_",
        )
    } else {
        (
            b"synor/native-blocked-cleanup-action/v2\0".as_slice(),
            "cleanup2_",
        )
    };
    let mut input = domain.to_vec();
    input.extend_from_slice(tracking_locator.as_slice());
    input.extend_from_slice(&generation.to_be_bytes());
    if epoch != 1 {
        input.extend_from_slice(&epoch.to_be_bytes());
    }
    let fingerprint = Fingerprint::from_bytes(&input);
    let mut output = String::with_capacity(prefix.len() + 32);
    output.push_str(prefix);
    for byte in fingerprint.0 {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

pub(crate) fn native_effect_obligation_key_fingerprint(
    tracking_locator: Fingerprint,
    source_generation: u64,
) -> Fingerprint {
    let mut input = b"synor/native-effect-obligation-key/v1\0".to_vec();
    input.extend_from_slice(tracking_locator.as_slice());
    input.extend_from_slice(&source_generation.to_be_bytes());
    Fingerprint::from_bytes(&input)
}

pub(crate) fn native_effect_lineage_key_fingerprint(tracking_locator: Fingerprint) -> Fingerprint {
    let mut input = b"synor/native-effect-lineage-key/v1\0".to_vec();
    input.extend_from_slice(tracking_locator.as_slice());
    Fingerprint::from_bytes(&input)
}

pub(crate) fn native_effect_evidence_id(tracking_locator: Fingerprint, epoch: u64) -> String {
    let epoch = epoch.max(1);
    let mut input = b"synor/native-effect-evidence-id/v1\0".to_vec();
    input.extend_from_slice(tracking_locator.as_slice());
    input.extend_from_slice(&epoch.to_be_bytes());
    let fingerprint = Fingerprint::from_bytes(&input);
    let mut output = String::with_capacity(8 + 32);
    output.push_str("effect1_");
    for byte in fingerprint.0 {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

pub(crate) fn native_effect_key_fingerprint(action_id: &str) -> Fingerprint {
    let mut input = b"synor/native-effect-key/v1\0".to_vec();
    input.extend_from_slice(&(action_id.len() as u64).to_be_bytes());
    input.extend_from_slice(action_id.as_bytes());
    Fingerprint::from_bytes(&input)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(byte: char) -> String {
        std::iter::repeat_n(byte, SHA256_HEX_BYTES).collect()
    }

    fn descriptor(action_id: &str) -> NativeEffectDescriptor {
        NativeEffectDescriptor {
            action_id: action_id.to_owned(),
            operation: NativeEffectOperation::Delete,
            source_digest: digest('a'),
            source_generation: 7,
            target_locator_digest: digest('b'),
        }
    }

    #[test]
    fn blocked_cleanup_id_is_stable_and_generation_bound() {
        let locator = Fingerprint::from_bytes(b"locator");
        let first = blocked_cleanup_action_id(locator, 4);
        assert_eq!(first, blocked_cleanup_action_id(locator, 4));
        assert_eq!(first, blocked_cleanup_action_id_for_epoch(locator, 4, 1));
        assert_ne!(first, blocked_cleanup_action_id(locator, 5));
        assert_ne!(first, blocked_cleanup_action_id_for_epoch(locator, 4, 2));
        assert_ne!(
            blocked_cleanup_action_id_for_epoch(locator, 4, 2),
            blocked_cleanup_action_id_for_epoch(locator, 4, 3)
        );
        assert!(is_safe_action_id(&first));
    }

    #[test]
    fn descriptors_reject_unbounded_or_non_digest_metadata() {
        let mut value = descriptor("action1_safe");
        value.source_digest = "raw-source-path".to_owned();
        assert!(value.validate().is_err());

        let mut value = descriptor("action1_safe");
        value.action_id = "unsafe action ID".to_owned();
        assert!(value.validate().is_err());

        let mut value = descriptor("action1_safe");
        value.source_generation = 0;
        assert!(value.validate().is_err());

        descriptor("delete:tenant.source-1").validate().unwrap();
        assert!(descriptor("_invalid-first").validate().is_err());
    }

    #[test]
    fn additive_fields_decode_to_safe_defaults() {
        #[derive(Serialize)]
        struct MinimalIntent<'a> {
            #[serde(rename = "D")]
            descriptor: &'a NativeEffectDescriptor,
            #[serde(rename = "L")]
            tracking_locator: Fingerprint,
        }

        let descriptor = descriptor("action1_legacy");
        let bytes = rmp_serde::to_vec_named(&MinimalIntent {
            descriptor: &descriptor,
            tracking_locator: Fingerprint::from_bytes(b"legacy"),
        })
        .unwrap();
        let decoded: NativeEffectIntent = synor_utils::deser::from_msgpack_slice(&bytes).unwrap();

        assert_eq!(
            decoded.verification_policy,
            NativeVerificationPolicy::LegacyUnverified
        );
        assert_eq!(decoded.cause, NativeEffectCause::Undeclared);
        assert_eq!(decoded.status, NativeEffectStatus::Pending);
        assert_eq!(decoded.attempt_count, 0);
        assert_eq!(decoded.created_at_unix_ms, 0);
        assert_eq!(decoded.last_error_code, None);
        decoded.validate().unwrap();
    }

    #[test]
    fn only_completed_counts_are_resolved() {
        assert!(!NativeEffectCounts::default().has_unresolved());
        for counts in [
            NativeEffectCounts {
                pending: 1,
                ..Default::default()
            },
            NativeEffectCounts {
                verified: 1,
                ..Default::default()
            },
            NativeEffectCounts {
                failed: 1,
                ..Default::default()
            },
            NativeEffectCounts {
                blocked: 1,
                ..Default::default()
            },
        ] {
            assert!(counts.has_unresolved());
        }
        assert!(
            !NativeEffectCounts {
                completed: 4,
                ..Default::default()
            }
            .has_unresolved()
        );
    }
}
