//! Per-app handle within a [`Storage`](super::Storage).
//!
//! An `AppStore` is a cheap-clone token that carries the per-app heed
//! `Database` plus a clone of the parent `Env` so standalone read
//! methods can open their own `RoTxn` (with `MDB_READERS_FULL` retry)
//! without the caller having to manage the transaction.
//!
//! Read methods come in two flavors:
//!
//! * **`*_in_txn(wtxn, ...)`** — reads inside a write transaction; see
//!   uncommitted writes in the same txn. Used by `pre_commit` and
//!   friends inside `run_txn` bodies.
//! * **Standalone `read_*(...)` / `list_*(...)`** — open their own snapshot
//!   internally. Used by callers that aren't inside a write txn (memo
//!   lookups, GC sweeps, inspection).
//!
//! Only operations actually invoked from both contexts in production
//! expose both shapes (today: just `read_component_memo`). Methods
//! invoked from one context only get only the corresponding flavor.
//!
//! All I/O methods are `async fn`. LMDB is synchronous internally — the
//! returned futures never yield except where the standalone reader's
//! `MDB_READERS_FULL` retry pauses — but the async signature
//! future-proofs the API.

use futures::future::BoxFuture;

use synor_utils::deser::from_msgpack_slice;
use synor_utils::fingerprint::Fingerprint;

use crate::prelude::*;
use crate::state::db_schema::{
    CHILD_TOMBSTONE_SCHEMA_VERSION, ChildExistenceInfo, ChildTombstoneCause, ChildTombstoneInfo,
    DbEntryKey, FunctionMemoizationEntry, IdSequencerInfo, LIVE_COMPONENT_GENERATION_KEY_SYMBOL,
    NativeSchemaVersion, StablePathEntryKey, StablePathNodeType, StateKind, TargetStateOwnerInfo,
};
use crate::state::native_effect::{
    NativeEffectCause, NativeEffectCompactionResult, NativeEffectCounts,
    NativeEffectDowngradeStripResult, NativeEffectErrorCode, NativeEffectIntent,
    NativeEffectLineageCursor, NativeEffectObligationCursor, NativeEffectStatus,
    NativeVerificationPolicy, blocked_cleanup_action_id_for_epoch, native_effect_evidence_id,
    native_effect_key_fingerprint, native_effect_lineage_key_fingerprint,
    native_effect_obligation_key_fingerprint,
};
use crate::state::stable_path::{StableKey, StablePath, StablePathRef};
use crate::state::target_state_path::TargetStatePath;
use crate::state_store::txn::{ReadTxn, WriteTxn};

/// LMDB database handle. Keys and values are opaque bytes; logical
/// key/value schemas live in [`crate::state::db_schema`].
pub(crate) type Database = heed::Database<heed::types::Bytes, heed::types::Bytes>;

/// Per-app handle within a `Storage`. Carries the `Database`, a clone
/// of the parent `Env` (so standalone read methods can open their own
/// `RoTxn` without the caller having to do so), and a clone of the
/// parent `Storage` (so the session backend can route writes through
/// `Storage::run_txn_boxed`'s single-writer batcher — bypassing it
/// would serialize every per-session write through heed's writer
/// mutex with no amortization).
#[derive(Clone)]
pub struct AppStore {
    pub(crate) db: Database,
    pub(crate) env: heed::Env<heed::WithoutTls>,
    pub(crate) storage: super::storage::Storage,
}

impl AppStore {
    pub(crate) fn new(
        db: Database,
        env: heed::Env<heed::WithoutTls>,
        storage: super::storage::Storage,
    ) -> Self {
        Self { db, env, storage }
    }

    /// Internal accessor for cursor-iteration code (e.g.
    /// `Storage::spawn_stable_path_iter`) that needs the
    /// raw heed handle.
    pub(crate) fn db(&self) -> Database {
        self.db
    }

    /// Run `body` inside a write txn driven by the single-writer
    /// batcher. Concurrent callers coalesce into one underlying
    /// `heed::RwTxn`; bodies within a batch are awaited sequentially.
    /// Ordinary application data writes go through this (or
    /// [`crate::state_store::Storage::run_txn`]) so they participate in
    /// `MDB_MAP_FULL` auto-resize; bypassing the batcher would serialize
    /// each call through heed's writer mutex with no amortization.
    pub(super) async fn run_in_batcher<F>(&self, body: F) -> Result<()>
    where
        F: for<'a, 'env> Fn(&'a mut WriteTxn<'env>) -> BoxFuture<'a, Result<()>>
            + Send
            + Sync
            + 'static,
    {
        // Call `body(wtxn)` directly (borrowing `body` via its `Fn` impl) rather
        // than capturing it in an `async move` block. This keeps the outer closure
        // `Fn` (retryable) instead of `FnOnce`.
        self.run_in_batcher_typed::<(), _>(move |wtxn| body(wtxn))
            .await
    }

    /// Generic variant of [`Self::run_in_batcher`] that returns a
    /// typed value out of the batched body. Used by methods like
    /// `reserve_id_range` whose batched work computes a fresh value.
    pub(super) async fn run_in_batcher_typed<T, F>(&self, body: F) -> Result<T>
    where
        T: Send + 'static,
        F: for<'a, 'env> Fn(&'a mut WriteTxn<'env>) -> BoxFuture<'a, Result<T>>
            + Send
            + Sync
            + 'static,
    {
        self.storage.run_txn(body).await
    }

    /// Open a fresh LMDB read transaction with `MDB_READERS_FULL` retry
    /// (two-phase: short retry → clear stale readers → retry
    /// indefinitely). Used by the standalone read methods and by the
    /// streaming inspection iter.
    ///
    /// The returned [`ReadTxn`] holds a coordinator read guard until it is
    /// dropped, so callers must not keep it open longer than needed.
    pub async fn read_txn<'a>(&'a self) -> Result<ReadTxn<'a>> {
        let guard = self.storage.txn_coordinator().read_owned().await;
        let env = &self.env;
        let try_open = || async {
            match env.read_txn() {
                Ok(txn) => synor_utils::retryable::Ok(txn),
                Err(heed::Error::Mdb(heed::MdbError::ReadersFull)) => {
                    warn!("LMDB readers full, retrying");
                    Err(synor_utils::retryable::Error::retryable(internal_error!(
                        "LMDB readers full"
                    )))
                }
                Err(e) => Err(synor_utils::retryable::Error::not_retryable(e)),
            }
        };

        // Phase 1: short timeout for transient concurrency.
        let txn = match synor_utils::retryable::run(&try_open, &READ_TXN_RETRY_PHASE1).await {
            Ok(txn) => txn,
            Err(e) if !e.is_retryable => return Err(e.into()),
            Err(_) => {
                let cleared = env.clear_stale_readers()?;
                if cleared > 0 {
                    warn!("Cleared {cleared} stale LMDB readers");
                }
                synor_utils::retryable::run(&try_open, &READ_TXN_RETRY_PHASE2)
                    .await
                    .map_err(Into::<Error>::into)?
            }
        };
        Ok(ReadTxn::new(guard, txn))
    }
}

static READ_TXN_RETRY_PHASE1: synor_utils::retryable::RetryOptions =
    synor_utils::retryable::RetryOptions {
        retry_timeout: Some(std::time::Duration::from_secs(3)),
        initial_backoff: std::time::Duration::from_millis(10),
        max_backoff: std::time::Duration::from_secs(1),
    };

static READ_TXN_RETRY_PHASE2: synor_utils::retryable::RetryOptions =
    synor_utils::retryable::RetryOptions {
        retry_timeout: None,
        initial_backoff: std::time::Duration::from_millis(10),
        max_backoff: std::time::Duration::from_secs(1),
    };

// --- Key encoding helpers (internal) -------------------------------------

fn key_tracking_info(path: &StablePath) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(path.clone(), StablePathEntryKey::TrackingInfo).encode()
}

fn key_component_memo(path: &StablePath) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(path.clone(), StablePathEntryKey::ComponentMemoization).encode()
}

fn key_fn_memo(path: &StablePath, fp: Fingerprint) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(path.clone(), StablePathEntryKey::FunctionMemoization(fp)).encode()
}

fn key_fn_memo_prefix(path: &StablePath) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(path.clone(), StablePathEntryKey::FunctionMemoizationPrefix).encode()
}

fn key_child_existence(parent: &StablePath, child_key: &StableKey) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(
        parent.clone(),
        StablePathEntryKey::ChildExistence(child_key.clone()),
    )
    .encode()
}

fn key_child_existence_prefix(parent: &StablePath) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(parent.clone(), StablePathEntryKey::ChildExistencePrefix).encode()
}

fn key_tombstone(parent: &StablePath, relative_path: &StablePath) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(
        parent.clone(),
        StablePathEntryKey::ChildComponentTombstone(relative_path.clone()),
    )
    .encode()
}

fn key_tombstone_prefix(parent: &StablePath) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(
        parent.clone(),
        StablePathEntryKey::ChildComponentTombstonePrefix,
    )
    .encode()
}

fn key_target_state_owner(path: &TargetStatePath) -> Result<Vec<u8>> {
    DbEntryKey::TargetState(path.clone()).encode()
}

fn key_target_segment_name(fp: Fingerprint) -> Result<Vec<u8>> {
    DbEntryKey::TargetSegmentName(fp).encode()
}

fn key_id_sequencer(key: &StableKey) -> Result<Vec<u8>> {
    DbEntryKey::IdSequencer(key.clone()).encode()
}

fn key_native_schema_version() -> Result<Vec<u8>> {
    DbEntryKey::NativeSchemaVersion.encode()
}

fn key_native_effect_prefix() -> Result<Vec<u8>> {
    DbEntryKey::NativeEffectPrefix.encode()
}

fn key_native_effect(action_id: &str) -> Result<Vec<u8>> {
    DbEntryKey::NativeEffect(native_effect_key_fingerprint(action_id)).encode()
}

fn key_native_effect_obligation_prefix() -> Result<Vec<u8>> {
    DbEntryKey::NativeEffectObligationPrefix.encode()
}

fn key_native_effect_obligation(
    tracking_locator: Fingerprint,
    source_generation: u64,
) -> Result<Vec<u8>> {
    DbEntryKey::NativeEffectObligation(native_effect_obligation_key_fingerprint(
        tracking_locator,
        source_generation,
    ))
    .encode()
}

fn key_native_effect_lineage_prefix() -> Result<Vec<u8>> {
    DbEntryKey::NativeEffectLineagePrefix.encode()
}

fn key_native_effect_lineage(tracking_locator: Fingerprint) -> Result<Vec<u8>> {
    DbEntryKey::NativeEffectLineage(native_effect_lineage_key_fingerprint(tracking_locator))
        .encode()
}

fn key_user_state(path: &StablePath, kind: StateKind, user_key: &StableKey) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(
        path.clone(),
        StablePathEntryKey::UserState(kind, user_key.clone()),
    )
    .encode()
}

fn key_user_state_prefix(path: &StablePath, kind: StateKind) -> Result<Vec<u8>> {
    DbEntryKey::StablePath(path.clone(), StablePathEntryKey::UserStatePrefix(kind)).encode()
}

fn decode_native_effect(bytes: &[u8]) -> Result<NativeEffectIntent> {
    let intent: NativeEffectIntent = from_msgpack_slice(bytes)?;
    intent.validate()?;
    Ok(intent)
}

fn decode_native_effect_obligation(bytes: &[u8]) -> Result<NativeEffectObligationCursor> {
    let cursor: NativeEffectObligationCursor = from_msgpack_slice(bytes)?;
    cursor.validate()?;
    Ok(cursor)
}

fn decode_native_effect_lineage(bytes: &[u8]) -> Result<NativeEffectLineageCursor> {
    let cursor: NativeEffectLineageCursor = from_msgpack_slice(bytes)?;
    cursor.validate()?;
    Ok(cursor)
}

fn decode_tombstone(bytes: &[u8]) -> Result<ChildTombstoneInfo> {
    let info = if bytes.is_empty() {
        ChildTombstoneInfo::default()
    } else {
        from_msgpack_slice(bytes)?
    };
    info.validate()?;
    Ok(info)
}

fn validate_native_effect_key(
    stored_fingerprint: Fingerprint,
    intent: &NativeEffectIntent,
) -> Result<()> {
    if native_effect_key_fingerprint(intent.evidence_id()) != stored_fingerprint {
        internal_bail!("native effect key does not match its stored evidence ID");
    }
    Ok(())
}

// --- Tracking info -------------------------------------------------------

impl AppStore {
    /// Read raw tracking-info bytes inside an open write txn. Returns
    /// owned bytes (`Vec<u8>`) so the caller can deserialize from a
    /// local buffer and avoid keeping the txn borrowed for the
    /// deserialized struct's lifetime. Callers typically then do
    /// `from_msgpack_slice::<StablePathEntryTrackingInfo>(&bytes)`.
    pub async fn read_tracking_info_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
    ) -> Result<Option<Vec<u8>>> {
        let key = key_tracking_info(path)?;
        Ok(self.db().get(&**txn, &key)?.map(<[u8]>::to_vec))
    }

    /// Standalone snapshot read of raw tracking-info bytes — no
    /// caller-managed txn. Engine `Committer` uses this to fetch the
    /// post-pre_commit tracking_info for prune+converge, then hands
    /// the new bytes to [`AppStoreTrait::commit`](super::AppStoreTrait::commit)
    /// via the plan.
    pub async fn read_tracking_info(&self, path: &StablePath) -> Result<Option<Vec<u8>>> {
        let rtxn = self.read_txn().await?;
        let key = key_tracking_info(path)?;
        Ok(self.db().get(&*rtxn, &key)?.map(<[u8]>::to_vec))
    }

    /// Write pre-serialized tracking info. Callers serialize externally so
    /// the txn can be re-borrowed mutably after the read-modify-write
    /// pattern used in `pre_commit` (the deserialized `tracking_info`
    /// borrows from the write txn and must be released before writing back).
    pub async fn write_tracking_info_raw(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
        encoded: &[u8],
    ) -> Result<()> {
        let key = key_tracking_info(path)?;
        self.db().put(&mut **txn, &key, encoded)?;
        Ok(())
    }

    pub async fn delete_tracking_info(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
    ) -> Result<()> {
        let key = key_tracking_info(path)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }

    /// Cleanup primitive: read the blob, clear `pending_process_token`
    /// iff it equals `self_token`, write back. Routed through the
    /// single-writer batcher so the write participates in `MDB_MAP_FULL`
    /// auto-resize and whole-transaction retry. Idempotent.
    pub async fn clear_staged_tracking(&self, path: &StablePath, self_token: u128) -> Result<()> {
        let app_store = self.clone();
        let path = path.clone();
        self.run_in_batcher(move |wtxn| {
            let app_store = app_store.clone();
            let path = path.clone();
            Box::pin(async move {
                let key = key_tracking_info(&path)?;
                let Some(bytes) = app_store.db().get(&**wtxn, &key)? else {
                    return Ok(());
                };
                let mut info: crate::state::db_schema::StablePathEntryTrackingInfo<'_> =
                    synor_utils::deser::from_msgpack_slice(bytes)?;
                if info.pending_process_token != Some(self_token) {
                    return Ok(());
                }
                info.pending_process_token = None;
                let new_bytes = rmp_serde::to_vec_named(&info)?;
                app_store.db().put(&mut **wtxn, &key, &new_bytes)?;
                Ok(())
            })
        })
        .await
    }

    /// Standalone Phase 5 sweep: delete one tombstone. Routed through
    /// the single-writer batcher so concurrent callers coalesce into
    /// one underlying write txn (opening `env.write_txn()` here would
    /// serialize every per-component sweep through heed's writer mutex
    /// with no amortization). Idempotent — `delete` on a missing key
    /// is a no-op for heed.
    pub async fn cleanup_tombstone_standalone(
        &self,
        parent: &StablePath,
        relative: &StablePath,
        expected_generation: Option<u64>,
    ) -> Result<bool> {
        let app_store = self.clone();
        let parent = parent.clone();
        let relative = relative.clone();
        self.run_in_batcher_typed(move |wtxn| {
            let app_store = app_store.clone();
            let parent = parent.clone();
            let relative = relative.clone();
            Box::pin(async move {
                app_store
                    .delete_tombstone(wtxn, &parent, &relative, expected_generation)
                    .await
            })
        })
        .await
    }

    /// Standalone existence-chain upsert. Writes the leaf
    /// `__cex(parent_of_leaf, leaf_key, Component)` row; missing
    /// ancestor `Directory` rows are filled in by
    /// [`Self::ensure_path_node_type`]'s recursion, which stops as
    /// soon as it finds an existing row.
    ///
    /// Routed through the single-writer batcher (see
    /// [`Self::cleanup_tombstone_standalone`] for the rationale).
    pub async fn ensure_existence_chain_standalone(
        &self,
        path: &StablePath,
        generation: Option<u64>,
    ) -> Result<()> {
        let Some((_, _)) = path.as_ref().split_parent() else {
            return Ok(());
        };
        let app_store = self.clone();
        let path = path.clone();
        self.run_in_batcher(move |wtxn| {
            let app_store = app_store.clone();
            let path = path.clone();
            Box::pin(async move {
                let Some((parent, key)) = path.as_ref().split_parent() else {
                    return Ok(());
                };
                let parent_owned: StablePath = parent.into();
                app_store
                    .ensure_path_node_type_with_generation(
                        wtxn,
                        parent_owned.as_ref(),
                        key,
                        StablePathNodeType::Component,
                        generation,
                    )
                    .await
            })
        })
        .await
    }

    /// Standalone Phase 6: upsert the component memo. Routed through
    /// the single-writer batcher (see [`Self::cleanup_tombstone_standalone`]
    /// for the rationale).
    pub async fn finalize_memoization_standalone(
        &self,
        component_path: &StablePath,
        encoded: &[u8],
    ) -> Result<()> {
        let app_store = self.clone();
        let path = component_path.clone();
        let encoded = encoded.to_vec();
        self.run_in_batcher(move |wtxn| {
            let app_store = app_store.clone();
            let path = path.clone();
            let encoded = encoded.clone();
            Box::pin(async move {
                app_store
                    .write_component_memo_raw(wtxn, &path, &encoded)
                    .await
            })
        })
        .await
    }

    /// Delete the component-memo row outside a caller-supplied txn.
    /// Routed through the single-writer batcher so concurrent
    /// callers coalesce into one underlying write txn (the same
    /// invariant the LMDB precommit/commit phases rely on; opening
    /// `env.write_txn()` here would serialize every Delete-mode
    /// preflight through heed's writer mutex with no amortization).
    pub async fn delete_component_memo(&self, path: &StablePath) -> Result<()> {
        let app_store = self.clone();
        let path = path.clone();
        self.run_in_batcher(move |wtxn| {
            let app_store = app_store.clone();
            let path = path.clone();
            Box::pin(async move { app_store.delete_component_memo_in_txn(wtxn, &path).await })
        })
        .await
    }

    /// Standalone snapshot read of the `(parent_path, key)` node type.
    pub async fn read_path_node_type(
        &self,
        parent_path: StablePathRef<'_>,
        key: &StableKey,
    ) -> Result<Option<StablePathNodeType>> {
        let rtxn = self.read_txn().await?;
        let parent_owned: StablePath = parent_path.into();
        let cex_key = key_child_existence(&parent_owned, key)?;
        let Some(bytes) = self.db().get(&*rtxn, &cex_key)? else {
            return Ok(None);
        };
        let info: ChildExistenceInfo = from_msgpack_slice(bytes)?;
        Ok(Some(info.node_type))
    }

    /// Reserve an ID range outside a caller-supplied txn. Routed
    /// through the single-writer batcher so concurrent callers
    /// coalesce. Returns the first reserved ID.
    pub async fn reserve_id_range(&self, key: &StableKey, count: u64) -> Result<u64> {
        let app_store = self.clone();
        let key = key.clone();
        self.run_in_batcher_typed(move |wtxn| {
            let app_store = app_store.clone();
            let key = key.clone();
            Box::pin(async move { app_store.reserve_id_range_in_txn(wtxn, &key, count).await })
        })
        .await
    }
}

// --- Component memoization -----------------------------------------------

impl AppStore {
    /// Read raw component-memo bytes inside an open write txn. Sees
    /// uncommitted writes in the same txn. Used by the engine's memo
    /// invalidation path.
    pub async fn read_component_memo_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
    ) -> Result<Option<Vec<u8>>> {
        let key = key_component_memo(path)?;
        Ok(self.db().get(&**txn, &key)?.map(<[u8]>::to_vec))
    }

    /// Read raw component-memo bytes from a fresh snapshot. Used by the
    /// memoization-check fast path outside `run_txn`.
    pub async fn read_component_memo(&self, path: &StablePath) -> Result<Option<Vec<u8>>> {
        let rtxn = self.read_txn().await?;
        let key = key_component_memo(path)?;
        Ok(self.db().get(&*rtxn, &key)?.map(<[u8]>::to_vec))
    }

    /// Write a pre-serialized component memo. Callers serialize externally
    /// for the read-modify-write pattern (see `update_component_memo_states`
    /// in engine code).
    pub async fn write_component_memo_raw(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
        encoded: &[u8],
    ) -> Result<()> {
        let key = key_component_memo(path)?;
        self.db().put(&mut **txn, &key, encoded)?;
        Ok(())
    }

    pub async fn delete_component_memo_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
    ) -> Result<()> {
        let key = key_component_memo(path)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }
}

// --- Function memoization ------------------------------------------------

impl AppStore {
    pub async fn write_fn_memo(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
        fp: Fingerprint,
        entry: &FunctionMemoizationEntry<'_>,
    ) -> Result<()> {
        let value = rmp_serde::to_vec_named(entry)?;
        self.write_fn_memo_raw(txn, path, fp, &value).await
    }

    pub async fn write_fn_memo_raw(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
        fp: Fingerprint,
        encoded: &[u8],
    ) -> Result<()> {
        let key = key_fn_memo(path, fp)?;
        self.db().put(&mut **txn, &key, encoded)?;
        Ok(())
    }

    pub async fn delete_fn_memo(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
        fp: Fingerprint,
    ) -> Result<()> {
        let key = key_fn_memo(path, fp)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }

    /// Prefix-delete every function memo under `path`. Used when the cache
    /// was not populated (full_reprocess, delete mode) — see
    /// `FnMemoCache::into_flush_plan`.
    pub async fn delete_all_fn_memos(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
    ) -> Result<()> {
        let prefix = key_fn_memo_prefix(path)?;
        let db = self.db();
        let mut iter = db.prefix_iter_mut(&mut **txn, &prefix)?;
        while iter.next().transpose()?.is_some() {
            // Safety: we drop the borrowed key/value before the next `next()`.
            unsafe {
                iter.del_current()?;
            }
        }
        Ok(())
    }
}

// --- Child existence -----------------------------------------------------

impl AppStore {
    pub async fn read_child_existence_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        parent: &StablePath,
        child_key: &StableKey,
    ) -> Result<Option<ChildExistenceInfo>> {
        let key = key_child_existence(parent, child_key)?;
        let data = self.db().get(&**txn, &key)?;
        data.map(from_msgpack_slice).transpose().map_err(Into::into)
    }

    pub async fn write_child_existence(
        &self,
        txn: &mut WriteTxn<'_>,
        parent: &StablePath,
        child_key: &StableKey,
        info: &ChildExistenceInfo,
    ) -> Result<bool> {
        let key = key_child_existence(parent, child_key)?;
        if let Some(bytes) = self.db().get(&**txn, &key)? {
            let existing: ChildExistenceInfo = from_msgpack_slice(bytes)?;
            let stale_same_node = existing.node_type == info.node_type
                && (matches!(
                    (existing.generation, info.generation),
                    (Some(existing), Some(proposed)) if proposed < existing
                ) || matches!((existing.generation, info.generation), (Some(_), None)));
            if stale_same_node {
                return Ok(false);
            }
        }
        let value = rmp_serde::to_vec_named(info)?;
        self.db().put(&mut **txn, &key, &value)?;
        Ok(true)
    }

    pub async fn delete_child_existence(
        &self,
        txn: &mut WriteTxn<'_>,
        parent: &StablePath,
        child_key: &StableKey,
    ) -> Result<()> {
        let key = key_child_existence(parent, child_key)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }

    /// All child-existence entries for `parent`, in sorted-key order (which
    /// matches `BTreeMap<StableKey, _>` iteration order because the on-disk
    /// encoding via `storekey` is order-preserving). Used by
    /// `Committer::update_existence` for the sorted-merge against the
    /// in-memory declared children.
    pub async fn list_child_existence_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        parent: &StablePath,
    ) -> Result<Vec<(StableKey, ChildExistenceInfo)>> {
        let prefix = key_child_existence_prefix(parent)?;
        let mut out = Vec::new();
        for entry in self.db().prefix_iter(&**txn, &prefix)? {
            let (raw_key, raw_value) = entry?;
            let stable_key: StableKey = storekey::decode(raw_key[prefix.len()..].as_ref())?;
            let info: ChildExistenceInfo = from_msgpack_slice(raw_value)?;
            out.push((stable_key, info));
        }
        Ok(out)
    }
}

// --- Tombstones ----------------------------------------------------------

impl AppStore {
    pub async fn write_tombstone(
        &self,
        txn: &mut WriteTxn<'_>,
        parent: &StablePath,
        relative_path: &StablePath,
        proposed: &ChildTombstoneInfo,
    ) -> Result<bool> {
        proposed.validate()?;
        if proposed.schema_version != CHILD_TOMBSTONE_SCHEMA_VERSION {
            client_bail!("new child tombstones must use the current schema");
        }
        let key = key_tombstone(parent, relative_path)?;
        let mut value = proposed.clone();
        if let Some(bytes) = self.db().get(&**txn, &key)? {
            let existing = decode_tombstone(bytes)?;
            match (existing.generation, proposed.generation) {
                (Some(existing), Some(proposed)) if proposed < existing => return Ok(false),
                (Some(_), None) => return Ok(false),
                (Some(existing_generation), Some(proposed_generation))
                    if existing_generation == proposed_generation =>
                {
                    if existing.source_digest != proposed.source_digest
                        || existing.verification_policy != proposed.verification_policy
                    {
                        client_bail!("child tombstone retry changed its persisted proof contract");
                    }
                    if existing.created_at_ms != 0 {
                        value.created_at_ms = existing.created_at_ms;
                    }
                    value.attempt_count = existing
                        .attempt_count
                        .checked_add(1)
                        .ok_or_else(|| internal_error!("child tombstone attempt count overflow"))?;
                    value.last_error_code = None;
                }
                (None, None) => {
                    if existing.schema_version != 0
                        && (existing.source_digest != proposed.source_digest
                            || existing.verification_policy != proposed.verification_policy)
                    {
                        client_bail!("child tombstone retry changed its persisted proof contract");
                    }
                    if existing.created_at_ms != 0 {
                        value.created_at_ms = existing.created_at_ms;
                    }
                    value.attempt_count = existing
                        .attempt_count
                        .checked_add(1)
                        .ok_or_else(|| internal_error!("child tombstone attempt count overflow"))?;
                    value.last_error_code = None;
                }
                _ => {}
            }
        }
        let encoded = rmp_serde::to_vec_named(&value)?;
        self.db().put(&mut **txn, &key, &encoded)?;
        Ok(true)
    }

    /// Reopen an existing tombstone for another cleanup attempt through the
    /// single-writer batcher. Same-generation records increment and clear the
    /// prior error; a stale generation returns `false` without mutation.
    pub async fn retry_tombstone(
        &self,
        parent: &StablePath,
        relative_path: &StablePath,
        tombstone: &ChildTombstoneInfo,
    ) -> Result<bool> {
        let app_store = self.clone();
        let parent = parent.clone();
        let relative_path = relative_path.clone();
        let tombstone = tombstone.clone();
        self.run_in_batcher_typed(move |wtxn| {
            let app_store = app_store.clone();
            let parent = parent.clone();
            let relative_path = relative_path.clone();
            let tombstone = tombstone.clone();
            Box::pin(async move {
                app_store
                    .write_tombstone(wtxn, &parent, &relative_path, &tombstone)
                    .await
            })
        })
        .await
    }

    /// Delete a tombstone only if it still names the generation for which
    /// cleanup completed. A stale cleanup can therefore never erase a newer
    /// delete obligation at the same path.
    pub async fn delete_tombstone(
        &self,
        txn: &mut WriteTxn<'_>,
        parent: &StablePath,
        relative_path: &StablePath,
        expected_generation: Option<u64>,
    ) -> Result<bool> {
        let key = key_tombstone(parent, relative_path)?;
        let Some(bytes) = self.db().get(&**txn, &key)? else {
            return Ok(false);
        };
        let current = decode_tombstone(bytes)?;
        if current.generation != expected_generation {
            return Ok(false);
        }
        self.db().delete(&mut **txn, &key)?;
        Ok(true)
    }

    async fn mark_tombstone_failed_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        parent: &StablePath,
        relative_path: &StablePath,
        expected_generation: Option<u64>,
        error_code: NativeEffectErrorCode,
    ) -> Result<bool> {
        let key = key_tombstone(parent, relative_path)?;
        let Some(bytes) = self.db().get(&**txn, &key)? else {
            return Ok(false);
        };
        let mut current = decode_tombstone(bytes)?;
        if current.generation != expected_generation {
            return Ok(false);
        }
        current.schema_version = CHILD_TOMBSTONE_SCHEMA_VERSION;
        current.attempt_count = current.attempt_count.max(1);
        current.last_error_code = Some(error_code);
        current.validate()?;
        let encoded = rmp_serde::to_vec_named(&current)?;
        self.db().put(&mut **txn, &key, &encoded)?;
        Ok(true)
    }

    /// Persist a fixed cleanup failure only while the tombstone still names
    /// the generation that failed.
    pub async fn mark_tombstone_failed(
        &self,
        parent: &StablePath,
        relative_path: &StablePath,
        expected_generation: Option<u64>,
        error_code: NativeEffectErrorCode,
    ) -> Result<bool> {
        let app_store = self.clone();
        let parent = parent.clone();
        let relative_path = relative_path.clone();
        self.run_in_batcher_typed(move |wtxn| {
            let app_store = app_store.clone();
            let parent = parent.clone();
            let relative_path = relative_path.clone();
            Box::pin(async move {
                app_store
                    .mark_tombstone_failed_in_txn(
                        wtxn,
                        &parent,
                        &relative_path,
                        expected_generation,
                        error_code,
                    )
                    .await
            })
        })
        .await
    }

    /// Relative paths and metadata for every cleanup obligation under
    /// `parent`. Empty legacy values decode conservatively.
    pub async fn list_tombstones(
        &self,
        parent: &StablePath,
    ) -> Result<Vec<(StablePath, ChildTombstoneInfo)>> {
        let rtxn = self.read_txn().await?;
        let prefix = key_tombstone_prefix(parent)?;
        let mut out = Vec::new();
        for entry in self.db().prefix_iter(&*rtxn, &prefix)? {
            let (raw_key, raw_value) = entry?;
            let relative: StablePath = storekey::decode(raw_key[prefix.len()..].as_ref())?;
            out.push((relative, decode_tombstone(raw_value)?));
        }
        Ok(out)
    }

    /// Whether any child cleanup obligation exists, regardless of assurance.
    ///
    /// Downgrade preparation uses the stricter all-tombstone check because a
    /// pre-native binary cannot safely resume the richer tombstone lifecycle.
    pub async fn has_any_tombstones(&self) -> Result<bool> {
        let rtxn = self.read_txn().await?;
        self.has_any_tombstones_in_ro_txn(&rtxn)
    }

    fn has_any_tombstones_in_ro_txn(
        &self,
        txn: &heed::RoTxn<'_, heed::WithoutTls>,
    ) -> Result<bool> {
        for entry in self.db().iter(txn)? {
            let (raw_key, raw_value) = entry?;
            if raw_key.first() != Some(&0x10) {
                continue;
            }
            if matches!(
                DbEntryKey::decode(raw_key)?,
                DbEntryKey::StablePath(_, StablePathEntryKey::ChildComponentTombstone(_))
            ) {
                decode_tombstone(raw_value)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn has_any_tombstones_in_txn(&self, txn: &mut WriteTxn<'_>) -> Result<bool> {
        for entry in self.db().iter(&**txn)? {
            let (raw_key, raw_value) = entry?;
            if raw_key.first() != Some(&0x10) {
                continue;
            }
            if matches!(
                DbEntryKey::decode(raw_key)?,
                DbEntryKey::StablePath(_, StablePathEntryKey::ChildComponentTombstone(_))
            ) {
                decode_tombstone(raw_value)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether any cleanup obligation still requires query-verified
    /// reconciliation. This global scan is used only at the strict App
    /// completion boundary, after descendants have quiesced. It closes the
    /// propagation gap where a background component's user error handler
    /// swallows a pre-intent legacy-sink rejection.
    pub async fn has_query_verified_tombstones(&self) -> Result<bool> {
        let rtxn = self.read_txn().await?;
        for entry in self.db().iter(&*rtxn)? {
            let (raw_key, raw_value) = entry?;
            if raw_key.first() != Some(&0x10) {
                continue;
            }
            if matches!(
                DbEntryKey::decode(raw_key)?,
                DbEntryKey::StablePath(_, StablePathEntryKey::ChildComponentTombstone(_))
            ) {
                let tombstone = decode_tombstone(raw_value)?;
                tombstone.validate()?;
                if tombstone.verification_policy == NativeVerificationPolicy::QueryVerified {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    async fn has_query_verified_tombstones_in_txn(&self, txn: &mut WriteTxn<'_>) -> Result<bool> {
        for entry in self.db().iter(&**txn)? {
            let (raw_key, raw_value) = entry?;
            if raw_key.first() != Some(&0x10) {
                continue;
            }
            if matches!(
                DbEntryKey::decode(raw_key)?,
                DbEntryKey::StablePath(_, StablePathEntryKey::ChildComponentTombstone(_))
            ) {
                let tombstone = decode_tombstone(raw_value)?;
                tombstone.validate()?;
                if tombstone.verification_policy == NativeVerificationPolicy::QueryVerified {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Atomic existence-removal + tombstone-write, matching the contract of
    /// `LiveComponentController::delete`'s synchronous step.
    pub async fn remove_child_with_tombstone(
        &self,
        txn: &mut WriteTxn<'_>,
        parent: &StablePath,
        child_key: &StableKey,
        owner_path: &StablePath,
        relative_child: &StablePath,
        cause: ChildTombstoneCause,
        source_digest: Option<String>,
        fallback_generation: Option<u64>,
        verification_policy: NativeVerificationPolicy,
    ) -> Result<Option<ChildTombstoneInfo>> {
        let generation = self
            .read_child_existence_in_txn(txn, parent, child_key)
            .await?
            .map_or(fallback_generation, |existing| existing.generation);
        let tombstone =
            ChildTombstoneInfo::new(cause, source_digest, generation, verification_policy)?;
        // Write first. A newer tombstone rejects this stale operation without
        // removing the corresponding newer child-existence record. Both
        // writes still commit atomically in the caller-owned transaction.
        if !self
            .write_tombstone(txn, owner_path, relative_child, &tombstone)
            .await?
        {
            return Ok(None);
        }
        self.delete_child_existence(txn, parent, child_key).await?;
        Ok(Some(tombstone))
    }
}

// --- Native effect evidence ----------------------------------------------

impl AppStore {
    async fn native_schema_version_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
    ) -> Result<Option<NativeSchemaVersion>> {
        let key = key_native_schema_version()?;
        self.db()
            .get(&**txn, &key)?
            .map(from_msgpack_slice)
            .transpose()
            .map_err(Into::into)
    }

    async fn native_keyspaces_are_empty_in_txn(&self, txn: &mut WriteTxn<'_>) -> Result<bool> {
        let effect_prefix = key_native_effect_prefix()?;
        if self
            .db()
            .prefix_iter(&**txn, &effect_prefix)?
            .next()
            .transpose()?
            .is_some()
        {
            return Ok(false);
        }
        let obligation_prefix = key_native_effect_obligation_prefix()?;
        if self
            .db()
            .prefix_iter(&**txn, &obligation_prefix)?
            .next()
            .transpose()?
            .is_some()
        {
            return Ok(false);
        }
        let lineage_prefix = key_native_effect_lineage_prefix()?;
        Ok(self
            .db()
            .prefix_iter(&**txn, &lineage_prefix)?
            .next()
            .transpose()?
            .is_none())
    }

    fn validate_native_cursor_integrity(
        &self,
        txn: &heed::RoTxn<'_, heed::WithoutTls>,
    ) -> Result<()> {
        let lineage_prefix = key_native_effect_lineage_prefix()?;
        for entry in self.db().prefix_iter(txn, &lineage_prefix)? {
            let (raw_key, raw_value) = entry?;
            let DbEntryKey::NativeEffectLineage(fingerprint) = DbEntryKey::decode(raw_key)? else {
                internal_bail!("unexpected key in native effect lineage keyspace");
            };
            let cursor = decode_native_effect_lineage(raw_value)?;
            if native_effect_lineage_key_fingerprint(cursor.tracking_locator) != fingerprint {
                internal_bail!("native effect lineage key does not match its tracking locator");
            }
            let evidence_key = key_native_effect(&cursor.current_evidence_id)?;
            let Some(evidence_bytes) = self.db().get(txn, &evidence_key)? else {
                internal_bail!("native effect lineage references missing evidence");
            };
            let intent = decode_native_effect(evidence_bytes)?;
            if intent.evidence_id() != cursor.current_evidence_id
                || intent.tracking_locator != cursor.tracking_locator
                || intent.cause == NativeEffectCause::ProviderMissing
            {
                internal_bail!("native effect lineage references mismatched evidence");
            }
        }

        let obligation_prefix = key_native_effect_obligation_prefix()?;
        for entry in self.db().prefix_iter(txn, &obligation_prefix)? {
            let (raw_key, raw_value) = entry?;
            let DbEntryKey::NativeEffectObligation(fingerprint) = DbEntryKey::decode(raw_key)?
            else {
                internal_bail!("unexpected key in native effect obligation keyspace");
            };
            let cursor = decode_native_effect_obligation(raw_value)?;
            if native_effect_obligation_key_fingerprint(
                cursor.tracking_locator,
                cursor.source_generation,
            ) != fingerprint
            {
                internal_bail!("native effect obligation key does not match its tracking metadata");
            }
            let action_id = blocked_cleanup_action_id_for_epoch(
                cursor.tracking_locator,
                cursor.source_generation,
                cursor.current_epoch,
            );
            let evidence_key = key_native_effect(&action_id)?;
            let Some(evidence_bytes) = self.db().get(txn, &evidence_key)? else {
                internal_bail!("native cleanup obligation cursor references missing evidence");
            };
            let intent = decode_native_effect(evidence_bytes)?;
            Self::validate_effect_obligation_lineage(
                &intent,
                cursor.tracking_locator,
                cursor.source_generation,
                &action_id,
            )?;
            if !matches!(
                intent.status,
                NativeEffectStatus::Blocked | NativeEffectStatus::Completed
            ) {
                internal_bail!("native cleanup obligation has an invalid lifecycle status");
            }
        }
        Ok(())
    }

    /// Validate the additive native schema inside a caller-owned write
    /// transaction. A missing marker is valid only when the effect keyspace is
    /// also empty, which is the pre-feature compatibility state.
    pub async fn validate_native_schema_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
    ) -> Result<Option<NativeSchemaVersion>> {
        let version = self.native_schema_version_in_txn(txn).await?;
        match version {
            Some(version) if version.is_supported() => Ok(Some(version)),
            Some(_) => client_bail!("native effect schema is newer than this binary"),
            None if self.native_keyspaces_are_empty_in_txn(txn).await? => Ok(None),
            None => internal_bail!("native effect metadata exists without a schema marker"),
        }
    }

    async fn ensure_native_schema_in_txn(&self, txn: &mut WriteTxn<'_>) -> Result<()> {
        let version = self.validate_native_schema_in_txn(txn).await?;
        if version == Some(NativeSchemaVersion::CURRENT) {
            return Ok(());
        }
        if version.is_some() {
            self.migrate_native_effect_lineages_in_txn(txn).await?;
        }
        let key = key_native_schema_version()?;
        let value = rmp_serde::to_vec_named(&NativeSchemaVersion::CURRENT)?;
        self.db().put(&mut **txn, &key, &value)?;
        Ok(())
    }

    fn validate_native_schema_in_ro_txn(
        &self,
        rtxn: &heed::RoTxn<'_, heed::WithoutTls>,
    ) -> Result<Option<NativeSchemaVersion>> {
        let schema_key = key_native_schema_version()?;
        let version: Option<NativeSchemaVersion> = self
            .db()
            .get(rtxn, &schema_key)?
            .map(from_msgpack_slice)
            .transpose()?;
        match version {
            Some(version) if version.is_supported() => {
                self.validate_native_cursor_integrity(rtxn)?;
                Ok(Some(version))
            }
            Some(_) => client_bail!("native effect schema is newer than this binary"),
            None => {
                let effect_prefix = key_native_effect_prefix()?;
                if self
                    .db()
                    .prefix_iter(rtxn, &effect_prefix)?
                    .next()
                    .transpose()?
                    .is_some()
                {
                    internal_bail!("native effect metadata exists without a schema marker");
                }
                let obligation_prefix = key_native_effect_obligation_prefix()?;
                if self
                    .db()
                    .prefix_iter(rtxn, &obligation_prefix)?
                    .next()
                    .transpose()?
                    .is_some()
                {
                    internal_bail!("native effect metadata exists without a schema marker");
                }
                let lineage_prefix = key_native_effect_lineage_prefix()?;
                if self
                    .db()
                    .prefix_iter(rtxn, &lineage_prefix)?
                    .next()
                    .transpose()?
                    .is_some()
                {
                    internal_bail!("native effect metadata exists without a schema marker");
                }
                Ok(None)
            }
        }
    }

    /// Validate the native schema from a fresh snapshot. Returns `None` for an
    /// untouched pre-feature database.
    pub async fn validate_native_schema(&self) -> Result<Option<NativeSchemaVersion>> {
        let rtxn = self.read_txn().await?;
        self.validate_native_schema_in_ro_txn(&rtxn)
    }

    async fn read_native_effect_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        evidence_id: &str,
    ) -> Result<Option<NativeEffectIntent>> {
        let key = key_native_effect(evidence_id)?;
        let Some(bytes) = self.db().get(&**txn, &key)? else {
            return Ok(None);
        };
        let intent = decode_native_effect(bytes)?;
        if intent.evidence_id() != evidence_id {
            internal_bail!("native effect evidence ID collided with an existing key");
        }
        Ok(Some(intent))
    }

    async fn write_native_effect_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        intent: &NativeEffectIntent,
    ) -> Result<()> {
        intent.validate()?;
        let key = key_native_effect(intent.evidence_id())?;
        let value = rmp_serde::to_vec_named(intent)?;
        self.db().put(&mut **txn, &key, &value)?;
        Ok(())
    }

    async fn read_native_effect_obligation_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        tracking_locator: Fingerprint,
        source_generation: u64,
    ) -> Result<Option<NativeEffectObligationCursor>> {
        let key = key_native_effect_obligation(tracking_locator, source_generation)?;
        let Some(bytes) = self.db().get(&**txn, &key)? else {
            return Ok(None);
        };
        let cursor = decode_native_effect_obligation(bytes)?;
        if cursor.tracking_locator != tracking_locator
            || cursor.source_generation != source_generation
        {
            internal_bail!("native effect obligation cursor collided with an existing key");
        }
        Ok(Some(cursor))
    }

    async fn write_native_effect_obligation_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        cursor: NativeEffectObligationCursor,
    ) -> Result<()> {
        cursor.validate()?;
        let key = key_native_effect_obligation(cursor.tracking_locator, cursor.source_generation)?;
        let value = rmp_serde::to_vec_named(&cursor)?;
        self.db().put(&mut **txn, &key, &value)?;
        Ok(())
    }

    async fn read_native_effect_lineage_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        tracking_locator: Fingerprint,
    ) -> Result<Option<NativeEffectLineageCursor>> {
        let key = key_native_effect_lineage(tracking_locator)?;
        let Some(bytes) = self.db().get(&**txn, &key)? else {
            return Ok(None);
        };
        let cursor = decode_native_effect_lineage(bytes)?;
        if cursor.tracking_locator != tracking_locator {
            internal_bail!("native effect lineage cursor collided with an existing key");
        }
        Ok(Some(cursor))
    }

    async fn write_native_effect_lineage_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        cursor: &NativeEffectLineageCursor,
    ) -> Result<()> {
        cursor.validate()?;
        let key = key_native_effect_lineage(cursor.tracking_locator)?;
        let value = rmp_serde::to_vec_named(cursor)?;
        self.db().put(&mut **txn, &key, &value)?;
        Ok(())
    }

    /// One-time v1/v2 migration. Build every ordinary locator cursor in one
    /// bounded scan before installing schema v3, avoiding an
    /// O(targets × retained-effects) lazy lookup path.
    async fn migrate_native_effect_lineages_in_txn(&self, txn: &mut WriteTxn<'_>) -> Result<()> {
        let prefix = key_native_effect_prefix()?;
        let mut by_locator: std::collections::HashMap<
            Fingerprint,
            (Option<NativeEffectIntent>, Option<NativeEffectIntent>),
        > = std::collections::HashMap::new();
        for entry in self.db().prefix_iter(&**txn, &prefix)? {
            let (raw_key, raw_value) = entry?;
            let DbEntryKey::NativeEffect(fingerprint) = DbEntryKey::decode(raw_key)? else {
                internal_bail!("unexpected key in native effect keyspace");
            };
            let intent = decode_native_effect(raw_value)?;
            validate_native_effect_key(fingerprint, &intent)?;
            if intent.cause == NativeEffectCause::ProviderMissing {
                continue;
            }
            let entry = by_locator
                .entry(intent.tracking_locator)
                .or_insert((None, None));
            if intent.status == NativeEffectStatus::Completed {
                if entry
                    .1
                    .as_ref()
                    .is_none_or(|current| intent.updated_at_unix_ms > current.updated_at_unix_ms)
                {
                    entry.1 = Some(intent);
                }
            } else if entry.0.replace(intent).is_some() {
                internal_bail!("multiple unresolved native effects share one tracking locator");
            }
        }
        for (tracking_locator, (unresolved, latest_completed)) in by_locator {
            let intent = unresolved
                .or(latest_completed)
                .ok_or_else(|| internal_error!("native lineage migration lost its effect"))?;
            let cursor = NativeEffectLineageCursor::new(
                tracking_locator,
                1,
                intent.evidence_id().to_owned(),
            )?;
            self.write_native_effect_lineage_in_txn(txn, &cursor)
                .await?;
        }
        Ok(())
    }

    /// Read the active ordinary effect without upgrading schema metadata or
    /// allocating a lineage cursor. Preview planning uses this path because
    /// its enclosing transaction must remain byte-for-byte write-free.
    pub async fn active_native_effect_id_for_locator_read_only_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        tracking_locator: Fingerprint,
    ) -> Result<Option<String>> {
        let version = self.validate_native_schema_in_txn(txn).await?;
        if version == Some(NativeSchemaVersion::CURRENT) {
            let Some(cursor) = self
                .read_native_effect_lineage_in_txn(txn, tracking_locator)
                .await?
            else {
                return Ok(None);
            };
            let Some(intent) = self
                .read_native_effect_in_txn(txn, &cursor.current_evidence_id)
                .await?
            else {
                internal_bail!("native effect lineage references missing evidence");
            };
            if intent.tracking_locator != tracking_locator {
                internal_bail!("native effect lineage references a different tracking locator");
            }
            return Ok((intent.status != NativeEffectStatus::Completed)
                .then_some(cursor.current_evidence_id));
        }

        let prefix = key_native_effect_prefix()?;
        let mut unresolved = None;
        for entry in self.db().prefix_iter(&**txn, &prefix)? {
            let (raw_key, raw_value) = entry?;
            let DbEntryKey::NativeEffect(fingerprint) = DbEntryKey::decode(raw_key)? else {
                internal_bail!("unexpected key in native effect keyspace");
            };
            let intent = decode_native_effect(raw_value)?;
            validate_native_effect_key(fingerprint, &intent)?;
            if intent.tracking_locator != tracking_locator
                || intent.cause == NativeEffectCause::ProviderMissing
                || intent.status == NativeEffectStatus::Completed
            {
                continue;
            }
            if unresolved
                .replace(intent.evidence_id().to_owned())
                .is_some()
            {
                internal_bail!("multiple unresolved native effects share one tracking locator");
            }
        }
        Ok(unresolved)
    }

    async fn plan_native_effect_lineage_details_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        proposed: NativeEffectIntent,
    ) -> Result<(NativeEffectIntent, u64)> {
        proposed.validate()?;
        let version = self.validate_native_schema_in_txn(txn).await?;
        let tracking_locator = proposed.tracking_locator;
        let existing = if version == Some(NativeSchemaVersion::CURRENT) {
            match self
                .read_native_effect_lineage_in_txn(txn, tracking_locator)
                .await?
            {
                Some(cursor) => {
                    let Some(intent) = self
                        .read_native_effect_in_txn(txn, &cursor.current_evidence_id)
                        .await?
                    else {
                        internal_bail!("native effect lineage references missing evidence");
                    };
                    if intent.tracking_locator != tracking_locator
                        || intent.cause == NativeEffectCause::ProviderMissing
                    {
                        internal_bail!("native effect lineage references mismatched evidence");
                    }
                    Some((intent, cursor.current_epoch))
                }
                None => None,
            }
        } else {
            let prefix = key_native_effect_prefix()?;
            let mut unresolved = None;
            let mut latest_completed = None;
            for entry in self.db().prefix_iter(&**txn, &prefix)? {
                let (raw_key, raw_value) = entry?;
                let DbEntryKey::NativeEffect(fingerprint) = DbEntryKey::decode(raw_key)? else {
                    internal_bail!("unexpected key in native effect keyspace");
                };
                let intent = decode_native_effect(raw_value)?;
                validate_native_effect_key(fingerprint, &intent)?;
                if intent.tracking_locator != tracking_locator
                    || intent.cause == NativeEffectCause::ProviderMissing
                {
                    continue;
                }
                if intent.status == NativeEffectStatus::Completed {
                    if latest_completed
                        .as_ref()
                        .is_none_or(|current: &NativeEffectIntent| {
                            intent.updated_at_unix_ms > current.updated_at_unix_ms
                        })
                    {
                        latest_completed = Some(intent);
                    }
                } else if unresolved.replace(intent).is_some() {
                    internal_bail!("multiple unresolved native effects share one tracking locator");
                }
            }
            unresolved.or(latest_completed).map(|intent| (intent, 1))
        };

        match existing {
            Some((intent, epoch)) if intent.status != NativeEffectStatus::Completed => {
                Self::require_same_native_effect_contract(&intent, &proposed)?;
                Ok((
                    proposed.with_evidence_id(intent.evidence_id().to_owned())?,
                    epoch,
                ))
            }
            Some((_intent, epoch)) => {
                let next_epoch = epoch
                    .checked_add(1)
                    .ok_or_else(|| internal_error!("native effect lineage epoch overflow"))?;
                let evidence_id = native_effect_evidence_id(tracking_locator, next_epoch);
                if self
                    .read_native_effect_in_txn(txn, &evidence_id)
                    .await?
                    .is_some()
                {
                    internal_bail!("native effect evidence ID collided with an existing record");
                }
                Ok((proposed.with_evidence_id(evidence_id)?, next_epoch))
            }
            None => {
                let evidence_id = native_effect_evidence_id(tracking_locator, 1);
                if self
                    .read_native_effect_in_txn(txn, &evidence_id)
                    .await?
                    .is_some()
                {
                    internal_bail!("native effect evidence ID collided with an existing record");
                }
                Ok((proposed.with_evidence_id(evidence_id)?, 1))
            }
        }
    }

    /// Bind an ordinary effect to the evidence identity it would receive at
    /// commit, without writing schema, cursor, or evidence metadata.
    pub async fn plan_native_effect_lineage_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        proposed: NativeEffectIntent,
    ) -> Result<NativeEffectIntent> {
        self.plan_native_effect_lineage_details_in_txn(txn, proposed)
            .await
            .map(|(intent, _)| intent)
    }

    /// Return the active ordinary effect for a tracking locator, if any.
    /// Lookup bootstraps legacy records but never advances a completed
    /// lineage, so read-only recovery checks cannot create a new lifecycle.
    pub async fn active_native_effect_id_for_locator_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        tracking_locator: Fingerprint,
    ) -> Result<Option<String>> {
        let version = self.validate_native_schema_in_txn(txn).await?;
        if version.is_some() && version != Some(NativeSchemaVersion::CURRENT) {
            self.ensure_native_schema_in_txn(txn).await?;
        }
        let Some(cursor) = self
            .read_native_effect_lineage_in_txn(txn, tracking_locator)
            .await?
        else {
            return Ok(None);
        };
        let Some(intent) = self
            .read_native_effect_in_txn(txn, &cursor.current_evidence_id)
            .await?
        else {
            internal_bail!("native effect lineage references missing evidence");
        };
        if intent.tracking_locator != tracking_locator {
            internal_bail!("native effect lineage references a different tracking locator");
        }
        Ok((intent.status != NativeEffectStatus::Completed).then_some(cursor.current_evidence_id))
    }

    /// Validate a previewed retry against its retained immutable proof
    /// contract without changing the evidence record or its lineage.
    pub async fn validate_native_effect_retry_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        evidence_id: &str,
        proposed: &NativeEffectIntent,
    ) -> Result<()> {
        let Some(existing) = self.read_native_effect_in_txn(txn, evidence_id).await? else {
            internal_bail!("native effect lineage references missing evidence");
        };
        Self::require_same_native_effect_contract(&existing, proposed)
    }

    /// Bind a proposed ordinary effect to its retained evidence lineage.
    /// Unresolved retries must preserve the exact proof contract. A successor
    /// evidence ID is allocated only after the prior record is Completed.
    pub async fn bind_native_effect_lineage_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        proposed: NativeEffectIntent,
    ) -> Result<NativeEffectIntent> {
        proposed.validate()?;
        self.ensure_native_schema_in_txn(txn).await?;
        let tracking_locator = proposed.tracking_locator;
        let mut next_epoch = 1;
        if let Some(cursor) = self
            .read_native_effect_lineage_in_txn(txn, tracking_locator)
            .await?
        {
            let Some(existing) = self
                .read_native_effect_in_txn(txn, &cursor.current_evidence_id)
                .await?
            else {
                internal_bail!("native effect lineage references missing evidence");
            };
            if existing.status != NativeEffectStatus::Completed {
                Self::require_same_native_effect_contract(&existing, &proposed)?;
                return proposed.with_evidence_id(cursor.current_evidence_id);
            }
            next_epoch = cursor
                .current_epoch
                .checked_add(1)
                .ok_or_else(|| internal_error!("native effect lineage epoch overflow"))?;
        }

        let evidence_id = native_effect_evidence_id(tracking_locator, next_epoch);
        if self
            .read_native_effect_in_txn(txn, &evidence_id)
            .await?
            .is_some()
        {
            internal_bail!("native effect evidence ID collided with an existing record");
        }
        let bound = proposed.with_evidence_id(evidence_id.clone())?;
        let cursor = NativeEffectLineageCursor::new(tracking_locator, next_epoch, evidence_id)?;
        self.write_native_effect_lineage_in_txn(txn, &cursor)
            .await?;
        Ok(bound)
    }

    fn validate_effect_obligation_lineage(
        intent: &NativeEffectIntent,
        tracking_locator: Fingerprint,
        source_generation: u64,
        expected_action_id: &str,
    ) -> Result<()> {
        if intent.tracking_locator != tracking_locator
            || intent.evidence_id() != expected_action_id
            || intent.descriptor.action_id != expected_action_id
            || intent.descriptor.source_generation != source_generation
            || intent.descriptor.operation
                != crate::state::native_effect::NativeEffectOperation::Cleanup
            || intent.descriptor.source_digest != "0".repeat(64)
            || intent.descriptor.target_locator_digest != "0".repeat(64)
            || intent.cause != NativeEffectCause::ProviderMissing
            || intent.verification_policy != NativeVerificationPolicy::QueryVerified
        {
            internal_bail!("native cleanup obligation ID is bound to different metadata");
        }
        Ok(())
    }

    /// Allocate a stable action ID for a provider-missing observation.
    ///
    /// Retries reuse the current unresolved obligation. Once that obligation
    /// is completed, a checked monotonic epoch produces a new ID while the
    /// completed record remains immutable.
    pub async fn allocate_blocked_cleanup_action_id_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        tracking_locator: Fingerprint,
        source_generation: u64,
    ) -> Result<String> {
        if source_generation == 0 {
            client_bail!("native cleanup obligation generation must be at least one");
        }
        self.ensure_native_schema_in_txn(txn).await?;
        let mut cursor = match self
            .read_native_effect_obligation_in_txn(txn, tracking_locator, source_generation)
            .await?
        {
            Some(cursor) => cursor,
            None => NativeEffectObligationCursor::new(tracking_locator, source_generation)?,
        };

        loop {
            let action_id = blocked_cleanup_action_id_for_epoch(
                tracking_locator,
                source_generation,
                cursor.current_epoch,
            );
            match self.read_native_effect_in_txn(txn, &action_id).await? {
                Some(intent) => {
                    Self::validate_effect_obligation_lineage(
                        &intent,
                        tracking_locator,
                        source_generation,
                        &action_id,
                    )?;
                    match intent.status {
                        NativeEffectStatus::Blocked => {
                            self.write_native_effect_obligation_in_txn(txn, cursor)
                                .await?;
                            return Ok(action_id);
                        }
                        NativeEffectStatus::Completed => {}
                        _ => {
                            internal_bail!(
                                "native cleanup obligation has an invalid lifecycle status"
                            );
                        }
                    }
                    cursor.current_epoch =
                        cursor.current_epoch.checked_add(1).ok_or_else(|| {
                            internal_error!("native cleanup obligation epoch overflow")
                        })?;
                }
                None => {
                    self.write_native_effect_obligation_in_txn(txn, cursor)
                        .await?;
                    return Ok(action_id);
                }
            }
        }
    }

    /// Return the current blocked provider-missing obligation without
    /// allocating a new lifecycle after completed evidence.
    pub async fn active_blocked_cleanup_action_id_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        tracking_locator: Fingerprint,
        source_generation: u64,
    ) -> Result<Option<String>> {
        if source_generation == 0 {
            return Ok(None);
        }
        self.validate_native_schema_in_txn(txn).await?;
        let cursor = match self
            .read_native_effect_obligation_in_txn(txn, tracking_locator, source_generation)
            .await?
        {
            Some(cursor) => cursor,
            None => {
                let legacy_action_id =
                    blocked_cleanup_action_id_for_epoch(tracking_locator, source_generation, 1);
                let Some(intent) = self
                    .read_native_effect_in_txn(txn, &legacy_action_id)
                    .await?
                else {
                    return Ok(None);
                };
                Self::validate_effect_obligation_lineage(
                    &intent,
                    tracking_locator,
                    source_generation,
                    &legacy_action_id,
                )?;
                self.ensure_native_schema_in_txn(txn).await?;
                let cursor =
                    NativeEffectObligationCursor::new(tracking_locator, source_generation)?;
                self.write_native_effect_obligation_in_txn(txn, cursor)
                    .await?;
                cursor
            }
        };
        let action_id = blocked_cleanup_action_id_for_epoch(
            tracking_locator,
            source_generation,
            cursor.current_epoch,
        );
        let Some(intent) = self.read_native_effect_in_txn(txn, &action_id).await? else {
            internal_bail!("native cleanup obligation cursor references missing evidence");
        };
        Self::validate_effect_obligation_lineage(
            &intent,
            tracking_locator,
            source_generation,
            &action_id,
        )?;
        match intent.status {
            NativeEffectStatus::Blocked => Ok(Some(action_id)),
            NativeEffectStatus::Completed => Ok(None),
            _ => internal_bail!("native cleanup obligation has an invalid lifecycle status"),
        }
    }

    /// Read a provider-missing obligation without installing legacy schema or
    /// cursor metadata. This is the preview-safe counterpart to
    /// [`Self::active_blocked_cleanup_action_id_in_txn`].
    pub async fn active_blocked_cleanup_action_id_read_only_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        tracking_locator: Fingerprint,
        source_generation: u64,
    ) -> Result<Option<String>> {
        if source_generation == 0 {
            return Ok(None);
        }
        self.validate_native_schema_in_txn(txn).await?;
        let cursor = self
            .read_native_effect_obligation_in_txn(txn, tracking_locator, source_generation)
            .await?;
        let has_cursor = cursor.is_some();
        let epoch = cursor.map_or(1, |cursor| cursor.current_epoch);
        let action_id =
            blocked_cleanup_action_id_for_epoch(tracking_locator, source_generation, epoch);
        let Some(intent) = self.read_native_effect_in_txn(txn, &action_id).await? else {
            if has_cursor {
                internal_bail!("native cleanup obligation cursor references missing evidence");
            }
            return Ok(None);
        };
        Self::validate_effect_obligation_lineage(
            &intent,
            tracking_locator,
            source_generation,
            &action_id,
        )?;
        match intent.status {
            NativeEffectStatus::Blocked => Ok(Some(action_id)),
            NativeEffectStatus::Completed => Ok(None),
            _ => internal_bail!("native cleanup obligation has an invalid lifecycle status"),
        }
    }

    async fn plan_blocked_cleanup_action_details_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        tracking_locator: Fingerprint,
        source_generation: u64,
    ) -> Result<(String, u64)> {
        if source_generation == 0 {
            client_bail!("native cleanup obligation generation must be at least one");
        }
        self.validate_native_schema_in_txn(txn).await?;
        let cursor = self
            .read_native_effect_obligation_in_txn(txn, tracking_locator, source_generation)
            .await?;
        let cursor_existed = cursor.is_some();
        let mut epoch = cursor.map_or(1, |cursor| cursor.current_epoch);
        let mut first = true;
        loop {
            let action_id =
                blocked_cleanup_action_id_for_epoch(tracking_locator, source_generation, epoch);
            match self.read_native_effect_in_txn(txn, &action_id).await? {
                Some(intent) => {
                    Self::validate_effect_obligation_lineage(
                        &intent,
                        tracking_locator,
                        source_generation,
                        &action_id,
                    )?;
                    match intent.status {
                        NativeEffectStatus::Blocked => return Ok((action_id, epoch)),
                        NativeEffectStatus::Completed => {
                            epoch = epoch.checked_add(1).ok_or_else(|| {
                                internal_error!("native cleanup obligation epoch overflow")
                            })?;
                            first = false;
                        }
                        _ => {
                            internal_bail!(
                                "native cleanup obligation has an invalid lifecycle status"
                            );
                        }
                    }
                }
                None if cursor_existed && first => {
                    internal_bail!("native cleanup obligation cursor references missing evidence");
                }
                None => return Ok((action_id, epoch)),
            }
        }
    }

    /// Return the stable blocker identity that commit would allocate without
    /// changing the obligation cursor or native schema.
    pub async fn plan_blocked_cleanup_action_id_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        tracking_locator: Fingerprint,
        source_generation: u64,
    ) -> Result<String> {
        self.plan_blocked_cleanup_action_details_in_txn(txn, tracking_locator, source_generation)
            .await
            .map(|(action_id, _)| action_id)
    }

    /// Terminal precommit writer for all native metadata. Every contract,
    /// lifecycle transition, and prospective cursor is validated first; only
    /// then are schema, evidence, and cursor records written.
    pub async fn apply_precommit_native_effects_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        ordinary: &[NativeEffectIntent],
        blocked: &[NativeEffectIntent],
    ) -> Result<()> {
        let mut ordinary_writes = Vec::with_capacity(ordinary.len());
        let mut ordinary_cursors = Vec::with_capacity(ordinary.len());
        let mut seen_ordinary = std::collections::HashSet::new();
        for proposed in ordinary {
            if !seen_ordinary.insert(proposed.tracking_locator) {
                internal_bail!("precommit contains duplicate ordinary native effect lineages");
            }
            let (planned, epoch) = self
                .plan_native_effect_lineage_details_in_txn(txn, proposed.clone())
                .await?;
            if planned.evidence_id() != proposed.evidence_id() {
                client_bail!("native effect evidence lineage changed before precommit");
            }
            let write = match self
                .read_native_effect_in_txn(txn, proposed.evidence_id())
                .await?
            {
                Some(existing) => {
                    Self::require_same_native_effect_contract(&existing, proposed)?;
                    if matches!(
                        existing.status,
                        NativeEffectStatus::Blocked
                            | NativeEffectStatus::Verified
                            | NativeEffectStatus::Completed
                    ) {
                        None
                    } else {
                        let mut intent = existing;
                        intent.start_attempt()?;
                        Some(intent)
                    }
                }
                None => {
                    let mut intent = proposed.clone();
                    intent.start_attempt()?;
                    Some(intent)
                }
            };
            ordinary_writes.push(write);
            ordinary_cursors.push(NativeEffectLineageCursor::new(
                proposed.tracking_locator,
                epoch,
                proposed.evidence_id().to_owned(),
            )?);
        }

        let mut blocked_writes = Vec::with_capacity(blocked.len());
        let mut blocked_cursors = Vec::with_capacity(blocked.len());
        let mut seen_blocked = std::collections::HashSet::new();
        for proposed in blocked {
            let tracking_locator = proposed.tracking_locator;
            let source_generation = proposed.descriptor.source_generation;
            if !seen_blocked.insert((tracking_locator, source_generation)) {
                internal_bail!("precommit contains duplicate native cleanup obligations");
            }
            let (action_id, epoch) = self
                .plan_blocked_cleanup_action_details_in_txn(
                    txn,
                    tracking_locator,
                    source_generation,
                )
                .await?;
            if action_id != proposed.evidence_id() || action_id != proposed.descriptor.action_id {
                client_bail!("native cleanup obligation lineage changed before precommit");
            }
            Self::validate_effect_obligation_lineage(
                proposed,
                tracking_locator,
                source_generation,
                &action_id,
            )?;
            let write = match self.read_native_effect_in_txn(txn, &action_id).await? {
                Some(existing) => {
                    Self::require_same_native_effect_contract(&existing, proposed)?;
                    if matches!(
                        existing.status,
                        NativeEffectStatus::Verified | NativeEffectStatus::Completed
                    ) {
                        None
                    } else {
                        let mut intent = existing;
                        intent.mark_blocked(NativeEffectErrorCode::ProviderMissing)?;
                        Some(intent)
                    }
                }
                None => {
                    let mut intent = proposed.clone();
                    intent.mark_blocked(NativeEffectErrorCode::ProviderMissing)?;
                    Some(intent)
                }
            };
            let mut cursor =
                NativeEffectObligationCursor::new(tracking_locator, source_generation)?;
            cursor.current_epoch = epoch;
            cursor.validate()?;
            blocked_writes.push(write);
            blocked_cursors.push(cursor);
        }

        if ordinary.is_empty() && blocked.is_empty() {
            return Ok(());
        }
        self.ensure_native_schema_in_txn(txn).await?;
        for intent in ordinary_writes.into_iter().flatten() {
            self.write_native_effect_in_txn(txn, &intent).await?;
        }
        for intent in blocked_writes.into_iter().flatten() {
            self.write_native_effect_in_txn(txn, &intent).await?;
        }
        for cursor in &ordinary_cursors {
            self.write_native_effect_lineage_in_txn(txn, cursor).await?;
        }
        for cursor in blocked_cursors {
            self.write_native_effect_obligation_in_txn(txn, cursor)
                .await?;
        }
        Ok(())
    }

    fn require_same_native_effect_contract(
        existing: &NativeEffectIntent,
        proposed: &NativeEffectIntent,
    ) -> Result<()> {
        if !existing.proof_contract_matches(proposed) {
            client_bail!("native effect retry changed its persisted proof contract");
        }
        Ok(())
    }

    /// Insert or reopen a native effect before external apply. Identity,
    /// generation, locator, operation, tracking locator, and verification
    /// policy are immutable across retries.
    ///
    /// This method intentionally takes a caller-owned transaction so precommit
    /// can persist the effect atomically with ordinary target tracking.
    pub async fn upsert_native_effect_intent_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        proposed: &NativeEffectIntent,
    ) -> Result<()> {
        proposed.validate()?;
        self.ensure_native_schema_in_txn(txn).await?;

        let mut intent = match self
            .read_native_effect_in_txn(txn, proposed.evidence_id())
            .await?
        {
            Some(existing) => {
                Self::require_same_native_effect_contract(&existing, proposed)?;
                if matches!(
                    existing.status,
                    NativeEffectStatus::Blocked
                        | NativeEffectStatus::Verified
                        | NativeEffectStatus::Completed
                ) {
                    return Ok(());
                }
                existing
            }
            None => proposed.clone(),
        };
        intent.start_attempt()?;
        self.write_native_effect_in_txn(txn, &intent).await
    }

    /// Persist a blocked cleanup obligation when apply cannot safely be
    /// attempted (for example, because its provider is unavailable).
    pub async fn upsert_blocked_native_effect_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        proposed: &NativeEffectIntent,
        error_code: NativeEffectErrorCode,
    ) -> Result<()> {
        proposed.validate()?;
        self.ensure_native_schema_in_txn(txn).await?;

        let mut intent = match self
            .read_native_effect_in_txn(txn, proposed.evidence_id())
            .await?
        {
            Some(existing) => {
                Self::require_same_native_effect_contract(&existing, proposed)?;
                if matches!(
                    existing.status,
                    NativeEffectStatus::Verified | NativeEffectStatus::Completed
                ) {
                    return Ok(());
                }
                existing
            }
            None => proposed.clone(),
        };
        intent.mark_blocked(error_code)?;
        self.write_native_effect_in_txn(txn, &intent).await
    }

    async fn load_native_effect_batch_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        action_ids: &std::collections::BTreeSet<String>,
    ) -> Result<Vec<NativeEffectIntent>> {
        let mut intents = Vec::with_capacity(action_ids.len());
        for action_id in action_ids {
            let Some(intent) = self.read_native_effect_in_txn(txn, action_id).await? else {
                client_bail!("native effect transition references an unknown action ID");
            };
            intents.push(intent);
        }
        Ok(intents)
    }

    pub async fn is_native_effect_blocked_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        action_id: &str,
    ) -> Result<bool> {
        self.validate_native_schema_in_txn(txn).await?;
        Ok(matches!(
            self.read_native_effect_in_txn(txn, action_id).await?,
            Some(intent) if intent.status == NativeEffectStatus::Blocked
        ))
    }

    async fn mark_native_effects_verified_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        action_ids: &std::collections::BTreeSet<String>,
    ) -> Result<()> {
        self.ensure_native_schema_in_txn(txn).await?;
        let mut intents = self
            .load_native_effect_batch_in_txn(txn, action_ids)
            .await?;
        for intent in &intents {
            if intent.verification_policy != NativeVerificationPolicy::QueryVerified {
                client_bail!("legacy native effect cannot be marked query-verified");
            }
            if intent.status == NativeEffectStatus::Blocked {
                client_bail!("blocked native effect requires the recovery transition");
            }
        }
        for intent in &mut intents {
            if intent.status != NativeEffectStatus::Completed {
                intent.mark_verified();
                self.write_native_effect_in_txn(txn, intent).await?;
            }
        }
        Ok(())
    }

    /// Persist verified postconditions in a standalone batched transaction.
    /// This does not complete an effect; finalization remains the responsibility
    /// of [`Self::finalize_native_effects_in_txn`] in the final tracking commit.
    pub async fn mark_native_effects_verified(&self, action_ids: &[String]) -> Result<()> {
        if action_ids.is_empty() {
            return Ok(());
        }
        let action_ids: std::collections::BTreeSet<String> = action_ids.iter().cloned().collect();
        let app_store = self.clone();
        self.run_in_batcher(move |wtxn| {
            let app_store = app_store.clone();
            let action_ids = action_ids.clone();
            Box::pin(async move {
                app_store
                    .mark_native_effects_verified_in_txn(wtxn, &action_ids)
                    .await
            })
        })
        .await
    }

    async fn mark_native_effects_failed_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        action_ids: &std::collections::BTreeSet<String>,
        error_code: NativeEffectErrorCode,
    ) -> Result<()> {
        self.ensure_native_schema_in_txn(txn).await?;
        let mut intents = self
            .load_native_effect_batch_in_txn(txn, action_ids)
            .await?;
        for intent in &intents {
            if intent.status == NativeEffectStatus::Verified {
                client_bail!("verified native effect cannot regress to failed");
            }
        }
        for intent in &mut intents {
            if intent.status != NativeEffectStatus::Completed {
                intent.mark_failed(error_code);
                self.write_native_effect_in_txn(txn, intent).await?;
            }
        }
        Ok(())
    }

    /// Persist a fixed, metadata-only failure code for a batch of effects.
    pub async fn mark_native_effects_failed(
        &self,
        action_ids: &[String],
        error_code: NativeEffectErrorCode,
    ) -> Result<()> {
        if action_ids.is_empty() {
            return Ok(());
        }
        let action_ids: std::collections::BTreeSet<String> = action_ids.iter().cloned().collect();
        let app_store = self.clone();
        self.run_in_batcher(move |wtxn| {
            let app_store = app_store.clone();
            let action_ids = action_ids.clone();
            Box::pin(async move {
                app_store
                    .mark_native_effects_failed_in_txn(wtxn, &action_ids, error_code)
                    .await
            })
        })
        .await
    }

    /// Complete verified effects in the caller's final tracking transaction.
    /// Pending, failed, blocked, and legacy-unverified records cannot be
    /// finalized.
    pub async fn finalize_native_effects_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        action_ids: &[String],
    ) -> Result<()> {
        if action_ids.is_empty() {
            return Ok(());
        }
        self.ensure_native_schema_in_txn(txn).await?;
        let action_ids: std::collections::BTreeSet<String> = action_ids.iter().cloned().collect();
        let mut intents = self
            .load_native_effect_batch_in_txn(txn, &action_ids)
            .await?;
        for intent in &intents {
            if intent.status != NativeEffectStatus::Verified
                && intent.status != NativeEffectStatus::Completed
            {
                client_bail!("native effect must be verified before final commit");
            }
            if intent.verification_policy != NativeVerificationPolicy::QueryVerified {
                client_bail!("legacy native effect cannot be finalized as verified");
            }
        }
        for intent in &mut intents {
            if intent.status != NativeEffectStatus::Completed {
                intent.mark_completed();
                self.write_native_effect_in_txn(txn, intent).await?;
            }
        }
        Ok(())
    }

    /// Resolve provider-missing obligations in the caller's final tracking
    /// transaction. Unknown and non-blocked IDs are idempotent no-ops.
    ///
    /// Only a verified strict recovery can complete a blocked record. A
    /// compatibility recovery (`verified == false`) deliberately leaves the
    /// record blocked and its tracking obligation retained.
    pub async fn resolve_blocked_native_effects_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        action_ids: &[String],
        verified: bool,
    ) -> Result<()> {
        if action_ids.is_empty() || !verified {
            return Ok(());
        }
        self.validate_native_schema_in_txn(txn).await?;
        let action_ids: std::collections::BTreeSet<String> = action_ids.iter().cloned().collect();
        let mut intents = Vec::new();
        for action_id in action_ids {
            let Some(intent) = self.read_native_effect_in_txn(txn, &action_id).await? else {
                continue;
            };
            if intent.status != NativeEffectStatus::Blocked {
                continue;
            }
            if intent.verification_policy != NativeVerificationPolicy::QueryVerified {
                client_bail!("legacy blocked effect cannot be resolved as verified");
            }
            intents.push(intent);
        }
        for mut intent in intents {
            intent.mark_completed();
            self.write_native_effect_in_txn(txn, &intent).await?;
        }
        Ok(())
    }

    /// Read one effect record by its safe action ID.
    pub async fn native_effect(&self, evidence_id: &str) -> Result<Option<NativeEffectIntent>> {
        self.validate_native_schema().await?;
        let rtxn = self.read_txn().await?;
        let key = key_native_effect(evidence_id)?;
        let Some(bytes) = self.db().get(&*rtxn, &key)? else {
            return Ok(None);
        };
        let intent = decode_native_effect(bytes)?;
        if intent.evidence_id() != evidence_id {
            internal_bail!("native effect evidence ID collided with an existing key");
        }
        Ok(Some(intent))
    }

    /// Return one consistent metadata-only snapshot for operator export.
    pub async fn native_effect_snapshot(
        &self,
    ) -> Result<(Option<NativeSchemaVersion>, Vec<NativeEffectIntent>)> {
        let rtxn = self.read_txn().await?;
        let version = self.validate_native_schema_in_ro_txn(&rtxn)?;
        let prefix = key_native_effect_prefix()?;
        let mut effects = Vec::new();
        for entry in self.db().prefix_iter(&rtxn, &prefix)? {
            let (raw_key, raw_value) = entry?;
            let DbEntryKey::NativeEffect(fingerprint) = DbEntryKey::decode(raw_key)? else {
                internal_bail!("unexpected key in native effect keyspace");
            };
            let intent = decode_native_effect(raw_value)?;
            validate_native_effect_key(fingerprint, &intent)?;
            effects.push(intent);
        }
        effects.sort_by(|left, right| left.evidence_id().cmp(right.evidence_id()));
        Ok((version, effects))
    }

    fn referenced_native_effect_ids(
        &self,
        txn: &heed::RoTxn<'_, heed::WithoutTls>,
    ) -> Result<std::collections::HashSet<String>> {
        let mut referenced = std::collections::HashSet::new();

        let lineage_prefix = key_native_effect_lineage_prefix()?;
        for entry in self.db().prefix_iter(txn, &lineage_prefix)? {
            let (_, raw_value) = entry?;
            let cursor = decode_native_effect_lineage(raw_value)?;
            referenced.insert(cursor.current_evidence_id);
        }

        let obligation_prefix = key_native_effect_obligation_prefix()?;
        for entry in self.db().prefix_iter(txn, &obligation_prefix)? {
            let (_, raw_value) = entry?;
            let cursor = decode_native_effect_obligation(raw_value)?;
            referenced.insert(blocked_cleanup_action_id_for_epoch(
                cursor.tracking_locator,
                cursor.source_generation,
                cursor.current_epoch,
            ));
        }

        Ok(referenced)
    }

    async fn compact_completed_native_effects_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        evidence_ids: &[String],
    ) -> Result<NativeEffectCompactionResult> {
        self.validate_native_schema_in_txn(txn).await?;
        self.validate_native_cursor_integrity(&**txn)?;

        let candidates: std::collections::BTreeSet<&str> =
            evidence_ids.iter().map(String::as_str).collect();
        let referenced = self.referenced_native_effect_ids(&**txn)?;
        let mut result = NativeEffectCompactionResult {
            requested: u64::try_from(candidates.len())
                .map_err(|_| internal_error!("native effect compaction count overflow"))?,
            ..NativeEffectCompactionResult::default()
        };

        for evidence_id in candidates {
            let key = key_native_effect(evidence_id)?;
            let Some(raw_value) = self.db().get(&**txn, &key)? else {
                result.already_absent = result
                    .already_absent
                    .checked_add(1)
                    .ok_or_else(|| internal_error!("native effect compaction count overflow"))?;
                continue;
            };
            let intent = decode_native_effect(raw_value)?;
            if intent.evidence_id() != evidence_id {
                internal_bail!("native effect evidence ID collided with an existing key");
            }
            if intent.status != NativeEffectStatus::Completed {
                client_bail!("native effect compaction candidate is not completed");
            }
            if referenced.contains(evidence_id) {
                result.protected = result
                    .protected
                    .checked_add(1)
                    .ok_or_else(|| internal_error!("native effect compaction count overflow"))?;
                continue;
            }
            if self.db().delete(&mut **txn, &key)? {
                result.deleted = result
                    .deleted
                    .checked_add(1)
                    .ok_or_else(|| internal_error!("native effect compaction count overflow"))?;
            }
        }

        Ok(result)
    }

    /// Delete only archive-selected completed records that no live cursor
    /// references. The caller must durably archive the selected IDs first.
    pub async fn compact_completed_native_effects(
        &self,
        evidence_ids: &[String],
    ) -> Result<NativeEffectCompactionResult> {
        let store = self.clone();
        let evidence_ids = evidence_ids.to_vec();
        self.run_in_batcher_typed(move |txn| {
            let store = store.clone();
            let evidence_ids = evidence_ids.clone();
            Box::pin(async move {
                store
                    .compact_completed_native_effects_in_txn(txn, &evidence_ids)
                    .await
            })
        })
        .await
    }

    async fn native_effect_counts_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
    ) -> Result<NativeEffectCounts> {
        self.validate_native_schema_in_txn(txn).await?;
        self.validate_native_cursor_integrity(&**txn)?;
        let prefix = key_native_effect_prefix()?;
        let mut counts = NativeEffectCounts::default();
        for entry in self.db().prefix_iter(&**txn, &prefix)? {
            let (raw_key, raw_value) = entry?;
            let DbEntryKey::NativeEffect(fingerprint) = DbEntryKey::decode(raw_key)? else {
                internal_bail!("unexpected key in native effect keyspace");
            };
            let intent = decode_native_effect(raw_value)?;
            validate_native_effect_key(fingerprint, &intent)?;
            counts.add(intent.status)?;
        }
        Ok(counts)
    }

    /// Metadata-only status totals for inspection and health reporting.
    pub async fn native_effect_counts(&self) -> Result<NativeEffectCounts> {
        self.validate_native_schema().await?;
        let rtxn = self.read_txn().await?;
        self.validate_native_cursor_integrity(&rtxn)?;
        let prefix = key_native_effect_prefix()?;
        let mut counts = NativeEffectCounts::default();
        for entry in self.db().prefix_iter(&*rtxn, &prefix)? {
            let (raw_key, raw_value) = entry?;
            let DbEntryKey::NativeEffect(fingerprint) = DbEntryKey::decode(raw_key)? else {
                internal_bail!("unexpected key in native effect keyspace");
            };
            let intent = decode_native_effect(raw_value)?;
            validate_native_effect_key(fingerprint, &intent)?;
            counts.add(intent.status)?;
        }
        Ok(counts)
    }

    pub async fn has_blocked_native_effects_in_txn(&self, txn: &mut WriteTxn<'_>) -> Result<bool> {
        Ok(self.native_effect_counts_in_txn(txn).await?.blocked != 0)
    }

    pub async fn has_blocked_native_effects(&self) -> Result<bool> {
        Ok(self.native_effect_counts().await?.blocked != 0)
    }

    pub async fn has_unresolved_native_effects_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
    ) -> Result<bool> {
        Ok(self
            .native_effect_counts_in_txn(txn)
            .await?
            .has_unresolved())
    }

    pub async fn has_unresolved_native_effects(&self) -> Result<bool> {
        Ok(self.native_effect_counts().await?.has_unresolved())
    }
}

// --- Inverted target-state owner index -----------------------------------

impl AppStore {
    pub async fn read_target_state_owner_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &TargetStatePath,
    ) -> Result<Option<TargetStateOwnerInfo>> {
        let key = key_target_state_owner(path)?;
        let data = self.db().get(&**txn, &key)?;
        data.map(from_msgpack_slice).transpose().map_err(Into::into)
    }

    pub async fn upsert_target_state_owner(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &TargetStatePath,
        owner: &StablePath,
    ) -> Result<()> {
        let key = key_target_state_owner(path)?;
        let value = rmp_serde::to_vec_named(&TargetStateOwnerInfo {
            component_path: owner.clone(),
        })?;
        self.db().put(&mut **txn, &key, &value)?;
        Ok(())
    }

    pub async fn delete_target_state_owner(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &TargetStatePath,
    ) -> Result<()> {
        let key = key_target_state_owner(path)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }

    /// Persist the readable name for one target-state path segment, keyed by
    /// the lone segment fingerprint. Write-once: an existing entry is left
    /// untouched (the fingerprint is a pure function of the key, so any
    /// existing value is already correct).
    pub async fn write_target_segment_name_if_missing(
        &self,
        txn: &mut WriteTxn<'_>,
        fp: Fingerprint,
        segment_key: &StableKey,
    ) -> Result<()> {
        let key = key_target_segment_name(fp)?;
        if self.db().get(&**txn, &key)?.is_some() {
            return Ok(());
        }
        let value = rmp_serde::to_vec_named(segment_key)?;
        self.db().put(&mut **txn, &key, &value)?;
        Ok(())
    }

    /// Delete every persisted target-segment-name entry. Benchmark support
    /// only: lets the read-path benches measure resolution against stores
    /// written before segment names existed (the fallback-miss shape).
    #[cfg(feature = "bench-support")]
    pub async fn delete_all_target_segment_names(&self, txn: &mut WriteTxn<'_>) -> Result<()> {
        let prefix = DbEntryKey::TargetSegmentNamePrefix.encode()?;
        let db = self.db();
        let mut iter = db.prefix_iter_mut(&mut **txn, &prefix)?;
        while iter.next().transpose()?.is_some() {
            // Safety: we drop the borrowed key/value before the next `next()`.
            unsafe {
                iter.del_current()?;
            }
        }
        Ok(())
    }
}

// --- ID sequencer --------------------------------------------------------

impl AppStore {
    pub async fn peek_id_sequence_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        key: &StableKey,
    ) -> Result<Option<u64>> {
        let db_key = key_id_sequencer(key)?;
        let data = self.db().get(&**txn, &db_key)?;
        match data {
            None => Ok(None),
            Some(bytes) => {
                let info: IdSequencerInfo = from_msgpack_slice(bytes)?;
                Ok(Some(info.next_id))
            }
        }
    }

    pub async fn write_id_sequence(
        &self,
        txn: &mut WriteTxn<'_>,
        key: &StableKey,
        next_id: u64,
    ) -> Result<()> {
        let db_key = key_id_sequencer(key)?;
        let info = IdSequencerInfo { next_id };
        let value = rmp_serde::to_vec_named(&info)?;
        self.db().put(&mut **txn, &db_key, &value)?;
        Ok(())
    }

    /// Atomically reserve `count` consecutive IDs starting from the next
    /// available ID. Returns the first reserved ID. IDs start at 1
    /// (0 is reserved).
    pub async fn reserve_id_range_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        key: &StableKey,
        count: u64,
    ) -> Result<u64> {
        let current_next_id = self
            .peek_id_sequence_in_txn(&mut *txn, key)
            .await?
            .unwrap_or(1);
        self.write_id_sequence(txn, key, current_next_id + count)
            .await?;
        Ok(current_next_id)
    }
}

// --- App-level -----------------------------------------------------------

impl AppStore {
    /// Remove native-only metadata from a separately copied, quiescent app
    /// database after proving that no effect or child cleanup remains open.
    ///
    /// This must never run against the source database. Storage owns the only
    /// call site and invokes it exclusively on an LMDB-consistent staging copy.
    pub(crate) async fn strip_native_metadata_for_downgrade_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
    ) -> Result<NativeEffectDowngradeStripResult> {
        if self.has_unresolved_native_effects_in_txn(txn).await? {
            client_bail!("downgrade blocked by unresolved native effects");
        }
        if self.has_any_tombstones_in_txn(txn).await? {
            client_bail!("downgrade blocked by child cleanup tombstones");
        }

        let schema_key = key_native_schema_version()?;
        let effect_prefix = key_native_effect_prefix()?;
        let obligation_prefix = key_native_effect_obligation_prefix()?;
        let lineage_prefix = key_native_effect_lineage_prefix()?;
        let live_generation_key = key_id_sequencer(&StableKey::Symbol(
            LIVE_COMPONENT_GENERATION_KEY_SYMBOL.into(),
        ))?;

        let mut result = NativeEffectDowngradeStripResult::default();
        let mut iter = self.db().iter_mut(&mut **txn)?;
        while let Some((key, _)) = iter.next().transpose()? {
            let counter = if key == schema_key.as_slice() {
                Some(&mut result.removed_schema_markers)
            } else if key.starts_with(&effect_prefix) {
                Some(&mut result.removed_effects)
            } else if key.starts_with(&obligation_prefix) {
                Some(&mut result.removed_obligation_cursors)
            } else if key.starts_with(&lineage_prefix) {
                Some(&mut result.removed_lineage_cursors)
            } else if key == live_generation_key.as_slice() {
                Some(&mut result.removed_live_generation_keys)
            } else {
                None
            };
            if let Some(counter) = counter {
                *counter = counter
                    .checked_add(1)
                    .ok_or_else(|| internal_error!("native downgrade count overflow"))?;
                // Safety: the borrowed key/value are not used after deleting
                // the cursor's current entry.
                unsafe {
                    iter.del_current()?;
                }
            }
        }
        Ok(result)
    }

    /// Clear operational state while retaining native schema/effect evidence,
    /// obligation cursors, and the live-generation sequencer. Returns `false`
    /// without writing if any effect is not completed or a query-verified
    /// child cleanup obligation remains.
    pub async fn clear_operational_state_in_txn(&self, txn: &mut WriteTxn<'_>) -> Result<bool> {
        if self.has_unresolved_native_effects_in_txn(txn).await?
            || self.has_query_verified_tombstones_in_txn(txn).await?
        {
            return Ok(false);
        }
        let schema_key = key_native_schema_version()?;
        let effect_prefix = key_native_effect_prefix()?;
        let obligation_prefix = key_native_effect_obligation_prefix()?;
        let lineage_prefix = key_native_effect_lineage_prefix()?;
        let live_generation_key = key_id_sequencer(&StableKey::Symbol(
            LIVE_COMPONENT_GENERATION_KEY_SYMBOL.into(),
        ))?;
        let db = self.db();
        let mut iter = db.iter_mut(&mut **txn)?;
        while let Some((key, _)) = iter.next().transpose()? {
            let retain = key == schema_key.as_slice()
                || key.starts_with(&effect_prefix)
                || key.starts_with(&obligation_prefix)
                || key.starts_with(&lineage_prefix)
                || key == live_generation_key.as_slice();
            if !retain {
                // Safety: the borrowed key/value are not used after deleting
                // the cursor's current entry.
                unsafe {
                    iter.del_current()?;
                }
            }
        }
        Ok(true)
    }

    pub async fn clear_all(&self, txn: &mut WriteTxn<'_>) -> Result<()> {
        if !self.clear_operational_state_in_txn(txn).await? {
            client_bail!(
                "app clear blocked by unresolved native effects or query-verified tombstones"
            );
        }
        Ok(())
    }
}

// --- Path node type ------------------------------------------------------

impl AppStore {
    /// Looks up the node type of `parent_path/key` by reading the parent's
    /// child-existence entry. Used by `pre_commit` path-existence checks.
    pub async fn read_path_node_type_in_txn(
        &self,
        txn: &mut WriteTxn<'_>,
        parent_path: StablePathRef<'_>,
        key: &StableKey,
    ) -> Result<Option<StablePathNodeType>> {
        let parent_owned: StablePath = parent_path.into();
        let info = self
            .read_child_existence_in_txn(txn, &parent_owned, key)
            .await?;
        Ok(info.map(|i| i.node_type))
    }

    /// Ensures `parent_path/key` is recorded with `target_node_type`.
    /// Recurses up the ancestor chain creating directory entries as needed.
    ///
    /// Promotion rule:
    /// - missing → write `target_node_type`
    /// - `Directory` + target=`Component` → upgrade to Component
    /// - anything else → no-op
    pub async fn ensure_path_node_type(
        &self,
        txn: &mut WriteTxn<'_>,
        parent_path: StablePathRef<'_>,
        key: &StableKey,
        target_node_type: StablePathNodeType,
    ) -> Result<()> {
        self.ensure_path_node_type_with_generation(txn, parent_path, key, target_node_type, None)
            .await
    }

    async fn ensure_path_node_type_with_generation(
        &self,
        txn: &mut WriteTxn<'_>,
        parent_path: StablePathRef<'_>,
        key: &StableKey,
        target_node_type: StablePathNodeType,
        generation: Option<u64>,
    ) -> Result<()> {
        let parent_owned: StablePath = parent_path.into();
        let existing = self
            .read_child_existence_in_txn(txn, &parent_owned, key)
            .await?;
        let existing_node_type = existing.as_ref().map(|i| i.node_type);
        let should_write = matches!(
            (existing_node_type, target_node_type),
            (None, _)
                | (
                    Some(StablePathNodeType::Directory),
                    StablePathNodeType::Component
                )
        ) || (existing_node_type == Some(StablePathNodeType::Component)
            && target_node_type == StablePathNodeType::Component
            && generation.is_some());
        if should_write {
            self.write_child_existence(
                txn,
                &parent_owned,
                key,
                &ChildExistenceInfo {
                    node_type: target_node_type,
                    generation,
                },
            )
            .await?;
        }
        if existing_node_type.is_none()
            && let Some((parent, key)) = parent_path.split_parent()
        {
            return Box::pin(self.ensure_path_node_type_with_generation(
                txn,
                parent,
                key,
                StablePathNodeType::Directory,
                None,
            ))
            .await;
        }
        Ok(())
    }
}

// --- User state ----------------------------------------------------------

impl AppStore {
    /// Point-read the single `kind` entry under `(path, user_key)` from a
    /// fresh snapshot, or `None` if absent. Used by `read_committed_state`
    /// to fetch one [`StateKind::Live`] key without scanning the prefix.
    pub async fn read_user_state(
        &self,
        path: &StablePath,
        kind: StateKind,
        user_key: &StableKey,
    ) -> Result<Option<Vec<u8>>> {
        let rtxn = self.read_txn().await?;
        let key = key_user_state(path, kind, user_key)?;
        Ok(self.db().get(&*rtxn, &key)?.map(<[u8]>::to_vec))
    }

    /// Read live user state only when the component's durable existence row
    /// still names this exact live incarnation.
    pub async fn read_live_user_state(
        &self,
        path: &StablePath,
        user_key: &StableKey,
        generation: u64,
    ) -> Result<Option<Vec<u8>>> {
        let Some((parent_ref, child_key)) = path.as_ref().split_parent() else {
            client_bail!("live committed state requires a non-root component path");
        };
        let rtxn = self.read_txn().await?;
        let parent: StablePath = parent_ref.into();
        let existence_key = key_child_existence(&parent, child_key)?;
        let Some(existence_bytes) = self.db().get(&*rtxn, &existence_key)? else {
            client_bail!("live component generation is no longer current");
        };
        let existence: ChildExistenceInfo = from_msgpack_slice(existence_bytes)?;
        if existence.generation != Some(generation) {
            client_bail!("live component generation is no longer current");
        }
        let key = key_user_state(path, StateKind::Live, user_key)?;
        Ok(self.db().get(&*rtxn, &key)?.map(<[u8]>::to_vec))
    }

    pub async fn write_user_state(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
        kind: StateKind,
        user_key: &StableKey,
        value: &[u8],
    ) -> Result<()> {
        let key = key_user_state(path, kind, user_key)?;
        self.db().put(&mut **txn, &key, value)?;
        Ok(())
    }

    /// Write a single `kind` user-state entry outside a caller-supplied txn.
    /// Routed through the single-writer batcher so concurrent writers
    /// coalesce (same invariant as the other standalone writers). Used by
    /// the live machinery's `write_committed_state`, which commits a
    /// [`StateKind::Live`] key independently of any component build's flush.
    pub async fn write_user_state_standalone(
        &self,
        path: &StablePath,
        kind: StateKind,
        user_key: &StableKey,
        value: &[u8],
    ) -> Result<()> {
        let app_store = self.clone();
        let path = path.clone();
        let user_key = user_key.clone();
        let value = value.to_vec();
        self.run_in_batcher(move |wtxn| {
            let app_store = app_store.clone();
            let path = path.clone();
            let user_key = user_key.clone();
            let value = value.clone();
            Box::pin(async move {
                app_store
                    .write_user_state(wtxn, &path, kind, &user_key, &value)
                    .await
            })
        })
        .await
    }

    /// Write live user state only if the component's durable generation still
    /// matches. The generation check and value write share one batched write
    /// transaction, preventing a stale incarnation from committing after a
    /// newer existence row becomes visible.
    pub async fn write_live_user_state_standalone(
        &self,
        path: &StablePath,
        user_key: &StableKey,
        value: &[u8],
        generation: u64,
    ) -> Result<()> {
        let app_store = self.clone();
        let path = path.clone();
        let user_key = user_key.clone();
        let value = value.to_vec();
        self.run_in_batcher(move |wtxn| {
            let app_store = app_store.clone();
            let path = path.clone();
            let user_key = user_key.clone();
            let value = value.clone();
            Box::pin(async move {
                let Some((parent_ref, child_key)) = path.as_ref().split_parent() else {
                    client_bail!("live committed state requires a non-root component path");
                };
                let parent: StablePath = parent_ref.into();
                let existence = app_store
                    .read_child_existence_in_txn(wtxn, &parent, child_key)
                    .await?;
                if existence.and_then(|info| info.generation) != Some(generation) {
                    client_bail!("live component generation is no longer current");
                }
                app_store
                    .write_user_state(wtxn, &path, StateKind::Live, &user_key, &value)
                    .await
            })
        })
        .await
    }

    pub async fn delete_user_state(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
        kind: StateKind,
        user_key: &StableKey,
    ) -> Result<()> {
        let key = key_user_state(path, kind, user_key)?;
        self.db().delete(&mut **txn, &key)?;
        Ok(())
    }

    /// Delete every user-state entry of `kind` under `path`. Used by the
    /// regular flush's clear-all (with [`StateKind::Regular`]) and by
    /// whole-component deletion (which clears both kinds).
    pub async fn delete_user_states_of_kind(
        &self,
        txn: &mut WriteTxn<'_>,
        path: &StablePath,
        kind: StateKind,
    ) -> Result<()> {
        let prefix = key_user_state_prefix(path, kind)?;
        let db = self.db();
        let mut iter = db.prefix_iter_mut(&mut **txn, &prefix)?;
        while iter.next().transpose()?.is_some() {
            // Safety: key/value borrows are dropped before the next iteration.
            unsafe {
                iter.del_current()?;
            }
        }
        Ok(())
    }
}

// --- Combined prefetch read ----------------------------------------------

impl AppStore {
    /// List every function-memo and user-state entry under `path` from a
    /// single read snapshot. Used by the per-component prefetch
    /// ([`crate::engine::context::ComponentProcessorContext::prefetch_states`]).
    ///
    /// Both ranges are read under one `RoTxn` rather than two. Under
    /// `MDB_NOTLS` each read-txn begin takes the reader-table mutex, so a
    /// single snapshot halves that cost — most visibly when many child
    /// components prefetch concurrently during `mount_each` fan-out — and
    /// halves concurrent reader-slot occupancy against the
    /// `MDB_READERS_FULL` limit.
    pub async fn prefetch_fn_processing_states(
        &self,
        path: &StablePath,
    ) -> Result<(Vec<(Fingerprint, Vec<u8>)>, Vec<(StableKey, Vec<u8>)>)> {
        let rtxn = self.read_txn().await?;
        let db = self.db();

        // Function memos, keyed by fingerprint.
        let fp_prefix = key_fn_memo_prefix(path)?;
        let mut memos = Vec::new();
        for entry in db.prefix_iter(&*rtxn, &fp_prefix)? {
            let (raw_key, raw_val) = entry?;
            let fp: Fingerprint = storekey::decode(raw_key[fp_prefix.len()..].as_ref())?;
            memos.push((fp, raw_val.to_vec()));
        }

        // User states, keyed by stable key.
        let us_prefix = key_user_state_prefix(path, StateKind::Regular)?;
        let mut states = Vec::new();
        for entry in db.prefix_iter(&*rtxn, &us_prefix)? {
            let (raw_key, raw_val) = entry?;
            let user_key: StableKey = storekey::decode(raw_key[us_prefix.len()..].as_ref())?;
            states.push((user_key, raw_val.to_vec()));
        }

        Ok((memos, states))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppStore, key_child_existence, key_native_effect, key_native_effect_lineage,
        key_native_effect_obligation, key_native_schema_version, key_tombstone,
    };
    use crate::state::db_schema::{
        CHILD_TOMBSTONE_SCHEMA_VERSION, ChildExistenceInfo, ChildTombstoneCause,
        ChildTombstoneInfo, LIVE_COMPONENT_GENERATION_KEY_SYMBOL, NativeSchemaVersion,
        StablePathNodeType, StateKind,
    };
    use crate::state::native_effect::{
        NativeEffectCause, NativeEffectDescriptor, NativeEffectErrorCode, NativeEffectIntent,
        NativeEffectLineageCursor, NativeEffectOperation, NativeEffectStatus,
        NativeVerificationPolicy, blocked_cleanup_action_id,
    };
    use crate::state::stable_path::{StableKey, StablePath};
    use crate::state_store::test_support::make_test_store;
    use crate::state_store::txn::WriteTxn;
    use sha2::{Digest as _, Sha256};
    use std::collections::HashMap;
    use std::fmt::Write as _;
    use std::sync::Arc;
    use synor_utils::fingerprint::Fingerprint;

    fn comp_path(name: &str) -> StablePath {
        StablePath(Arc::from(vec![StableKey::Str(Arc::from(name))]))
    }

    fn sym(s: &str) -> StableKey {
        StableKey::Symbol(Arc::from(s))
    }

    fn to_map(pairs: Vec<(StableKey, Vec<u8>)>) -> HashMap<StableKey, Vec<u8>> {
        pairs.into_iter().collect()
    }

    fn effect_intent(action_id: &str, policy: NativeVerificationPolicy) -> NativeEffectIntent {
        NativeEffectIntent::new(
            NativeEffectDescriptor {
                action_id: action_id.to_owned(),
                operation: NativeEffectOperation::Delete,
                source_digest: "a".repeat(64),
                source_generation: 3,
                target_locator_digest: "b".repeat(64),
            },
            Fingerprint::from_bytes(action_id.as_bytes()),
            policy,
        )
        .unwrap()
    }

    fn provider_missing_intent(
        action_id: String,
        tracking_locator: Fingerprint,
        source_generation: u64,
    ) -> NativeEffectIntent {
        NativeEffectIntent::new(
            NativeEffectDescriptor {
                action_id,
                operation: NativeEffectOperation::Cleanup,
                source_digest: "0".repeat(64),
                source_generation,
                target_locator_digest: "0".repeat(64),
            },
            tracking_locator,
            NativeVerificationPolicy::QueryVerified,
        )
        .unwrap()
        .with_cause(NativeEffectCause::ProviderMissing)
    }

    async fn allocate_and_block(
        store: &AppStore,
        tracking_locator: Fingerprint,
        source_generation: u64,
    ) -> String {
        let store_for_txn = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                Box::pin(async move {
                    let action_id = store
                        .allocate_blocked_cleanup_action_id_in_txn(
                            wtxn,
                            tracking_locator,
                            source_generation,
                        )
                        .await?;
                    let intent = provider_missing_intent(
                        action_id.clone(),
                        tracking_locator,
                        source_generation,
                    );
                    store
                        .upsert_blocked_native_effect_in_txn(
                            wtxn,
                            &intent,
                            NativeEffectErrorCode::ProviderMissing,
                        )
                        .await?;
                    Ok(action_id)
                })
            })
            .await
            .unwrap()
    }

    async fn active_blocker(
        store: &AppStore,
        tracking_locator: Fingerprint,
        source_generation: u64,
    ) -> Option<String> {
        let store_for_txn = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                Box::pin(async move {
                    store
                        .active_blocked_cleanup_action_id_in_txn(
                            wtxn,
                            tracking_locator,
                            source_generation,
                        )
                        .await
                })
            })
            .await
            .unwrap()
    }

    async fn raw_app_entries(store: &AppStore) -> Vec<(Vec<u8>, Vec<u8>)> {
        let rtxn = store.read_txn().await.unwrap();
        store
            .db()
            .iter(&*rtxn)
            .unwrap()
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.to_vec(), value.to_vec())
            })
            .collect()
    }

    async fn bind_and_complete_effect(store: &AppStore, action_id: &str) -> String {
        let proposed = effect_intent(action_id, NativeVerificationPolicy::QueryVerified);
        let store_for_txn = store.clone();
        let proposed_for_txn = proposed.clone();
        let evidence_id = store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let proposed = proposed_for_txn.clone();
                Box::pin(async move {
                    let bound = store
                        .bind_native_effect_lineage_in_txn(wtxn, proposed)
                        .await?;
                    let evidence_id = bound.evidence_id().to_owned();
                    store
                        .upsert_native_effect_intent_in_txn(wtxn, &bound)
                        .await?;
                    Ok(evidence_id)
                })
            })
            .await
            .unwrap();
        store
            .mark_native_effects_verified(std::slice::from_ref(&evidence_id))
            .await
            .unwrap();
        let store_for_txn = store.clone();
        let evidence_for_txn = evidence_id.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let evidence_id = evidence_for_txn.clone();
                Box::pin(async move {
                    store
                        .finalize_native_effects_in_txn(wtxn, &[evidence_id])
                        .await
                })
            })
            .await
            .unwrap();
        evidence_id
    }

    async fn complete_blocker(store: &AppStore, action_id: &str) {
        let store_for_txn = store.clone();
        let action_for_txn = action_id.to_owned();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let action_id = action_for_txn.clone();
                Box::pin(async move {
                    store
                        .resolve_blocked_native_effects_in_txn(wtxn, &[action_id], true)
                        .await
                })
            })
            .await
            .unwrap();
    }

    async fn assert_cursor_corruption_is_fail_closed(store: &AppStore, expected_error: &str) {
        let before = raw_app_entries(store).await;
        assert!(before.iter().any(|(key, value)| {
            key.as_slice() == b"cursor-integrity-operational" && value.as_slice() == b"must-survive"
        }));

        let schema_error = store
            .validate_native_schema()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            schema_error.contains(expected_error),
            "unexpected schema error: {schema_error}"
        );
        assert_eq!(raw_app_entries(store).await, before);

        let counts_error = store.native_effect_counts().await.unwrap_err().to_string();
        assert!(
            counts_error.contains(expected_error),
            "unexpected counts error: {counts_error}"
        );
        assert_eq!(raw_app_entries(store).await, before);

        let drop_error = store
            .storage
            .drop_app("test_app")
            .await
            .unwrap_err()
            .to_string();
        assert!(
            drop_error.contains(expected_error),
            "unexpected drop error: {drop_error}"
        );
        assert_eq!(raw_app_entries(store).await, before);
    }

    /// Read back a component's Regular user states through the production
    /// prefetch path (`prefetch_fn_processing_states`), so these tests assert
    /// against the same read code the engine runs. The fn-memo half of the
    /// result is unused here (these tests write no memos).
    async fn read_regular_states(store: &AppStore, p: &StablePath) -> HashMap<StableKey, Vec<u8>> {
        to_map(store.prefetch_fn_processing_states(p).await.unwrap().1)
    }

    async fn write_tracking_with_token(store: &AppStore, path: &StablePath, token: Option<u128>) {
        use crate::state::db_schema::StablePathEntryTrackingInfo;
        use std::borrow::Cow;

        let mut info = StablePathEntryTrackingInfo::new(Cow::Borrowed("test"));
        info.pending_process_token = token;
        let bytes = rmp_serde::to_vec_named(&info).unwrap();
        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_tracking_info_raw(&mut wtxn, path, &bytes)
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();
    }

    async fn read_pending_process_token(store: &AppStore, path: &StablePath) -> Option<u128> {
        use crate::state::db_schema::StablePathEntryTrackingInfo;

        let bytes = store.read_tracking_info(path).await.unwrap()?;
        let info: StablePathEntryTrackingInfo<'_> =
            synor_utils::deser::from_msgpack_slice(&bytes).unwrap();
        info.pending_process_token
    }

    // --- clear_staged_tracking ---------------------------------------------

    #[tokio::test]
    async fn clear_staged_tracking_clears_matching_token() {
        let (store, _dir) = make_test_store().await;
        let path = comp_path("comp");
        let token = 42u128;

        write_tracking_with_token(&store, &path, Some(token)).await;
        store.clear_staged_tracking(&path, token).await.unwrap();

        assert_eq!(read_pending_process_token(&store, &path).await, None);
    }

    #[tokio::test]
    async fn clear_staged_tracking_leaves_non_matching_token() {
        let (store, _dir) = make_test_store().await;
        let path = comp_path("comp");
        let token = 42u128;

        write_tracking_with_token(&store, &path, Some(token)).await;
        store.clear_staged_tracking(&path, token + 1).await.unwrap();

        assert_eq!(read_pending_process_token(&store, &path).await, Some(token));
    }

    #[tokio::test]
    async fn clear_staged_tracking_missing_entry_is_noop() {
        let (store, _dir) = make_test_store().await;
        let path = comp_path("missing");

        store.clear_staged_tracking(&path, 99).await.unwrap();

        assert_eq!(read_pending_process_token(&store, &path).await, None);
    }

    // --- user state read-back (via prefetch) -------------------------------

    #[tokio::test]
    async fn user_states_empty_on_fresh_path() {
        let (store, _dir) = make_test_store().await;
        let result = read_regular_states(&store, &comp_path("comp")).await;
        assert!(result.is_empty());
    }

    // --- write_user_state + list -------------------------------------------

    #[tokio::test]
    async fn write_then_list_returns_all_entries() {
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, StateKind::Regular, &sym("count"), b"42")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, StateKind::Regular, &sym("name"), b"hello")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, StateKind::Regular, &sym("flag"), b"true")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let entries = read_regular_states(&store, &p).await;
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[&sym("count")], b"42");
        assert_eq!(entries[&sym("name")], b"hello");
        assert_eq!(entries[&sym("flag")], b"true");
    }

    #[tokio::test]
    async fn write_overwrites_existing_entry() {
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, StateKind::Regular, &sym("k"), b"v1")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, StateKind::Regular, &sym("k"), b"v2")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let entries = read_regular_states(&store, &p).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[&sym("k")], b"v2");
    }

    // --- delete_user_state -------------------------------------------------

    #[tokio::test]
    async fn delete_selective_within_flush_txn() {
        // A, B, C are written; a second txn updates A and deletes B in one
        // atomic operation; C is untouched.
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, StateKind::Regular, &sym("a"), b"old_a")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, StateKind::Regular, &sym("b"), b"b_val")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, StateKind::Regular, &sym("c"), b"c_val")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        // write and delete are atomic within the same txn.
        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, StateKind::Regular, &sym("a"), b"new_a")
            .await
            .unwrap();
        store
            .delete_user_state(&mut wtxn, &p, StateKind::Regular, &sym("b"))
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let entries = read_regular_states(&store, &p).await;
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[&sym("a")], b"new_a");
        assert!(!entries.contains_key(&sym("b")));
        assert_eq!(entries[&sym("c")], b"c_val");
    }

    // --- delete_user_states_of_kind ----------------------------------------

    #[tokio::test]
    async fn delete_all_then_write_within_flush_txn() {
        // A, B, C are written; a second txn calls delete_all then writes
        // A (new value) and D (new key) — all atomically. B and C must be gone.
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, StateKind::Regular, &sym("a"), b"old_a")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, StateKind::Regular, &sym("b"), b"b_val")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, StateKind::Regular, &sym("c"), b"c_val")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        // delete_all and subsequent writes are atomic within the same txn.
        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .delete_user_states_of_kind(&mut wtxn, &p, StateKind::Regular)
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, StateKind::Regular, &sym("a"), b"new_a")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, StateKind::Regular, &sym("d"), b"d_val")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let entries = read_regular_states(&store, &p).await;
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[&sym("a")], b"new_a");
        assert!(!entries.contains_key(&sym("b")));
        assert!(!entries.contains_key(&sym("c")));
        assert_eq!(entries[&sym("d")], b"d_val");
    }

    // --- isolation ---------------------------------------------------------

    #[tokio::test]
    async fn user_states_isolated_by_path() {
        let (store, _dir) = make_test_store().await;
        let p1 = comp_path("comp_a");
        let p2 = comp_path("comp_b");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p1, StateKind::Regular, &sym("k"), b"from_a")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p2, StateKind::Regular, &sym("k"), b"from_b")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        let r1 = read_regular_states(&store, &p1).await;
        let r2 = read_regular_states(&store, &p2).await;
        assert_eq!(r1.len(), 1);
        assert_eq!(r2.len(), 1);
        assert_eq!(r1[&sym("k")], b"from_a");
        assert_eq!(r2[&sym("k")], b"from_b");
    }

    // --- kind isolation ----------------------------------------------------

    #[tokio::test]
    async fn user_states_isolated_by_kind() {
        // Regular and Live share the component path and even the same user
        // key, but never collide: the Regular bulk read excludes Live, point
        // reads resolve per kind, and a Regular bulk-delete leaves Live intact.
        // (Live has no bulk reader by design — production point-reads it.)
        let (store, _dir) = make_test_store().await;
        let p = comp_path("comp");

        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .write_user_state(&mut wtxn, &p, StateKind::Regular, &sym("k"), b"reg")
            .await
            .unwrap();
        store
            .write_user_state(&mut wtxn, &p, StateKind::Live, &sym("k"), b"live")
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        // The Regular bulk read sees only the Regular entry, never the Live
        // one written under the same key.
        let reg = read_regular_states(&store, &p).await;
        assert_eq!(reg.len(), 1);
        assert_eq!(reg[&sym("k")], b"reg");

        // Point-read resolves per kind for the shared key, and misses absent.
        assert_eq!(
            store
                .read_user_state(&p, StateKind::Regular, &sym("k"))
                .await
                .unwrap()
                .as_deref(),
            Some(&b"reg"[..])
        );
        assert_eq!(
            store
                .read_user_state(&p, StateKind::Live, &sym("k"))
                .await
                .unwrap()
                .as_deref(),
            Some(&b"live"[..])
        );
        assert_eq!(
            store
                .read_user_state(&p, StateKind::Live, &sym("absent"))
                .await
                .unwrap(),
            None
        );

        // Clearing the Regular keyspace must not touch Live (the live
        // bootstrap state survives a component's regular flush).
        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .delete_user_states_of_kind(&mut wtxn, &p, StateKind::Regular)
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();

        assert!(read_regular_states(&store, &p).await.is_empty());
        assert_eq!(
            store
                .read_user_state(&p, StateKind::Live, &sym("k"))
                .await
                .unwrap()
                .as_deref(),
            Some(&b"live"[..])
        );

        // Clearing Live too leaves the component with no user state.
        let mut wtxn = WriteTxn::new(store.env.write_txn().unwrap());
        store
            .delete_user_states_of_kind(&mut wtxn, &p, StateKind::Live)
            .await
            .unwrap();
        wtxn.into_inner().commit().unwrap();
        assert!(
            store
                .read_user_state(&p, StateKind::Live, &sym("k"))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn native_effect_requires_verified_finalization_and_retains_evidence() {
        let (store, _dir) = make_test_store().await;
        assert_eq!(store.validate_native_schema().await.unwrap(), None);
        assert_eq!(store.native_effect_counts().await.unwrap().pending, 0);

        let intent = effect_intent(
            "delete:tenant.source.3",
            NativeVerificationPolicy::QueryVerified,
        );
        let action_id = intent.descriptor.action_id.clone();
        let store_for_txn = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let intent = intent.clone();
                Box::pin(async move {
                    store
                        .upsert_native_effect_intent_in_txn(wtxn, &intent)
                        .await
                })
            })
            .await
            .unwrap();
        let pending = store.native_effect(&action_id).await.unwrap().unwrap();
        assert_eq!(pending.status, NativeEffectStatus::Pending);
        assert_eq!(pending.attempt_count, 1);

        let ids = vec![action_id.clone()];
        store.mark_native_effects_verified(&ids).await.unwrap();
        assert_eq!(
            store
                .native_effect(&action_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            NativeEffectStatus::Verified
        );

        let store_for_txn = store.clone();
        let ids_for_txn = ids.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let ids = ids_for_txn.clone();
                Box::pin(async move { store.finalize_native_effects_in_txn(wtxn, &ids).await })
            })
            .await
            .unwrap();
        assert_eq!(
            store
                .native_effect(&action_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            NativeEffectStatus::Completed
        );
        assert_eq!(store.native_effect_counts().await.unwrap().completed, 1);
    }

    #[tokio::test]
    async fn ordinary_effect_lineage_requires_exact_retry_and_allocates_successor_evidence() {
        let (store, _dir) = make_test_store().await;
        let proposed = effect_intent(
            "delete:connector-visible",
            NativeVerificationPolicy::QueryVerified,
        );
        let tracking_locator = proposed.tracking_locator;

        let store_for_txn = store.clone();
        let proposed_for_txn = proposed.clone();
        let first_evidence_id = store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let proposed = proposed_for_txn.clone();
                Box::pin(async move {
                    let bound = store
                        .bind_native_effect_lineage_in_txn(wtxn, proposed)
                        .await?;
                    let evidence_id = bound.evidence_id().to_owned();
                    store
                        .upsert_native_effect_intent_in_txn(wtxn, &bound)
                        .await?;
                    Ok(evidence_id)
                })
            })
            .await
            .unwrap();
        assert_ne!(first_evidence_id, proposed.descriptor.action_id);

        store
            .mark_native_effects_verified(std::slice::from_ref(&first_evidence_id))
            .await
            .unwrap();

        let store_for_txn = store.clone();
        let active_id = store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                Box::pin(async move {
                    store
                        .active_native_effect_id_for_locator_in_txn(wtxn, tracking_locator)
                        .await
                })
            })
            .await
            .unwrap();
        assert_eq!(active_id.as_deref(), Some(first_evidence_id.as_str()));

        let mut changed = proposed.clone();
        changed.descriptor.action_id = "delete:changed-contract".to_owned();
        let store_for_txn = store.clone();
        assert!(
            store
                .storage
                .run_txn(move |wtxn| {
                    let store = store_for_txn.clone();
                    let changed = changed.clone();
                    Box::pin(async move {
                        store
                            .bind_native_effect_lineage_in_txn(wtxn, changed)
                            .await
                            .map(|_| ())
                    })
                })
                .await
                .is_err()
        );

        let store_for_txn = store.clone();
        let proposed_for_txn = proposed.clone();
        let retry_evidence_id = store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let proposed = proposed_for_txn.clone();
                Box::pin(async move {
                    store
                        .bind_native_effect_lineage_in_txn(wtxn, proposed)
                        .await
                        .map(|bound| bound.evidence_id().to_owned())
                })
            })
            .await
            .unwrap();
        assert_eq!(retry_evidence_id, first_evidence_id);

        let store_for_txn = store.clone();
        let first_for_txn = first_evidence_id.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let evidence_id = first_for_txn.clone();
                Box::pin(async move {
                    store
                        .finalize_native_effects_in_txn(wtxn, &[evidence_id])
                        .await
                })
            })
            .await
            .unwrap();

        // Simulate App.drop's operational clear. Completed evidence and its
        // lineage cursor must survive so a reused App allocates a successor.
        let store_for_txn = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                Box::pin(async move {
                    assert!(store.clear_operational_state_in_txn(wtxn).await?);
                    Ok(())
                })
            })
            .await
            .unwrap();

        let store_for_txn = store.clone();
        let proposed_for_txn = proposed.clone();
        let second_evidence_id = store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let proposed = proposed_for_txn.clone();
                Box::pin(async move {
                    let bound = store
                        .bind_native_effect_lineage_in_txn(wtxn, proposed)
                        .await?;
                    let evidence_id = bound.evidence_id().to_owned();
                    store
                        .upsert_native_effect_intent_in_txn(wtxn, &bound)
                        .await?;
                    Ok(evidence_id)
                })
            })
            .await
            .unwrap();
        assert_ne!(second_evidence_id, first_evidence_id);
        assert_eq!(
            store
                .native_effect(&first_evidence_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            NativeEffectStatus::Completed
        );
        assert_eq!(
            store
                .native_effect(&second_evidence_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            NativeEffectStatus::Pending
        );
    }

    #[tokio::test]
    async fn archived_compaction_deletes_only_unreferenced_completed_evidence() {
        let (store, _dir) = make_test_store().await;
        let first = bind_and_complete_effect(&store, "delete:retention-lineage").await;
        let second = bind_and_complete_effect(&store, "delete:retention-lineage").await;
        assert_ne!(first, second);

        let tracking_locator = Fingerprint::from_bytes(b"retention-obligation");
        let blocker = allocate_and_block(&store, tracking_locator, 17).await;
        complete_blocker(&store, &blocker).await;

        let (schema, exported) = store.native_effect_snapshot().await.unwrap();
        assert_eq!(schema, Some(NativeSchemaVersion::CURRENT));
        assert_eq!(exported.len(), 3);
        assert!(
            exported
                .windows(2)
                .all(|pair| { pair[0].evidence_id().cmp(pair[1].evidence_id()).is_le() })
        );

        let candidates = vec![first.clone(), second.clone(), blocker.clone()];
        let result = store
            .compact_completed_native_effects(&candidates)
            .await
            .unwrap();
        assert_eq!(result.requested, 3);
        assert_eq!(result.deleted, 1);
        assert_eq!(result.protected, 2);
        assert_eq!(result.already_absent, 0);
        assert!(store.native_effect(&first).await.unwrap().is_none());
        assert!(store.native_effect(&second).await.unwrap().is_some());
        assert!(store.native_effect(&blocker).await.unwrap().is_some());

        let repeated = store
            .compact_completed_native_effects(&candidates)
            .await
            .unwrap();
        assert_eq!(repeated.deleted, 0);
        assert_eq!(repeated.protected, 2);
        assert_eq!(repeated.already_absent, 1);
    }

    #[tokio::test]
    async fn lineage_cursor_missing_evidence_blocks_reads_and_drop_without_mutation() {
        let (store, _dir) = make_test_store().await;
        let evidence_id = bind_and_complete_effect(&store, "delete:cursor-missing-evidence").await;
        let evidence_key = key_native_effect(&evidence_id).unwrap();
        let store_for_txn = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let evidence_key = evidence_key.clone();
                Box::pin(async move {
                    assert!(store.db().delete(&mut **wtxn, &evidence_key)?);
                    store.db().put(
                        &mut **wtxn,
                        b"cursor-integrity-operational",
                        b"must-survive",
                    )?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        assert_cursor_corruption_is_fail_closed(
            &store,
            "native effect lineage references missing evidence",
        )
        .await;
    }

    #[tokio::test]
    async fn lineage_cursor_mismatched_evidence_blocks_reads_and_drop_without_mutation() {
        let (store, _dir) = make_test_store().await;
        let first_action_id = "delete:cursor-mismatched-first";
        let first_tracking_locator = Fingerprint::from_bytes(first_action_id.as_bytes());
        bind_and_complete_effect(&store, first_action_id).await;
        let second_evidence_id =
            bind_and_complete_effect(&store, "delete:cursor-mismatched-second").await;

        let mismatched_cursor =
            NativeEffectLineageCursor::new(first_tracking_locator, 1, second_evidence_id).unwrap();
        let cursor_key = key_native_effect_lineage(first_tracking_locator).unwrap();
        let cursor_value = rmp_serde::to_vec_named(&mismatched_cursor).unwrap();
        let store_for_txn = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let cursor_key = cursor_key.clone();
                let cursor_value = cursor_value.clone();
                Box::pin(async move {
                    store.db().put(&mut **wtxn, &cursor_key, &cursor_value)?;
                    store.db().put(
                        &mut **wtxn,
                        b"cursor-integrity-operational",
                        b"must-survive",
                    )?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        assert_cursor_corruption_is_fail_closed(
            &store,
            "native effect lineage references mismatched evidence",
        )
        .await;
    }

    #[tokio::test]
    async fn obligation_cursor_missing_evidence_blocks_reads_and_drop_without_mutation() {
        let (store, _dir) = make_test_store().await;
        let tracking_locator = Fingerprint::from_bytes(b"obligation-missing-evidence");
        let generation = 23;
        let action_id = allocate_and_block(&store, tracking_locator, generation).await;
        complete_blocker(&store, &action_id).await;

        let evidence_key = key_native_effect(&action_id).unwrap();
        let store_for_txn = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let evidence_key = evidence_key.clone();
                Box::pin(async move {
                    assert!(store.db().delete(&mut **wtxn, &evidence_key)?);
                    store.db().put(
                        &mut **wtxn,
                        b"cursor-integrity-operational",
                        b"must-survive",
                    )?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        assert_cursor_corruption_is_fail_closed(
            &store,
            "native cleanup obligation cursor references missing evidence",
        )
        .await;
    }

    #[tokio::test]
    async fn obligation_cursor_forged_evidence_blocks_reads_and_drop_without_mutation() {
        let (store, _dir) = make_test_store().await;
        let tracking_locator = Fingerprint::from_bytes(b"obligation-forged-evidence");
        let generation = 29;
        let action_id = allocate_and_block(&store, tracking_locator, generation).await;
        complete_blocker(&store, &action_id).await;

        let mut forged = store.native_effect(&action_id).await.unwrap().unwrap();
        assert_eq!(forged.status, NativeEffectStatus::Completed);
        forged.cause = NativeEffectCause::Explicit;
        let evidence_key = key_native_effect(&action_id).unwrap();
        let evidence_value = rmp_serde::to_vec_named(&forged).unwrap();
        let store_for_txn = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let evidence_key = evidence_key.clone();
                let evidence_value = evidence_value.clone();
                Box::pin(async move {
                    store
                        .db()
                        .put(&mut **wtxn, &evidence_key, &evidence_value)?;
                    store.db().put(
                        &mut **wtxn,
                        b"cursor-integrity-operational",
                        b"must-survive",
                    )?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        assert_cursor_corruption_is_fail_closed(
            &store,
            "native cleanup obligation ID is bound to different metadata",
        )
        .await;
    }

    #[tokio::test]
    async fn blocked_effect_resolution_is_strict_and_idempotent() {
        let (store, _dir) = make_test_store().await;
        let intent = effect_intent(
            "cleanup:provider-missing",
            NativeVerificationPolicy::QueryVerified,
        );
        let action_id = intent.descriptor.action_id.clone();
        let store_for_txn = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let intent = intent.clone();
                Box::pin(async move {
                    store
                        .upsert_blocked_native_effect_in_txn(
                            wtxn,
                            &intent,
                            NativeEffectErrorCode::ProviderMissing,
                        )
                        .await
                })
            })
            .await
            .unwrap();

        let ids = vec![action_id.clone(), "unknown:effect".to_owned()];
        let store_for_txn = store.clone();
        let ids_for_txn = ids.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let ids = ids_for_txn.clone();
                Box::pin(async move {
                    store
                        .resolve_blocked_native_effects_in_txn(wtxn, &ids, false)
                        .await
                })
            })
            .await
            .unwrap();
        assert!(store.has_blocked_native_effects().await.unwrap());

        let store_for_txn = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let ids = ids.clone();
                Box::pin(async move {
                    store
                        .resolve_blocked_native_effects_in_txn(wtxn, &ids, true)
                        .await
                })
            })
            .await
            .unwrap();
        assert!(!store.has_blocked_native_effects().await.unwrap());
        assert_eq!(
            store
                .native_effect(&action_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            NativeEffectStatus::Completed
        );
    }

    #[tokio::test]
    async fn repeated_provider_missing_lifecycle_allocates_new_stable_evidence() {
        let (store, _dir) = make_test_store().await;
        let tracking_locator = Fingerprint::from_bytes(b"repeated-provider-obligation");
        let generation = 11;

        let first = allocate_and_block(&store, tracking_locator, generation).await;
        assert_eq!(
            first,
            blocked_cleanup_action_id(tracking_locator, generation)
        );
        let store_for_txn = store.clone();
        let first_for_txn = first.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let first = first_for_txn.clone();
                Box::pin(async move {
                    store
                        .resolve_blocked_native_effects_in_txn(wtxn, &[first], true)
                        .await
                })
            })
            .await
            .unwrap();
        assert_eq!(
            active_blocker(&store, tracking_locator, generation).await,
            None
        );

        let second = allocate_and_block(&store, tracking_locator, generation).await;
        assert_ne!(first, second);
        assert_eq!(
            allocate_and_block(&store, tracking_locator, generation).await,
            second
        );
        let counts = store.native_effect_counts().await.unwrap();
        assert_eq!(counts.completed, 1);
        assert_eq!(counts.blocked, 1);
        assert_eq!(
            store.native_effect(&first).await.unwrap().unwrap().status,
            NativeEffectStatus::Completed
        );
        assert_eq!(
            store.native_effect(&second).await.unwrap().unwrap().status,
            NativeEffectStatus::Blocked
        );

        let active = active_blocker(&store, tracking_locator, generation).await;
        assert_eq!(active.as_deref(), Some(second.as_str()));

        let store_for_txn = store.clone();
        let second_for_txn = second.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let second = second_for_txn.clone();
                Box::pin(async move {
                    store
                        .resolve_blocked_native_effects_in_txn(wtxn, &[second], true)
                        .await?;
                    assert!(store.clear_operational_state_in_txn(wtxn).await?);
                    Ok(())
                })
            })
            .await
            .unwrap();
        let rtxn = store.read_txn().await.unwrap();
        assert!(
            store
                .db()
                .get(
                    &*rtxn,
                    &key_native_effect_obligation(tracking_locator, generation).unwrap(),
                )
                .unwrap()
                .is_some()
        );
        drop(rtxn);

        let third = allocate_and_block(&store, tracking_locator, generation).await;
        assert_ne!(second, third);
        let counts = store.native_effect_counts().await.unwrap();
        assert_eq!(counts.completed, 2);
        assert_eq!(counts.blocked, 1);
    }

    #[tokio::test]
    async fn operational_clear_retains_live_generation_sequencer() {
        let (store, _dir) = make_test_store().await;
        let generation_key = StableKey::Symbol(LIVE_COMPONENT_GENERATION_KEY_SYMBOL.into());
        assert_eq!(store.reserve_id_range(&generation_key, 1).await.unwrap(), 1);

        let child_key = StableKey::Str(Arc::from("operational-child"));
        let store_for_txn = store.clone();
        let child_key_for_txn = child_key.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let child_key = child_key_for_txn.clone();
                Box::pin(async move {
                    store
                        .write_child_existence(
                            wtxn,
                            &StablePath::root(),
                            &child_key,
                            &ChildExistenceInfo {
                                node_type: StablePathNodeType::Component,
                                generation: Some(1),
                            },
                        )
                        .await?;
                    assert!(store.clear_operational_state_in_txn(wtxn).await?);
                    Ok(())
                })
            })
            .await
            .unwrap();

        assert_eq!(store.reserve_id_range(&generation_key, 1).await.unwrap(), 2);
        let rtxn = store.read_txn().await.unwrap();
        assert!(
            store
                .db()
                .get(
                    &*rtxn,
                    &key_child_existence(&StablePath::root(), &child_key).unwrap(),
                )
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn legacy_completed_blocker_without_cursor_advances_to_a_new_epoch() {
        let (store, _dir) = make_test_store().await;
        let tracking_locator = Fingerprint::from_bytes(b"legacy-provider-obligation");
        let generation = 17;
        let first = blocked_cleanup_action_id(tracking_locator, generation);
        let intent = provider_missing_intent(first.clone(), tracking_locator, generation);
        let schema_key = key_native_schema_version().unwrap();
        let schema_v1 = rmp_serde::to_vec_named(&NativeSchemaVersion(1)).unwrap();
        let store_for_txn = store.clone();
        let first_for_txn = first.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let intent = intent.clone();
                let first = first_for_txn.clone();
                let schema_key = schema_key.clone();
                let schema_v1 = schema_v1.clone();
                Box::pin(async move {
                    store
                        .upsert_blocked_native_effect_in_txn(
                            wtxn,
                            &intent,
                            NativeEffectErrorCode::ProviderMissing,
                        )
                        .await?;
                    store
                        .resolve_blocked_native_effects_in_txn(wtxn, &[first], true)
                        .await?;
                    // Model a schema-v1 database written before allocation
                    // cursors existed.
                    store.db().put(&mut **wtxn, &schema_key, &schema_v1)?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let second = allocate_and_block(&store, tracking_locator, generation).await;
        assert_ne!(first, second);
        assert_eq!(
            store.native_effect(&first).await.unwrap().unwrap().status,
            NativeEffectStatus::Completed
        );
        assert_eq!(
            store.validate_native_schema().await.unwrap(),
            Some(NativeSchemaVersion::CURRENT)
        );
        let counts = store.native_effect_counts().await.unwrap();
        assert_eq!(counts.completed, 1);
        assert_eq!(counts.blocked, 1);
    }

    #[tokio::test]
    async fn legacy_blocked_effect_without_cursor_is_discovered_by_recovery_lookup() {
        let (store, _dir) = make_test_store().await;
        let tracking_locator = Fingerprint::from_bytes(b"legacy-active-provider-obligation");
        let generation = 19;
        let first = blocked_cleanup_action_id(tracking_locator, generation);
        let intent = provider_missing_intent(first.clone(), tracking_locator, generation);
        let schema_key = key_native_schema_version().unwrap();
        let schema_v1 = rmp_serde::to_vec_named(&NativeSchemaVersion(1)).unwrap();
        let store_for_txn = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let intent = intent.clone();
                let schema_key = schema_key.clone();
                let schema_v1 = schema_v1.clone();
                Box::pin(async move {
                    store
                        .upsert_blocked_native_effect_in_txn(
                            wtxn,
                            &intent,
                            NativeEffectErrorCode::ProviderMissing,
                        )
                        .await?;
                    store.db().put(&mut **wtxn, &schema_key, &schema_v1)?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        assert_eq!(
            active_blocker(&store, tracking_locator, generation)
                .await
                .as_deref(),
            Some(first.as_str())
        );
        assert_eq!(
            store.validate_native_schema().await.unwrap(),
            Some(NativeSchemaVersion::CURRENT)
        );
        let rtxn = store.read_txn().await.unwrap();
        assert!(
            store
                .db()
                .get(
                    &*rtxn,
                    &key_native_effect_obligation(tracking_locator, generation).unwrap(),
                )
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn future_native_schema_refuses_reads_and_writes() {
        let (store, _dir) = make_test_store().await;
        let schema_key = key_native_schema_version().unwrap();
        let future_schema = rmp_serde::to_vec_named(&NativeSchemaVersion(4)).unwrap();
        let store_for_txn = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let schema_key = schema_key.clone();
                let future_schema = future_schema.clone();
                Box::pin(async move {
                    store.db().put(&mut **wtxn, &schema_key, &future_schema)?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        assert!(store.validate_native_schema().await.is_err());
        let intent = effect_intent(
            "delete:future-schema",
            NativeVerificationPolicy::QueryVerified,
        );
        let store_for_txn = store.clone();
        assert!(
            store
                .storage
                .run_txn(move |wtxn| {
                    let store = store_for_txn.clone();
                    let intent = intent.clone();
                    Box::pin(async move {
                        store
                            .upsert_native_effect_intent_in_txn(wtxn, &intent)
                            .await
                    })
                })
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn native_schema_v1_is_readable_and_upgrades_before_write() {
        let (store, _dir) = make_test_store().await;
        let schema_key = key_native_schema_version().unwrap();
        let schema_v1 = rmp_serde::to_vec_named(&NativeSchemaVersion(1)).unwrap();
        let store_for_txn = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let schema_key = schema_key.clone();
                let schema_v1 = schema_v1.clone();
                Box::pin(async move {
                    store.db().put(&mut **wtxn, &schema_key, &schema_v1)?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        assert_eq!(
            store.validate_native_schema().await.unwrap(),
            Some(NativeSchemaVersion(1))
        );

        let intent = effect_intent(
            "delete:schema-upgrade",
            NativeVerificationPolicy::QueryVerified,
        );
        let store_for_txn = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let intent = intent.clone();
                Box::pin(async move {
                    store
                        .upsert_native_effect_intent_in_txn(wtxn, &intent)
                        .await
                })
            })
            .await
            .unwrap();
        assert_eq!(
            store.validate_native_schema().await.unwrap(),
            Some(NativeSchemaVersion::CURRENT)
        );
    }

    #[tokio::test]
    async fn remove_child_with_tombstone_uses_persisted_generation_over_fallback() {
        let (store, _dir) = make_test_store().await;
        let parent = comp_path("owner");
        let child_key = StableKey::Str(Arc::from("child"));
        let relative = comp_path("child");
        let store_for_txn = store.clone();
        let parent_for_txn = parent.clone();
        let child_key_for_txn = child_key.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let parent = parent_for_txn.clone();
                let child_key = child_key_for_txn.clone();
                Box::pin(async move {
                    store
                        .write_child_existence(
                            wtxn,
                            &parent,
                            &child_key,
                            &ChildExistenceInfo {
                                node_type: StablePathNodeType::Component,
                                generation: Some(41),
                            },
                        )
                        .await
                })
            })
            .await
            .unwrap();

        let store_for_txn = store.clone();
        let parent_for_txn = parent.clone();
        let child_key_for_txn = child_key.clone();
        let relative_for_txn = relative.clone();
        let tombstone = store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let parent = parent_for_txn.clone();
                let child_key = child_key_for_txn.clone();
                let relative = relative_for_txn.clone();
                Box::pin(async move {
                    store
                        .remove_child_with_tombstone(
                            wtxn,
                            &parent,
                            &child_key,
                            &parent,
                            &relative,
                            ChildTombstoneCause::LiveDelete,
                            None,
                            Some(7),
                            NativeVerificationPolicy::QueryVerified,
                        )
                        .await
                })
            })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(tombstone.generation, Some(41));
        let rtxn = store.read_txn().await.unwrap();
        assert!(
            store
                .db()
                .get(&*rtxn, &key_child_existence(&parent, &child_key).unwrap(),)
                .unwrap()
                .is_none()
        );
        drop(rtxn);
        let persisted = store.list_tombstones(&parent).await.unwrap();
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].1.generation, Some(41));
    }

    #[tokio::test]
    async fn newer_tombstone_rejects_older_delete_without_removing_existence() {
        let (store, _dir) = make_test_store().await;
        let parent = comp_path("owner");
        let child_key = StableKey::Str(Arc::from("child"));
        let relative = comp_path("child");
        let newer_tombstone = ChildTombstoneInfo::new(
            ChildTombstoneCause::LiveDelete,
            None,
            Some(13),
            NativeVerificationPolicy::QueryVerified,
        )
        .unwrap();
        let store_for_txn = store.clone();
        let parent_for_txn = parent.clone();
        let child_key_for_txn = child_key.clone();
        let relative_for_txn = relative.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let parent = parent_for_txn.clone();
                let child_key = child_key_for_txn.clone();
                let relative = relative_for_txn.clone();
                let newer_tombstone = newer_tombstone.clone();
                Box::pin(async move {
                    store
                        .write_child_existence(
                            wtxn,
                            &parent,
                            &child_key,
                            &ChildExistenceInfo {
                                node_type: StablePathNodeType::Component,
                                generation: Some(12),
                            },
                        )
                        .await?;
                    store
                        .write_tombstone(wtxn, &parent, &relative, &newer_tombstone)
                        .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let store_for_txn = store.clone();
        let parent_for_txn = parent.clone();
        let child_key_for_txn = child_key.clone();
        let relative_for_txn = relative.clone();
        let result = store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let parent = parent_for_txn.clone();
                let child_key = child_key_for_txn.clone();
                let relative = relative_for_txn.clone();
                Box::pin(async move {
                    store
                        .remove_child_with_tombstone(
                            wtxn,
                            &parent,
                            &child_key,
                            &parent,
                            &relative,
                            ChildTombstoneCause::LiveDelete,
                            None,
                            Some(5),
                            NativeVerificationPolicy::QueryVerified,
                        )
                        .await
                })
            })
            .await
            .unwrap();
        assert!(result.is_none());

        let rtxn = store.read_txn().await.unwrap();
        let existence_bytes = store
            .db()
            .get(&*rtxn, &key_child_existence(&parent, &child_key).unwrap())
            .unwrap()
            .unwrap();
        let existence: ChildExistenceInfo =
            synor_utils::deser::from_msgpack_slice(existence_bytes).unwrap();
        assert_eq!(existence.generation, Some(12));
        drop(rtxn);
        let persisted = store.list_tombstones(&parent).await.unwrap();
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].1.generation, Some(13));
        assert_eq!(persisted[0].1.attempt_count, 1);
    }

    #[tokio::test]
    async fn legacy_tombstone_decodes_and_newer_generation_survives_stale_cleanup() {
        let (store, _dir) = make_test_store().await;
        let parent = comp_path("owner");
        let relative = comp_path("child");
        let raw_key = key_tombstone(&parent, &relative).unwrap();
        let store_for_txn = store.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let raw_key = raw_key.clone();
                Box::pin(async move {
                    store.db().put(&mut **wtxn, &raw_key, &[])?;
                    Ok(())
                })
            })
            .await
            .unwrap();
        let legacy = store.list_tombstones(&parent).await.unwrap();
        assert_eq!(legacy[0].1, ChildTombstoneInfo::default());

        let tombstone = ChildTombstoneInfo::new(
            ChildTombstoneCause::LiveDelete,
            Some("c".repeat(64)),
            Some(2),
            NativeVerificationPolicy::QueryVerified,
        )
        .unwrap();
        let retry_tombstone = tombstone.clone();
        let store_for_txn = store.clone();
        let parent_for_txn = parent.clone();
        let relative_for_txn = relative.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let parent = parent_for_txn.clone();
                let relative = relative_for_txn.clone();
                let tombstone = tombstone.clone();
                Box::pin(async move {
                    store
                        .write_tombstone(wtxn, &parent, &relative, &tombstone)
                        .await
                })
            })
            .await
            .unwrap();

        let written_tombstones = store.list_tombstones(&parent).await.unwrap();
        let written = &written_tombstones[0].1;
        assert_eq!(written.attempt_count, 1);
        assert_eq!(written.last_error_code, None);
        assert!(
            !store
                .mark_tombstone_failed(
                    &parent,
                    &relative,
                    Some(1),
                    NativeEffectErrorCode::CleanupFailed,
                )
                .await
                .unwrap()
        );
        assert!(
            store
                .mark_tombstone_failed(
                    &parent,
                    &relative,
                    Some(2),
                    NativeEffectErrorCode::CleanupFailed,
                )
                .await
                .unwrap()
        );
        let failed_tombstones = store.list_tombstones(&parent).await.unwrap();
        let failed = &failed_tombstones[0].1;
        assert_eq!(
            failed.last_error_code,
            Some(NativeEffectErrorCode::CleanupFailed)
        );

        store
            .retry_tombstone(&parent, &relative, &retry_tombstone)
            .await
            .unwrap();
        let retried_tombstones = store.list_tombstones(&parent).await.unwrap();
        let retried = &retried_tombstones[0].1;
        assert_eq!(retried.attempt_count, 2);
        assert_eq!(retried.last_error_code, None);

        assert!(
            !store
                .cleanup_tombstone(&parent, &relative, Some(1))
                .await
                .unwrap()
        );
        assert_eq!(
            store.list_tombstones(&parent).await.unwrap()[0]
                .1
                .generation,
            Some(2)
        );
        assert!(
            store
                .cleanup_tombstone(&parent, &relative, Some(2))
                .await
                .unwrap()
        );
        assert!(store.list_tombstones(&parent).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn rich_tombstone_lmdb_excludes_sensitive_source_values() {
        let (store, dir) = make_test_store().await;
        let raw_sentinels = [
            "tombstone-content-sentinel-0731c9",
            "tombstone-principal-sentinel-a2e54b",
            "tombstone-credential-sentinel-629ea1",
            "tombstone-raw-locator-sentinel-4dc220",
            "tombstone-remote-error-sentinel-7de08f",
        ];
        let mut hasher = Sha256::new();
        for sentinel in raw_sentinels {
            hasher.update(sentinel.as_bytes());
            hasher.update([0]);
        }
        let mut source_digest = String::with_capacity(64);
        for byte in hasher.finalize() {
            write!(&mut source_digest, "{byte:02x}").unwrap();
        }

        let parent = comp_path("privacy-owner");
        let relative = comp_path("privacy-child");
        let tombstone = ChildTombstoneInfo::new(
            ChildTombstoneCause::LiveDelete,
            Some(source_digest.clone()),
            Some(41),
            NativeVerificationPolicy::QueryVerified,
        )
        .unwrap();
        let store_for_txn = store.clone();
        let parent_for_txn = parent.clone();
        let relative_for_txn = relative.clone();
        store
            .storage
            .run_txn(move |wtxn| {
                let store = store_for_txn.clone();
                let parent = parent_for_txn.clone();
                let relative = relative_for_txn.clone();
                let tombstone = tombstone.clone();
                Box::pin(async move {
                    store
                        .write_tombstone(wtxn, &parent, &relative, &tombstone)
                        .await
                })
            })
            .await
            .unwrap();
        assert!(
            store
                .mark_tombstone_failed(
                    &parent,
                    &relative,
                    Some(41),
                    NativeEffectErrorCode::VerificationFailed,
                )
                .await
                .unwrap()
        );

        let persisted = store.list_tombstones(&parent).await.unwrap();
        assert_eq!(persisted.len(), 1);
        let info = &persisted[0].1;
        assert_eq!(info.schema_version, CHILD_TOMBSTONE_SCHEMA_VERSION);
        assert_eq!(info.cause, ChildTombstoneCause::LiveDelete);
        assert_eq!(info.source_digest.as_deref(), Some(source_digest.as_str()));
        assert_eq!(info.generation, Some(41));
        assert_ne!(info.created_at_ms, 0);
        assert_eq!(info.attempt_count, 1);
        assert_eq!(
            info.last_error_code,
            Some(NativeEffectErrorCode::VerificationFailed)
        );
        assert_eq!(
            info.verification_policy,
            NativeVerificationPolicy::QueryVerified
        );

        let serialized_lmdb = std::fs::read(dir.path().join("mdb/data.mdb")).unwrap();
        assert!(
            serialized_lmdb
                .windows(source_digest.len())
                .any(|window| window == source_digest.as_bytes()),
            "the expected opaque source digest was not serialized"
        );
        for sentinel in raw_sentinels {
            assert!(
                !serialized_lmdb
                    .windows(sentinel.len())
                    .any(|window| window == sentinel.as_bytes()),
                "sensitive tombstone source value leaked: {sentinel}"
            );
        }
    }

    #[tokio::test]
    async fn eager_existence_records_generation_on_leaf_only() {
        let (store, _dir) = make_test_store().await;
        let owner_key = StableKey::Str(Arc::from("owner"));
        let child_key = StableKey::Str(Arc::from("child"));
        let owner = StablePath(Arc::from(vec![owner_key.clone()]));
        let child = StablePath(Arc::from(vec![owner_key.clone(), child_key.clone()]));

        store
            .ensure_existence_chain_standalone(&child, Some(9))
            .await
            .unwrap();
        let rtxn = store.read_txn().await.unwrap();
        let ancestor_bytes = store
            .db()
            .get(
                &*rtxn,
                &key_child_existence(&StablePath::root(), &owner_key).unwrap(),
            )
            .unwrap()
            .unwrap();
        let ancestor: crate::state::db_schema::ChildExistenceInfo =
            synor_utils::deser::from_msgpack_slice(ancestor_bytes).unwrap();
        let leaf_bytes = store
            .db()
            .get(&*rtxn, &key_child_existence(&owner, &child_key).unwrap())
            .unwrap()
            .unwrap();
        let leaf: crate::state::db_schema::ChildExistenceInfo =
            synor_utils::deser::from_msgpack_slice(leaf_bytes).unwrap();

        assert_eq!(ancestor.generation, None);
        assert_eq!(leaf.generation, Some(9));
    }
}

// --- Submit lifecycle (engine-facing shapes) ----------------------------
//
// Convenience aliases for the `*_standalone` helpers above, named to
// match how engine code refers to these operations.

impl AppStore {
    /// Standalone Phase 5 tombstone sweep. See
    /// [`Self::cleanup_tombstone_standalone`].
    pub async fn cleanup_tombstone(
        &self,
        parent_path: &StablePath,
        relative_path: &StablePath,
        expected_generation: Option<u64>,
    ) -> Result<bool> {
        self.cleanup_tombstone_standalone(parent_path, relative_path, expected_generation)
            .await
    }

    /// Standalone Phase 6 component-memo persist. See
    /// [`Self::finalize_memoization_standalone`].
    pub async fn finalize_memoization(
        &self,
        component_path: &StablePath,
        encoded: &[u8],
    ) -> Result<()> {
        self.finalize_memoization_standalone(component_path, encoded)
            .await
    }

    /// Standalone existence-chain upsert. `_known_parent_path` is
    /// unused on LMDB — `ensure_path_node_type`'s recursion already
    /// short-circuits at the first existing row — but kept for
    /// signature parity with how engine code calls this.
    pub async fn ensure_existence_chain(
        &self,
        path: &StablePath,
        _known_parent_path: &StablePath,
        generation: Option<u64>,
    ) -> Result<()> {
        self.ensure_existence_chain_standalone(path, generation)
            .await
    }

    /// Spawn a background task that streams every `(StablePath,
    /// StablePathNodeType)` pair in this app's store, in stable-path
    /// order. Iteration runs on a dedicated `spawn_blocking` thread
    /// because the LMDB cursor is `!Send`. Forwards to
    /// [`crate::state_store::Storage::spawn_stable_path_iter`].
    pub async fn spawn_stable_path_iter(
        &self,
    ) -> tokio::sync::mpsc::Receiver<Result<(StablePath, StablePathNodeType)>> {
        self.storage.spawn_stable_path_iter(self.clone()).await
    }
}
