//! Per-environment storage handle: opens the underlying LMDB env, batches
//! write transactions, and exposes per-app [`AppStore`] creation.
//!
//! `Storage` is the env-level analog of the per-app [`AppStore`]. Both are
//! cheaply clonable (internally `Arc`-backed) so callers can move them
//! into spawned threads for inspection-style streaming reads.

use crate::prelude::*;
use crate::state::db_schema::{
    ChildExistenceInfo, DbEntryKey, StablePathEntryKey, StablePathNodeType,
};
use crate::state::native_effect::{
    NativeEffectAppSnapshot, NativeEffectDowngradeResult, NativeEffectDowngradeStripResult,
};
use crate::state::stable_path::{StablePath, StablePathPrefix, StablePathRef};
use crate::state_store::app_store::{AppStore, Database};
use crate::state_store::txn::{ReadTxn, WriteTxn};

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, Weak};
use synor_utils::batching::{BatchQueue, Batcher, BatchingOptions, Runner};
use synor_utils::deser::from_msgpack_slice;
use synor_utils::fingerprint::Fingerprint;

const DEFAULT_MAX_DBS: u32 = 1024;
const DEFAULT_MAP_SIZE: usize = 0x1_0000_0000; // 4GiB
const MAP_SIZE_GROWTH_FACTOR: usize = 2;
const ENVIRONMENT_OPERATION_LEASE_FILENAME: &str = "environment.lock";
const MAP_RESIZE_LEASE_FILENAME: &str = "map-resize.lock";
const OPERATION_LEASE_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

/// Sync sibling of [`AppStore::read_txn`]'s `MDB_READERS_FULL` retry,
/// for use inside `spawn_blocking` where the async retry helper isn't
/// reachable. Same two-phase policy. Caller must already hold a coordinator
/// read guard before opening the LMDB read transaction.
fn open_read_txn_on_env_with_retry(
    env: &heed::Env<heed::WithoutTls>,
) -> Result<heed::RoTxn<'_, heed::WithoutTls>> {
    use std::time::{Duration, Instant};

    const INITIAL_BACKOFF: Duration = Duration::from_millis(10);
    const MAX_BACKOFF: Duration = Duration::from_secs(1);
    const PHASE1_TIMEOUT: Duration = Duration::from_secs(3);

    // Phase 1: short timeout for transient concurrency.
    let phase1_start = Instant::now();
    let mut backoff = INITIAL_BACKOFF;
    loop {
        match env.read_txn() {
            Ok(txn) => return Ok(txn),
            Err(heed::Error::Mdb(heed::MdbError::ReadersFull)) => {
                if phase1_start.elapsed() >= PHASE1_TIMEOUT {
                    break;
                }
                warn!("LMDB readers full, retrying");
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            Err(e) => return Err(e.into()),
        }
    }

    // Phase 2: clear stale readers, then retry indefinitely.
    let cleared = env.clear_stale_readers()?;
    if cleared > 0 {
        warn!("Cleared {cleared} stale LMDB readers");
    }
    backoff = INITIAL_BACKOFF;
    loop {
        match env.read_txn() {
            Ok(txn) => return Ok(txn),
            Err(heed::Error::Mdb(heed::MdbError::ReadersFull)) => {
                warn!("LMDB readers still full after clearing stale readers, retrying");
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Returns `true` if `err` is an LMDB `MDB_MAP_RESIZED` error.
fn is_map_resized(err: &Error) -> bool {
    let inner = err.without_contexts();
    if let Error::Internal(anyhow_err) = inner {
        return matches!(
            anyhow_err.downcast_ref::<heed::Error>(),
            Some(heed::Error::Mdb(heed::MdbError::MapResized))
        );
    }
    false
}

fn default_max_dbs() -> u32 {
    DEFAULT_MAX_DBS
}

fn default_map_size() -> usize {
    DEFAULT_MAP_SIZE
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

/// Round `requested` up to the next multiple of the OS page size.
///
/// heed/LMDB require `map_size` to be a multiple of the system page size
/// (4 KiB on most Linux, 16 KiB on Apple Silicon), rejecting other values
/// with a hard error. Users shouldn't have to know their page size, so we
/// align for them here. Rounding *up* only raises the cap on how far the
/// memory map may grow — it never shrinks the user's request. We read the
/// page size via the same `page_size` crate heed validates against, so the
/// aligned value is guaranteed to satisfy heed.
fn align_map_size_to_page(requested: usize) -> usize {
    let page = page_size::get();
    // `page` is a power of two on every supported platform, so it can't be 0
    // and `div_ceil` won't divide by zero. `saturating_mul` guards the
    // (practically impossible) case of `requested` being within one page of
    // `usize::MAX`.
    requested.div_ceil(page).saturating_mul(page)
}

/// Return the process-wide transaction/resize coordinator for one canonical
/// LMDB path. heed deduplicates environments opened on the same path within a
/// process, so independently constructed `Storage` handles must also share
/// this guard; otherwise one handle could resize while another handle still
/// owns a transaction.
fn txn_coordinator_for(db_path: &Path) -> Arc<tokio::sync::RwLock<()>> {
    static COORDINATORS: OnceLock<
        parking_lot::Mutex<HashMap<PathBuf, Weak<tokio::sync::RwLock<()>>>>,
    > = OnceLock::new();

    let canonical = db_path
        .canonicalize()
        .unwrap_or_else(|_| db_path.to_path_buf());
    let mut coordinators = COORDINATORS
        .get_or_init(|| parking_lot::Mutex::new(HashMap::new()))
        .lock();
    coordinators.retain(|_, coordinator| coordinator.strong_count() != 0);
    if let Some(coordinator) = coordinators.get(&canonical).and_then(Weak::upgrade) {
        return coordinator;
    }
    let coordinator = Arc::new(tokio::sync::RwLock::new(()));
    coordinators.insert(canonical, Arc::downgrade(&coordinator));
    coordinator
}

/// Configuration for opening the storage environment.
///
/// The on-disk schema (field names, defaults) is the public configuration
/// surface deserialized from user settings.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StorageSettings {
    pub db_path: PathBuf,
    #[serde(default = "default_max_dbs")]
    pub lmdb_max_dbs: u32,
    #[serde(default = "default_map_size")]
    pub lmdb_map_size: usize,
}

#[derive(Clone)]
pub struct Storage {
    inner: Arc<StorageInner>,
}

struct StorageInner {
    db_env: heed::Env<heed::WithoutTls>,
    coord: Arc<tokio::sync::RwLock<()>>,
    map_resize: MapResizeCoordinator,
    batcher: Batcher<TxnRunner>,
    lease_dir: PathBuf,
    settings: StorageSettings,
}

/// Coordinates LMDB map-size changes both within this process and with other
/// Synor processes sharing the same environment.
///
/// The Tokio lock proves that this process has no live transaction when
/// `mdb_env_set_mapsize` is called. The durable lock file serializes competing
/// growers across processes. A process that observes `MDB_MAP_RESIZED` adopts
/// the size stored in LMDB's metadata by calling `Env::resize(0)` and retries
/// the transaction opening operation.
#[derive(Clone)]
struct MapResizeCoordinator {
    db_env: heed::Env<heed::WithoutTls>,
    coord: Arc<tokio::sync::RwLock<()>>,
    lease_path: PathBuf,
}

impl MapResizeCoordinator {
    fn open_lease_file(&self) -> Result<File> {
        Ok(OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&self.lease_path)?)
    }

    async fn acquire_lease(&self) -> Result<File> {
        let file = self.open_lease_file()?;
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(file),
                Err(std::fs::TryLockError::WouldBlock) => {
                    tokio::time::sleep(OPERATION_LEASE_RETRY_INTERVAL).await;
                }
                Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
            }
        }
    }

    /// Adopt the map size published by another process. The write guard must
    /// be acquired before the cross-process lease so every local transaction
    /// is gone before the unsafe LMDB call.
    async fn adopt_external_resize(&self) -> Result<usize> {
        let local_guard = self.coord.write().await;
        let _lease = self.acquire_lease().await?;
        self.adopt_external_resize_unlocked(&local_guard)
    }

    fn adopt_external_resize_unlocked(
        &self,
        _local_guard: &tokio::sync::RwLockWriteGuard<'_, ()>,
    ) -> Result<usize> {
        // LMDB defines a zero size as "adopt the size currently recorded in
        // the environment metadata". The local write guard guarantees that
        // no transaction in this process is active.
        unsafe {
            self.db_env.resize(0)?;
        }
        let adopted = self.db_env.info().map_size;
        debug!("Adopted externally resized LMDB map: {adopted} bytes");
        Ok(adopted)
    }

    /// Resolve `MDB_MAP_FULL` without racing a concurrent process. First
    /// adopt any size that another writer published while this process was
    /// waiting; grow only when the shared size has not already advanced.
    async fn grow_after_map_full(&self, observed_size: usize) -> Result<usize> {
        let local_guard = self.coord.write().await;
        let _lease = self.acquire_lease().await?;
        let adopted_size = self.adopt_external_resize_unlocked(&local_guard)?;
        if adopted_size > observed_size {
            debug!(
                "Another process already grew the LMDB map from {observed_size} to {adopted_size} bytes"
            );
            return Ok(adopted_size);
        }

        // `resize(0)` can expose an older persisted size after this process
        // has already grown its local map but has not yet committed beyond
        // the old boundary. Never discard that local progress.
        let current = observed_size.max(adopted_size);
        let new_size = current.checked_mul(MAP_SIZE_GROWTH_FACTOR).ok_or_else(|| {
            internal_error!("LMDB map size overflow while doubling: current={current} bytes")
        })?;
        let new_size = align_map_size_to_page(new_size);
        warn!("LMDB map full, auto-resizing to {new_size} bytes and retrying");
        unsafe {
            self.db_env.resize(new_size)?;
        }
        Ok(new_size)
    }

    /// Synchronous counterpart used only on a Tokio blocking-pool thread.
    fn adopt_external_resize_blocking(&self) -> Result<usize> {
        let local_guard = self.coord.blocking_write();
        let lease = self.open_lease_file()?;
        lease.lock()?;
        self.adopt_external_resize_unlocked(&local_guard)
    }

    /// Create a compact LMDB snapshot while recovering a map size published by
    /// another process. LMDB's compact-copy implementation opens an internal
    /// read transaction, so it can surface `MDB_MAP_RESIZED` even though no
    /// explicit transaction is visible at this call site.
    fn compact_copy_to_path_blocking(&self, copy_path: &Path) -> Result<()> {
        loop {
            let local_guard = self.coord.blocking_read();
            // The downgrade staging directory is freshly created. Refuse to
            // follow or overwrite a path that appeared unexpectedly, and use a
            // fresh file for every retry because a failed compact copy may have
            // already written a prefix.
            let mut file = OpenOptions::new()
                .write(true)
                .read(true)
                .create_new(true)
                .open(copy_path)?;
            match self
                .db_env
                .copy_to_file(&mut file, heed::CompactionOption::Enabled)
            {
                Ok(()) => {
                    file.sync_all()?;
                    return Ok(());
                }
                Err(raw_error) => {
                    let error: Error = raw_error.into();
                    drop(file);
                    drop(local_guard);
                    match std::fs::remove_file(copy_path) {
                        Ok(()) => {}
                        Err(remove_error)
                            if remove_error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(remove_error) => return Err(remove_error.into()),
                    }
                    if is_map_resized(&error) {
                        self.adopt_external_resize_blocking()?;
                        continue;
                    }
                    return Err(error);
                }
            }
        }
    }
}

/// Process-scoped exclusive lease for one app's mutating operation lifecycle.
///
/// The operating system releases the lock when the process exits, including
/// after `SIGKILL`. The lock file itself is intentionally retained: unlinking
/// a lock file while another process may be opening it creates two independent
/// inodes and defeats mutual exclusion.
#[derive(Debug)]
pub struct AppOperationLease {
    _environment_file: File,
    _app_file: File,
}

impl Drop for AppOperationLease {
    fn drop(&mut self) {
        // Explicit unlock prevents a duplicated descriptor from extending the
        // lease lifetime; closing each owner file remains the fallback.
        let _ = self._app_file.unlock();
        let _ = self._environment_file.unlock();
    }
}

/// Exclusive lease for a whole-environment administrative snapshot.
#[derive(Debug)]
struct EnvironmentOperationLease {
    _file: File,
}

impl Drop for EnvironmentOperationLease {
    fn drop(&mut self) {
        let _ = self._file.unlock();
    }
}

/// Type-erased body for a batched write transaction. Each body returns a
/// future that runs against the shared `WriteTxn` and resolves to a boxed
/// output. The future is bound to the borrow of the txn (`'a`).
///
/// `Fn + Sync` (not `FnOnce`) so the batcher can retry the entire batch on
/// `MDB_MAP_FULL`: the env is resized between attempts, then every body is
/// called again with a fresh write transaction. Callers must therefore
/// ensure their closures are side-effect–free on the captured state (i.e.
/// they may be invoked more than once). In practice all callers clone `Arc`
/// handles inside the closure and do not move-out of captures, so this is
/// already satisfied.
///
/// `Sync` is required because `try_run_once` holds `&[TxnBody]` across
/// `await` points; for `&T` to be `Send`, `T` must be `Sync`.
type TxnBody = Box<
    dyn for<'a, 'env> Fn(&'a mut WriteTxn<'env>) -> BoxFuture<'a, Result<Box<dyn Any + Send>>>
        + Send
        + Sync,
>;

/// Returns `true` if `err` is an LMDB `MDB_MAP_FULL` error.
fn is_map_full(err: &Error) -> bool {
    let inner = err.without_contexts();
    if let Error::Internal(anyhow_err) = inner {
        return matches!(
            anyhow_err.downcast_ref::<heed::Error>(),
            Some(heed::Error::Mdb(heed::MdbError::MapFull))
        );
    }
    false
}

/// When a `MDB_MAP_FULL` error occurs (either from a put inside a body or
/// from the final commit), the write txn and its coordinator read guard are
/// dropped, the coordinator write guard is acquired, the map size is doubled
/// via `env.resize`, and the whole batch is retried.
///
/// Safety: `resize` is only called while holding the coordinator write guard,
/// which guarantees no read or write LMDB transaction opened through this
/// coordinator is active in the current process.
struct TxnRunner {
    db_env: heed::Env<heed::WithoutTls>,
    coord: Arc<tokio::sync::RwLock<()>>,
    map_resize: MapResizeCoordinator,
}

impl TxnRunner {
    /// Read the current local map size while excluding a concurrent local
    /// resize/adoption. `mdb_env_info` dereferences LMDB's mapped metadata, so
    /// it must participate in the same coordinator protocol as transactions.
    async fn observed_map_size(&self) -> usize {
        let _read_guard = self.coord.read().await;
        self.db_env.info().map_size
    }

    /// Attempts one write-txn pass over `inputs`. If any body or the final
    /// commit returns an error the write txn and coordinator read guard are
    /// dropped before the error propagates. On `MapFull` the caller should
    /// resize (under the coordinator write guard) and retry.
    async fn try_run_once(&self, inputs: &[TxnBody]) -> Result<Vec<Box<dyn Any + Send>>> {
        let _read_guard = self.coord.read().await;
        let mut outputs = Vec::with_capacity(inputs.len());
        let mut wtxn = WriteTxn::new(self.db_env.write_txn()?);
        for body in inputs {
            outputs.push(body(&mut wtxn).await?);
        }
        wtxn.into_inner().commit()?;
        Ok(outputs)
    }
}

#[async_trait]
impl Runner for TxnRunner {
    type Input = TxnBody;
    type Output = Box<dyn Any + Send>;

    async fn run(
        &self,
        inputs: Vec<TxnBody>,
    ) -> Result<impl ExactSizeIterator<Item = Box<dyn Any + Send>>> {
        loop {
            // This short guard is released before `try_run_once` acquires its
            // transaction-lifetime guard, avoiding a nested read acquisition.
            let observed_size = self.observed_map_size().await;
            match self.try_run_once(&inputs).await {
                Ok(outputs) => return Ok(outputs.into_iter()),
                Err(e) if is_map_full(&e) => {
                    self.map_resize.grow_after_map_full(observed_size).await?;
                }
                Err(e) if is_map_resized(&e) => {
                    self.map_resize.adopt_external_resize().await?;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

impl Storage {
    pub async fn new(settings: &StorageSettings) -> Result<Self> {
        let db_path = settings.db_path.join("mdb");
        let lease_dir = settings.db_path.join("leases");
        std::fs::create_dir_all(&db_path)?;
        std::fs::create_dir_all(&lease_dir)?;
        // Backward compatibility: migrate files from old layout into mdb/.
        Self::migrate_legacy_db_files(&settings.db_path, &db_path)?;
        if settings.lmdb_max_dbs < 1 {
            client_bail!("lmdb_max_dbs must be >= 1, got {}", settings.lmdb_max_dbs);
        }
        if settings.lmdb_map_size == 0 {
            client_bail!("lmdb_map_size must be > 0, got {}", settings.lmdb_map_size);
        }
        let map_size = align_map_size_to_page(settings.lmdb_map_size);
        if map_size != settings.lmdb_map_size {
            debug!(
                "Rounded lmdb_map_size up from {} to {} to match the system page size ({})",
                settings.lmdb_map_size,
                map_size,
                page_size::get()
            );
        }
        let db_env = unsafe {
            heed::EnvOpenOptions::new()
                .read_txn_without_tls()
                .max_dbs(settings.lmdb_max_dbs)
                .map_size(map_size)
                .open(db_path)
        }?;
        let cleared_count = db_env.clear_stale_readers()?;
        if cleared_count > 0 {
            info!("Cleared {cleared_count} stale readers");
        }
        let coord = txn_coordinator_for(db_env.path());
        let map_resize = MapResizeCoordinator {
            db_env: db_env.clone(),
            coord: coord.clone(),
            lease_path: lease_dir.join(MAP_RESIZE_LEASE_FILENAME),
        };
        let batcher = Batcher::new(
            TxnRunner {
                db_env: db_env.clone(),
                coord: coord.clone(),
                map_resize: map_resize.clone(),
            },
            Arc::new(BatchQueue::new()),
            BatchingOptions::default(),
        );
        Ok(Self {
            inner: Arc::new(StorageInner {
                db_env,
                coord,
                map_resize,
                batcher,
                lease_dir,
                settings: settings.clone(),
            }),
        })
    }

    /// Construct a `Storage` from an already-open `heed::Env`. Used in unit
    /// tests that open an env directly without going through `StorageSettings`.
    #[cfg(test)]
    pub(crate) fn from_env(db_env: heed::Env<heed::WithoutTls>) -> Self {
        let map_size = db_env.info().map_size;
        let base_path = db_env
            .path()
            .parent()
            .unwrap_or_else(|| db_env.path())
            .to_path_buf();
        let lease_dir = db_env
            .path()
            .parent()
            .unwrap_or_else(|| db_env.path())
            .join("leases");
        std::fs::create_dir_all(&lease_dir).expect("create test lease directory");
        let coord = txn_coordinator_for(db_env.path());
        let map_resize = MapResizeCoordinator {
            db_env: db_env.clone(),
            coord: coord.clone(),
            lease_path: lease_dir.join(MAP_RESIZE_LEASE_FILENAME),
        };
        let batcher = Batcher::new(
            TxnRunner {
                db_env: db_env.clone(),
                coord: coord.clone(),
                map_resize: map_resize.clone(),
            },
            Arc::new(BatchQueue::new()),
            BatchingOptions::default(),
        );
        Self {
            inner: Arc::new(StorageInner {
                db_env,
                coord,
                map_resize,
                batcher,
                lease_dir,
                settings: StorageSettings {
                    db_path: base_path,
                    lmdb_max_dbs: DEFAULT_MAX_DBS,
                    lmdb_map_size: map_size,
                },
            }),
        }
    }

    fn try_acquire_app_operation_lease_once(
        &self,
        app_name: &str,
    ) -> Result<Option<AppOperationLease>> {
        let environment_file = self.open_environment_operation_lease_file()?;
        match environment_file.try_lock_shared() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Ok(None),
            Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
        }

        let fingerprint = Fingerprint::from_bytes(app_name.as_bytes());
        let mut filename = String::with_capacity(fingerprint.0.len() * 2 + 5);
        for byte in fingerprint.0 {
            write!(&mut filename, "{byte:02x}").expect("writing to String cannot fail");
        }
        filename.push_str(".lock");

        let path = self.inner.lease_dir.join(filename);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        match file.try_lock() {
            Ok(()) => Ok(Some(AppOperationLease {
                _environment_file: environment_file,
                _app_file: file,
            })),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
        }
    }

    fn open_environment_operation_lease_file(&self) -> Result<File> {
        Ok(OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(
                self.inner
                    .lease_dir
                    .join(ENVIRONMENT_OPERATION_LEASE_FILENAME),
            )?)
    }

    fn try_acquire_environment_shared_lease_once(&self) -> Result<Option<File>> {
        let file = self.open_environment_operation_lease_file()?;
        match file.try_lock_shared() {
            Ok(()) => Ok(Some(file)),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
        }
    }

    fn try_acquire_environment_operation_lease_once(
        &self,
    ) -> Result<Option<EnvironmentOperationLease>> {
        let file = self.open_environment_operation_lease_file()?;
        match file.try_lock() {
            Ok(()) => Ok(Some(EnvironmentOperationLease { _file: file })),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
        }
    }

    async fn acquire_environment_shared_lease(&self, timeout: std::time::Duration) -> Result<File> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(lease) = self.try_acquire_environment_shared_lease_once()? {
                return Ok(lease);
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                client_bail!(
                    "timed out waiting for the environment-operation lease to allow app access"
                );
            }
            tokio::time::sleep(OPERATION_LEASE_RETRY_INTERVAL.min(deadline - now)).await;
        }
    }

    async fn acquire_environment_operation_lease(
        &self,
        timeout: std::time::Duration,
    ) -> Result<EnvironmentOperationLease> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(lease) = self.try_acquire_environment_operation_lease_once()? {
                return Ok(lease);
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                client_bail!(
                    "timed out waiting for active app operations before the environment snapshot"
                );
            }
            tokio::time::sleep(OPERATION_LEASE_RETRY_INTERVAL.min(deadline - now)).await;
        }
    }

    /// Acquire the cross-process lease protecting one app's operations.
    ///
    /// Update uses this fail-fast form before changing operation state. All
    /// update modes participate so a plain update cannot race a live driver.
    pub fn try_acquire_app_operation_lease(&self, app_name: &str) -> Result<AppOperationLease> {
        match self.try_acquire_app_operation_lease_once(app_name)? {
            Some(lease) => Ok(lease),
            None => {
                client_bail!("another process already owns the app-operation lease for this app")
            }
        }
    }

    /// Wait for the cross-process app-operation lease up to `timeout`.
    ///
    /// App drop uses this only after cancelling and draining its own live
    /// controllers. That sequencing lets the current process release its live
    /// update lease while still bounding a wait on a genuinely external owner.
    pub async fn acquire_app_operation_lease(
        &self,
        app_name: &str,
        timeout: std::time::Duration,
    ) -> Result<AppOperationLease> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(lease) = self.try_acquire_app_operation_lease_once(app_name)? {
                return Ok(lease);
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                client_bail!(
                    "timed out waiting for another process to release the app-operation lease for this app"
                );
            }
            tokio::time::sleep(OPERATION_LEASE_RETRY_INTERVAL.min(deadline - now)).await;
        }
    }

    #[cfg(test)]
    pub(crate) fn txn_coordinator(&self) -> Arc<tokio::sync::RwLock<()>> {
        self.inner.coord.clone()
    }

    /// Open a locally guarded read transaction, recovering both exhausted
    /// reader slots and a map size published by another process.
    pub(crate) async fn read_txn(&self) -> Result<ReadTxn<'_>> {
        loop {
            let guard = self.inner.coord.clone().read_owned().await;
            let env = &self.inner.db_env;
            let try_open = || async {
                match env.read_txn() {
                    Ok(txn) => synor_utils::retryable::Ok(txn),
                    Err(heed::Error::Mdb(heed::MdbError::ReadersFull)) => {
                        warn!("LMDB readers full, retrying");
                        Err(synor_utils::retryable::Error::retryable(internal_error!(
                            "LMDB readers full"
                        )))
                    }
                    Err(error) => Err(synor_utils::retryable::Error::not_retryable(error)),
                }
            };

            let opened: Result<heed::RoTxn<'_, heed::WithoutTls>> =
                match synor_utils::retryable::run(&try_open, &READ_TXN_RETRY_PHASE1).await {
                    Ok(txn) => Ok(txn),
                    Err(error) if !error.is_retryable => Err(error.into()),
                    Err(_) => {
                        let cleared = env.clear_stale_readers()?;
                        if cleared > 0 {
                            warn!("Cleared {cleared} stale LMDB readers");
                        }
                        synor_utils::retryable::run(&try_open, &READ_TXN_RETRY_PHASE2)
                            .await
                            .map_err(Into::<Error>::into)
                    }
                };

            match opened {
                Ok(txn) => return Ok(ReadTxn::new(guard, txn)),
                Err(error) if is_map_resized(&error) => {
                    drop(guard);
                    self.inner.map_resize.adopt_external_resize().await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Migrate legacy files from the old layout (directly in `base_path`)
    /// into the new `db_path` subdirectory.
    fn migrate_legacy_db_files(base_path: &Path, db_path: &Path) -> Result<()> {
        let legacy_files: Vec<PathBuf> = ["data.mdb", "lock.mdb"]
            .iter()
            .map(|name| base_path.join(name))
            .filter(|path| path.exists())
            .collect();
        if legacy_files.is_empty() {
            return Ok(());
        }
        info!(
            "Migrating legacy storage files from {} to {}",
            base_path.display(),
            db_path.display()
        );
        for src in legacy_files {
            let dst = db_path.join(src.file_name().unwrap());
            std::fs::rename(&src, &dst)?;
        }
        Ok(())
    }

    /// Run `body` inside a batched write transaction.
    ///
    /// `body` receives `&mut WriteTxn` and returns a `Send` future (typically
    /// `Box::pin(async move { … })`). Multiple concurrent callers' bodies are
    /// coalesced into a single underlying write txn for throughput. FIFO:
    /// the first caller executes inline; concurrent callers queue up and are
    /// flushed together once the current batch commits. Bodies within a
    /// batch are awaited sequentially against the same txn. If any body
    /// resolves to `Err`, the whole batch is rolled back (the `WriteTxn` is
    /// dropped without committing) and every caller in the batch receives
    /// an error.
    ///
    /// The future must be boxed (`BoxFuture<'a, _>` = `Pin<Box<dyn Future +
    /// Send + 'a>>`) because stable Rust can't yet express a `Send` bound on
    /// the future returned by an `AsyncFnOnce` borrowing from the txn.
    pub async fn run_txn<T, F>(&self, body: F) -> Result<T>
    where
        T: Send + 'static,
        F: for<'a, 'env> Fn(&'a mut WriteTxn<'env>) -> BoxFuture<'a, Result<T>>
            + Send
            + Sync
            + 'static,
    {
        // Call `body(wtxn)` rather than wrapping in `async move { body(wtxn).await }`.
        // The latter would move `body` into the async block, making the outer closure
        // `FnOnce`. By calling `body(wtxn)` directly we borrow `body` (via its `Fn`
        // impl) and only move the returned `future` into the mapping async block,
        // keeping the outer closure `Fn` (retryable on `MDB_MAP_FULL`).
        let erased: TxnBody = Box::new(move |wtxn| {
            let future = body(wtxn);
            Box::pin(async move {
                let value = future.await?;
                Ok(Box::new(value) as Box<dyn Any + Send>)
            })
        });
        let output = self.inner.batcher.run(erased).await?;
        output
            .downcast::<T>()
            .map(|b| *b)
            .map_err(|_| internal_error!("Storage::run_txn: output type mismatch"))
    }

    /// Create the per-app sub-database and wrap it in an `AppStore`.
    pub async fn create_app_store(&self, app_name: &str) -> Result<AppStore> {
        let _environment_lease = self
            .acquire_environment_shared_lease(std::time::Duration::from_secs(30))
            .await?;
        let env = self.inner.db_env.clone();
        let app_name = app_name.to_owned();
        let db = self
            .run_txn(move |wtxn| {
                let env = env.clone();
                let app_name = app_name.clone();
                Box::pin(async move { Ok(env.create_database(&mut **wtxn, Some(&app_name))?) })
            })
            .await?;
        let store = AppStore::new(db, self.inner.db_env.clone(), self.clone());
        store.upgrade_native_obligation_summary_if_present().await?;
        Ok(store)
    }

    /// Open the per-app sub-database by name, or `None` if it doesn't exist.
    /// Opens an internal read transaction for the lookup.
    pub async fn open_app_store_by_name(&self, app_name: &str) -> Result<Option<AppStore>> {
        let _environment_lease = self
            .acquire_environment_shared_lease(std::time::Duration::from_secs(30))
            .await?;
        let rtxn = self.read_txn().await?;
        let db: Option<Database> = self.inner.db_env.open_database(&rtxn, Some(app_name))?;
        // The dbi handle opened in a read txn only becomes usable by other
        // transactions after this txn commits; dropping (aborting) it instead
        // leaves the handle invalid and later reads fail with EINVAL when the
        // sub-database was created by another process.
        rtxn.commit()?;
        let Some(db) = db else {
            return Ok(None);
        };
        let store = AppStore::new(db, self.inner.db_env.clone(), self.clone());
        store.upgrade_native_obligation_summary_if_present().await?;
        Ok(Some(store))
    }

    /// Drop an app's operational data while retaining native effect evidence.
    ///
    /// Any non-completed native effect or query-verified child tombstone
    /// aborts the drop before mutation, preserving all tracking needed for
    /// retry or final commit. Otherwise this retains native schema/effect
    /// evidence, obligation cursors, and the live-generation sequencer. The
    /// sub-database remains registered because heed 0.22 does not expose
    /// `mdb_drop`.
    /// Idempotent: dropping a non-existent app is a no-op.
    pub async fn drop_app(&self, app_name: &str) -> Result<()> {
        let db = {
            let rtxn = self.read_txn().await?;
            let db = self
                .inner
                .db_env
                .open_database::<heed::types::Bytes, heed::types::Bytes>(&rtxn, Some(app_name))?;
            // See `open_app_store_by_name`: commit so the dbi handle stays
            // valid for the write txn below.
            rtxn.commit()?;
            db
        };
        let Some(db) = db else {
            return Ok(());
        };
        let app_store = AppStore::new(db, self.inner.db_env.clone(), self.clone());
        let dropped = self
            .run_txn(move |wtxn| {
                let app_store = app_store.clone();
                Box::pin(async move { app_store.clear_operational_state_in_txn(wtxn).await })
            })
            .await?;
        if !dropped {
            client_bail!(
                "app drop blocked by unresolved native effects or query-verified tombstones"
            );
        }
        Ok(())
    }

    /// Run `f` with `app_store`'s `(db, txn, sender)` on a
    /// `tokio::task::spawn_blocking` thread, streaming items over the
    /// returned channel. LMDB cursors (`RoPrefix`) wrap a raw
    /// `*mut MDB_cursor` and are `!Send`, so iteration can't be held across
    /// an `.await`; the sync loop on the blocking-pool thread should use
    /// `blocking_send` for backpressure and stop when it fails (receiver
    /// dropped). The rtxn open uses the same `MDB_READERS_FULL` retry policy
    /// as [`AppStore::read_txn`], but sync (since we're off the runtime).
    /// An `Err` from `f` is sent as the final item.
    pub(crate) async fn spawn_read_txn_receiver<T, F>(
        &self,
        app_store: AppStore,
        f: F,
    ) -> tokio::sync::mpsc::Receiver<Result<T>>
    where
        T: Send + 'static,
        F: FnOnce(
                &Database,
                &heed::RoTxn<'_, heed::WithoutTls>,
                &tokio::sync::mpsc::Sender<Result<T>>,
            ) -> Result<()>
            + Send
            + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        let coord = self.inner.coord.clone();
        let map_resize = self.inner.map_resize.clone();
        tokio::task::spawn_blocking(move || {
            let result: Result<()> = loop {
                let guard = coord.blocking_read();
                match open_read_txn_on_env_with_retry(&app_store.env) {
                    Ok(txn) => {
                        let db = app_store.db();
                        break f(&db, &txn, &tx);
                    }
                    Err(error) if is_map_resized(&error) => {
                        drop(guard);
                        if let Err(error) = map_resize.adopt_external_resize_blocking() {
                            break Err(error);
                        }
                    }
                    Err(error) => break Err(error),
                }
            };
            if let Err(err) = result {
                let _ = tx.blocking_send(Err(err));
            }
        });

        rx
    }

    /// Stream every `(StablePath, node_type)` entry from `app_store` via
    /// a channel (see [`Self::spawn_read_txn_receiver`] for the threading shape).
    pub async fn spawn_stable_path_iter(
        &self,
        app_store: AppStore,
    ) -> tokio::sync::mpsc::Receiver<Result<(StablePath, StablePathNodeType)>> {
        self.spawn_read_txn_receiver(app_store, |db, txn, tx| {
            Self::for_each_stable_path_in_txn(db, txn, |path, node_type| {
                Ok(tx.blocking_send(Ok((path, node_type))).is_ok())
            })
        })
        .await
    }

    /// Walk every stable path in `db` within an open read txn, calling
    /// `emit(path, node_type)` per path in stored order. `emit` returns
    /// `false` to stop early (e.g. when a channel receiver is gone).
    /// Shared by the stable-path streaming above and the detail streaming
    /// in `inspect::db_inspect`, which folds per-path reads into the same
    /// txn.
    pub(crate) fn for_each_stable_path_in_txn(
        db: &Database,
        txn: &heed::RoTxn<'_, heed::WithoutTls>,
        mut emit: impl FnMut(StablePath, StablePathNodeType) -> Result<bool>,
    ) -> Result<()> {
        let encoded_key_prefix =
            DbEntryKey::StablePathPrefixPrefix(StablePathPrefix::default()).encode()?;

        let mut last_prefix: Option<Vec<u8>> = None;
        for entry in db.prefix_iter(txn, &encoded_key_prefix)? {
            let (raw_key, _) = entry?;
            if let Some(last_prefix) = &last_prefix
                && raw_key.starts_with(last_prefix)
            {
                continue;
            }
            let key: DbEntryKey = DbEntryKey::decode(raw_key)?;
            let path = match key {
                DbEntryKey::StablePath(path, _) => path,
                other => {
                    return Err(internal_error!("Expected StablePath, got {other:?}"));
                }
            };
            last_prefix = Some(DbEntryKey::StablePathPrefix(path.as_ref()).encode()?);

            let node_type = if path.as_ref().is_empty() {
                StablePathNodeType::Component
            } else {
                let path_ref: StablePathRef<'_> = path.as_ref();
                if let Some((parent_ref, key)) = path_ref.split_parent() {
                    let parent_owned: StablePath = parent_ref.into();
                    let info = {
                        let key_encoded = DbEntryKey::StablePath(
                            parent_owned,
                            StablePathEntryKey::ChildExistence(key.clone()),
                        )
                        .encode()?;
                        db.get(txn, &key_encoded)?
                            .map(from_msgpack_slice::<ChildExistenceInfo>)
                            .transpose()?
                    };
                    info.map(|i| i.node_type)
                        .unwrap_or(StablePathNodeType::Directory)
                } else {
                    StablePathNodeType::Component
                }
            };

            if !emit(path, node_type)? {
                break;
            }
        }
        Ok(())
    }

    /// Resolves the app store by name, then spawns the stable-path iteration
    /// thread. Returns `None` if the app's database doesn't exist.
    pub async fn spawn_stable_path_iter_by_name(
        &self,
        app_name: &str,
    ) -> Result<Option<tokio::sync::mpsc::Receiver<Result<(StablePath, StablePathNodeType)>>>> {
        let app_store = self.open_app_store_by_name(app_name).await?;
        Ok(match app_store {
            Some(store) => Some(self.spawn_stable_path_iter(store).await),
            None => None,
        })
    }

    /// Create an LMDB-consistent, compacted copy and remove native-only
    /// metadata from that copy after proving every copied app is quiescent.
    ///
    /// The caller supplies a fresh staging directory and must archive the
    /// returned metadata before atomically publishing the directory. The
    /// source environment is read through LMDB's snapshot-copy API and is
    /// never modified.
    pub async fn prepare_native_downgrade_copy(
        &self,
        staging_path: &Path,
    ) -> Result<NativeEffectDowngradeResult> {
        let _environment_lease = self
            .acquire_environment_operation_lease(std::time::Duration::from_secs(30))
            .await?;
        if staging_path.exists() {
            client_bail!("native downgrade staging path already exists");
        }
        let staging_parent = staging_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(staging_parent)?;
        let staging_parent = staging_parent.canonicalize()?;
        let staging_name = staging_path
            .file_name()
            .ok_or_else(|| client_error!("native downgrade staging path must name a directory"))?;
        let staging_absolute = staging_parent.join(staging_name);
        let source_path = self
            .inner
            .settings
            .db_path
            .canonicalize()
            .unwrap_or_else(|_| self.inner.settings.db_path.clone());
        if staging_absolute.starts_with(&source_path) {
            client_bail!("native downgrade staging path must be outside the source database");
        }

        let staging_path = staging_absolute.as_path();
        std::fs::create_dir(staging_path)?;

        let result = async {
            let staging_mdb = staging_path.join("mdb");
            std::fs::create_dir(&staging_mdb)?;
            let copy_path = staging_mdb.join("data.mdb");
            let map_resize = self.inner.map_resize.clone();
            tokio::task::spawn_blocking(move || -> Result<()> {
                map_resize.compact_copy_to_path_blocking(&copy_path)
            })
            .await
            .map_err(|error| internal_error!("native downgrade copy task failed: {error}"))??;

            let mut copied_settings = self.inner.settings.clone();
            copied_settings.db_path = staging_path.to_path_buf();
            let copied = Storage::new(&copied_settings).await?;
            let app_names = copied.list_app_names().await?;
            let mut stores = Vec::with_capacity(app_names.len());
            let mut apps = Vec::with_capacity(app_names.len());
            for app_name in app_names {
                let store = copied
                    .open_app_store_by_name(&app_name)
                    .await?
                    .ok_or_else(|| internal_error!("copied app database disappeared"))?;
                let (schema_version, effects) = store.native_effect_snapshot().await?;
                apps.push(NativeEffectAppSnapshot {
                    app_name,
                    schema_version: schema_version.map(|version| version.0),
                    effects,
                });
                stores.push(store);
            }

            let stores_for_txn = stores.clone();
            let stripped = copied
                .run_txn(move |txn| {
                    let stores = stores_for_txn.clone();
                    Box::pin(async move {
                        let mut total = NativeEffectDowngradeStripResult::default();
                        for store in stores {
                            let result = store
                                .strip_native_metadata_for_downgrade_in_txn(txn)
                                .await?;
                            total.removed_schema_markers = total
                                .removed_schema_markers
                                .checked_add(result.removed_schema_markers)
                                .ok_or_else(|| {
                                    internal_error!("native downgrade count overflow")
                                })?;
                            total.removed_effects = total
                                .removed_effects
                                .checked_add(result.removed_effects)
                                .ok_or_else(|| {
                                    internal_error!("native downgrade count overflow")
                                })?;
                            total.removed_obligation_cursors = total
                                .removed_obligation_cursors
                                .checked_add(result.removed_obligation_cursors)
                                .ok_or_else(|| {
                                    internal_error!("native downgrade count overflow")
                                })?;
                            total.removed_lineage_cursors = total
                                .removed_lineage_cursors
                                .checked_add(result.removed_lineage_cursors)
                                .ok_or_else(|| {
                                    internal_error!("native downgrade count overflow")
                                })?;
                            total.removed_live_generation_keys = total
                                .removed_live_generation_keys
                                .checked_add(result.removed_live_generation_keys)
                                .ok_or_else(|| {
                                    internal_error!("native downgrade count overflow")
                                })?;
                        }
                        Ok(total)
                    })
                })
                .await?;

            for store in &stores {
                let (schema_version, effects) = store.native_effect_snapshot().await?;
                if schema_version.is_some() || !effects.is_empty() {
                    internal_bail!("native downgrade copy retained native effect metadata");
                }
            }
            copied.inner.db_env.force_sync()?;

            Ok(NativeEffectDowngradeResult {
                apps,
                removed_schema_markers: stripped.removed_schema_markers,
                removed_effects: stripped.removed_effects,
                removed_obligation_cursors: stripped.removed_obligation_cursors,
                removed_lineage_cursors: stripped.removed_lineage_cursors,
                removed_live_generation_keys: stripped.removed_live_generation_keys,
            })
        }
        .await;

        if result.is_err() {
            let _ = std::fs::remove_dir_all(staging_path);
        }
        result
    }

    /// List every non-empty named app sub-store in this storage environment.
    /// The "unnamed database" is LMDB's catalog of named sub-databases.
    pub async fn list_app_names(&self) -> Result<Vec<String>> {
        let db_env = &self.inner.db_env;
        let rtxn = self.read_txn().await?;
        let unnamed: heed::Database<heed::types::Str, heed::types::DecodeIgnore> = db_env
            .open_database(&rtxn, None)?
            .expect("the unnamed database always exists");

        let mut names = Vec::new();
        for result in unnamed.iter(&rtxn)? {
            let (name, ()) = result?;
            if let Ok(Some(db)) =
                db_env.open_database::<heed::types::Bytes, heed::types::Bytes>(&rtxn, Some(name))
                && db.first(&rtxn)?.is_some()
            {
                names.push(name.to_string());
            }
        }
        names.sort();
        Ok(names)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::db_schema::{
        ChildExistenceInfo, ChildTombstoneCause, ChildTombstoneInfo,
        LIVE_COMPONENT_GENERATION_KEY_SYMBOL, NativeSchemaVersion, StablePathNodeType,
    };
    use crate::state::native_effect::{
        NativeEffectCounts, NativeEffectDescriptor, NativeEffectErrorCode, NativeEffectIntent,
        NativeEffectOperation, NativeEffectStatus, NativeVerificationPolicy,
    };
    use crate::state::stable_path::{StableKey, StablePath};
    use crate::state_store::submit_session::PrecommitWritePlan;
    use sha2::{Digest as _, Sha256};
    use std::collections::{BTreeMap, HashSet};
    use std::sync::Arc;
    use synor_utils::fingerprint::Fingerprint;
    use tempfile::TempDir;

    fn component_path(name: &str) -> StablePath {
        StablePath(Arc::from(vec![StableKey::Str(Arc::from(name))]))
    }

    async fn raw_app_entries(app_store: &AppStore) -> Vec<(Vec<u8>, Vec<u8>)> {
        let rtxn = app_store.read_txn().await.unwrap();
        app_store
            .db()
            .iter(&*rtxn)
            .unwrap()
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.to_vec(), value.to_vec())
            })
            .collect()
    }

    fn effect_intent(action_id: &str) -> NativeEffectIntent {
        NativeEffectIntent::new(
            NativeEffectDescriptor {
                action_id: action_id.to_owned(),
                operation: NativeEffectOperation::Cleanup,
                source_digest: "a".repeat(64),
                source_generation: 1,
                target_locator_digest: "b".repeat(64),
            },
            Fingerprint::from_bytes(action_id.as_bytes()),
            NativeVerificationPolicy::QueryVerified,
        )
        .unwrap()
    }

    /// Persist one production-shaped native-effect batch through the same
    /// preview-plan and terminal precommit writer used by engine submission.
    /// Keeping the scale certification on this path ensures it exercises the
    /// single summary read/write performed by `write_native_effects_in_txn`,
    /// rather than the compatibility single-record upsert helper.
    async fn precommit_native_effect_batch(
        app_store: &AppStore,
        component_path: &StablePath,
        proposed: Vec<NativeEffectIntent>,
    ) -> Result<Vec<String>> {
        let store_for_precommit = app_store.clone();
        let component_path_for_plan = component_path.clone();
        let proposed = Arc::new(proposed);
        app_store
            .precommit(component_path, move |txn, _session| {
                let store = store_for_precommit.clone();
                let component_path = component_path_for_plan.clone();
                let proposed = Arc::clone(&proposed);
                Box::pin(async move {
                    let mut bound = Vec::with_capacity(proposed.len());
                    for intent in proposed.iter() {
                        bound.push(
                            store
                                .plan_native_effect_lineage_in_txn(txn, intent.clone())
                                .await?,
                        );
                    }
                    let evidence_ids = bound
                        .iter()
                        .map(|intent| intent.evidence_id().to_owned())
                        .collect();
                    Ok(Some((
                        PrecommitWritePlan {
                            self_path: component_path,
                            new_tracking_info: None,
                            preempted_owner_updates: BTreeMap::new(),
                            segment_names: HashMap::new(),
                            native_effect_intents: bound,
                            blocked_native_effect_intents: Vec::new(),
                            id_sequence_updates: Vec::new(),
                        },
                        evidence_ids,
                    )))
                })
            })
            .await?
            .ok_or_else(|| internal_error!("native-effect precommit unexpectedly returned retry"))
    }

    #[cfg(unix)]
    fn publish_durable_test_marker(path: &Path) {
        let marker = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .unwrap();
        marker.sync_all().unwrap();
        File::open(path.parent().unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
    }

    #[test]
    fn align_map_size_rounds_up_to_page_multiple() {
        let page = page_size::get();
        // Exact multiples are left untouched.
        assert_eq!(align_map_size_to_page(page), page);
        assert_eq!(align_map_size_to_page(4 * page), 4 * page);
        // Anything below a full page rounds up to a single page.
        assert_eq!(align_map_size_to_page(1), page);
        assert_eq!(align_map_size_to_page(page - 1), page);
        // A value just past a page boundary rounds up to the next page.
        assert_eq!(align_map_size_to_page(page + 1), 2 * page);

        // The value from the original bug report (10 KiB) becomes a valid
        // page multiple no smaller than what was requested.
        let aligned = align_map_size_to_page(10 * 1024);
        assert_eq!(aligned % page, 0);
        assert!(aligned >= 10 * 1024);
    }

    /// Regression test for the user-facing failure: a `lmdb_map_size` that
    /// isn't a multiple of the system page size used to surface heed's hard
    /// error ("map size (N) must be a multiple of the system page size").
    /// We now align it up transparently, so opening the env just works.
    #[tokio::test]
    async fn new_accepts_unaligned_map_size() {
        let dir = TempDir::new().unwrap();
        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: DEFAULT_MAX_DBS,
            // 4 MiB + 1 byte: deliberately not a multiple of any page size,
            // yet large enough to back a real env on both 4 KiB and 16 KiB
            // page platforms once aligned up.
            lmdb_map_size: 4 * 1024 * 1024 + 1,
        };
        Storage::new(&settings).await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn downgrade_copy_rejects_symlinked_staging_parent_inside_source() {
        let root = TempDir::new().unwrap();
        let source_path = root.path().join("source");
        let settings = StorageSettings {
            db_path: source_path.clone(),
            lmdb_max_dbs: DEFAULT_MAX_DBS,
            lmdb_map_size: 16 * 1024 * 1024,
        };
        let storage = Storage::new(&settings).await.unwrap();
        let source_link = root.path().join("source-link");
        std::os::unix::fs::symlink(&source_path, &source_link).unwrap();
        let staging_path = source_link.join("unsafe-staging");

        let error = storage
            .prepare_native_downgrade_copy(&staging_path)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("outside the source database"));
        assert!(!source_path.join("unsafe-staging").exists());
    }

    #[tokio::test]
    async fn downgrade_copy_exports_and_strips_only_native_metadata() {
        let root = TempDir::new().unwrap();
        let source_path = root.path().join("source");
        let staging_path = root.path().join("staging");
        let settings = StorageSettings {
            db_path: source_path.clone(),
            lmdb_max_dbs: DEFAULT_MAX_DBS,
            lmdb_map_size: 16 * 1024 * 1024,
        };
        let storage = Storage::new(&settings).await.unwrap();
        let store = storage.create_app_store("downgrade-app").await.unwrap();
        let operational_key = StableKey::Symbol("operator/sequence".into());
        let live_generation_key = StableKey::Symbol(LIVE_COMPONENT_GENERATION_KEY_SYMBOL.into());
        let proposed = effect_intent("downgrade:completed");

        let store_for_txn = store.clone();
        let proposed_for_txn = proposed.clone();
        let operational_for_txn = operational_key.clone();
        let live_for_txn = live_generation_key.clone();
        let evidence_id = storage
            .run_txn(move |txn| {
                let store = store_for_txn.clone();
                let proposed = proposed_for_txn.clone();
                let operational_key = operational_for_txn.clone();
                let live_generation_key = live_for_txn.clone();
                Box::pin(async move {
                    store.write_id_sequence(txn, &operational_key, 77).await?;
                    store
                        .write_id_sequence(txn, &live_generation_key, 9)
                        .await?;
                    let bound = store
                        .bind_native_effect_lineage_in_txn(txn, proposed)
                        .await?;
                    let evidence_id = bound.evidence_id().to_owned();
                    store
                        .upsert_native_effect_intent_in_txn(txn, &bound)
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
        storage
            .run_txn(move |txn| {
                let store = store_for_txn.clone();
                let evidence_id = evidence_for_txn.clone();
                Box::pin(async move {
                    store
                        .finalize_native_effects_in_txn(txn, &[evidence_id])
                        .await
                })
            })
            .await
            .unwrap();

        let prepared = storage
            .prepare_native_downgrade_copy(&staging_path)
            .await
            .unwrap();
        assert_eq!(prepared.apps.len(), 1);
        assert_eq!(prepared.apps[0].app_name, "downgrade-app");
        assert_eq!(prepared.apps[0].schema_version, Some(4));
        assert_eq!(prepared.apps[0].effects.len(), 1);
        assert_eq!(prepared.removed_schema_markers, 1);
        assert_eq!(prepared.removed_effects, 1);
        assert_eq!(prepared.removed_lineage_cursors, 1);
        assert_eq!(prepared.removed_live_generation_keys, 1);

        // The source remains fully native and retains the same operational key.
        assert_eq!(store.native_effect_counts().await.unwrap().completed, 1);
        let source_store = store.clone();
        let source_operational_key = operational_key.clone();
        assert_eq!(
            storage
                .run_txn(move |txn| {
                    let store = source_store.clone();
                    let key = source_operational_key.clone();
                    Box::pin(async move { store.peek_id_sequence_in_txn(txn, &key).await })
                })
                .await
                .unwrap(),
            Some(77)
        );

        let mut copied_settings = settings.clone();
        copied_settings.db_path = staging_path;
        let copied = Storage::new(&copied_settings).await.unwrap();
        let copied_store = copied
            .open_app_store_by_name("downgrade-app")
            .await
            .unwrap()
            .unwrap();
        let (schema, effects) = copied_store.native_effect_snapshot().await.unwrap();
        assert_eq!(schema, None);
        assert!(effects.is_empty());
        let copied_store_for_txn = copied_store.clone();
        let copied_operational_key = operational_key.clone();
        let copied_live_key = live_generation_key.clone();
        let (operational, live_generation) = copied
            .run_txn(move |txn| {
                let store = copied_store_for_txn.clone();
                let operational_key = copied_operational_key.clone();
                let live_generation_key = copied_live_key.clone();
                Box::pin(async move {
                    Ok((
                        store.peek_id_sequence_in_txn(txn, &operational_key).await?,
                        store
                            .peek_id_sequence_in_txn(txn, &live_generation_key)
                            .await?,
                    ))
                })
            })
            .await
            .unwrap();
        assert_eq!(operational, Some(77));
        assert_eq!(live_generation, None);
    }

    #[tokio::test]
    async fn downgrade_copy_refuses_unresolved_effects_and_all_tombstones() {
        let root = TempDir::new().unwrap();
        let settings = StorageSettings {
            db_path: root.path().join("source"),
            lmdb_max_dbs: DEFAULT_MAX_DBS,
            lmdb_map_size: 16 * 1024 * 1024,
        };
        let storage = Storage::new(&settings).await.unwrap();
        let unresolved_store = storage.create_app_store("unresolved").await.unwrap();
        let proposed = effect_intent("downgrade:pending");
        let unresolved_for_txn = unresolved_store.clone();
        storage
            .run_txn(move |txn| {
                let store = unresolved_for_txn.clone();
                let proposed = proposed.clone();
                Box::pin(async move {
                    let bound = store
                        .bind_native_effect_lineage_in_txn(txn, proposed)
                        .await?;
                    store.upsert_native_effect_intent_in_txn(txn, &bound).await
                })
            })
            .await
            .unwrap();

        let unresolved_staging = root.path().join("unresolved-staging");
        let error = storage
            .prepare_native_downgrade_copy(&unresolved_staging)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("unresolved native effects"));
        assert!(!unresolved_staging.exists());
        assert_eq!(
            unresolved_store
                .native_effect_counts()
                .await
                .unwrap()
                .pending,
            1
        );

        // Resolve the effect, then prove even a legacy-unverified tombstone
        // blocks the downgrade rather than being silently discarded.
        let (schema, effects) = unresolved_store.native_effect_snapshot().await.unwrap();
        assert_eq!(schema, Some(NativeSchemaVersion::CURRENT));
        let evidence_id = effects[0].evidence_id().to_owned();
        unresolved_store
            .mark_native_effects_verified(std::slice::from_ref(&evidence_id))
            .await
            .unwrap();
        let unresolved_for_txn = unresolved_store.clone();
        let evidence_for_txn = evidence_id.clone();
        storage
            .run_txn(move |txn| {
                let store = unresolved_for_txn.clone();
                let evidence_id = evidence_for_txn.clone();
                Box::pin(async move {
                    store
                        .finalize_native_effects_in_txn(txn, &[evidence_id])
                        .await
                })
            })
            .await
            .unwrap();

        let parent = StablePath::root();
        let relative = component_path("retained-cleanup");
        let tombstone = ChildTombstoneInfo::new(
            ChildTombstoneCause::ComponentOrphan,
            None,
            Some(3),
            NativeVerificationPolicy::LegacyUnverified,
        )
        .unwrap();
        let unresolved_for_txn = unresolved_store.clone();
        storage
            .run_txn(move |txn| {
                let store = unresolved_for_txn.clone();
                let parent = parent.clone();
                let relative = relative.clone();
                let tombstone = tombstone.clone();
                Box::pin(async move {
                    store
                        .write_tombstone(txn, &parent, &relative, &tombstone)
                        .await
                        .map(|_| ())
                })
            })
            .await
            .unwrap();

        let tombstone_staging = root.path().join("tombstone-staging");
        let error = storage
            .prepare_native_downgrade_copy(&tombstone_staging)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("child cleanup tombstones"));
        assert!(!tombstone_staging.exists());
        assert!(unresolved_store.has_any_tombstones().await.unwrap());
    }

    #[tokio::test]
    async fn app_operation_lease_is_exclusive_and_released_on_drop() {
        let dir = TempDir::new().unwrap();
        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: default_map_size(),
        };
        let storage = Storage::new(&settings).await.unwrap();

        let lease = storage
            .try_acquire_app_operation_lease("lease-app")
            .unwrap();
        // Simulate descriptors inherited across fork while the owner is dropped.
        #[cfg(unix)]
        let duplicated_lease_files = (
            lease._environment_file.try_clone().unwrap(),
            lease._app_file.try_clone().unwrap(),
        );
        let error = storage
            .try_acquire_app_operation_lease("lease-app")
            .expect_err("a second owner must be rejected");
        assert!(
            error
                .to_string()
                .contains("another process already owns the app-operation lease")
        );

        // A different app has an independent lease even in the same storage.
        let other = storage
            .try_acquire_app_operation_lease("different-app")
            .unwrap();
        drop(other);

        assert!(
            storage
                .try_acquire_environment_operation_lease_once()
                .unwrap()
                .is_none(),
            "an active app must block a whole-environment snapshot"
        );
        drop(lease);
        let environment_lease = storage
            .try_acquire_environment_operation_lease_once()
            .unwrap()
            .expect("the environment lease must become available");
        #[cfg(unix)]
        let duplicated_environment_lease_file = environment_lease._file.try_clone().unwrap();
        storage
            .try_acquire_app_operation_lease("lease-app")
            .expect_err("the environment snapshot must block new app operations");
        drop(environment_lease);
        storage
            .try_acquire_app_operation_lease("lease-app")
            .expect("dropping the owner must release the OS lock");
        #[cfg(unix)]
        drop((duplicated_lease_files, duplicated_environment_lease_file));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn app_operation_lease_child_process() {
        let Some(db_path) = std::env::var_os("SYNOR_LIVE_LEASE_CHILD_DB") else {
            return;
        };
        let marker_path = PathBuf::from(std::env::var_os("SYNOR_LIVE_LEASE_CHILD_MARKER").unwrap());
        let storage = Storage::new(&StorageSettings {
            db_path: PathBuf::from(db_path),
            lmdb_max_dbs: 8,
            lmdb_map_size: default_map_size(),
        })
        .await
        .unwrap();
        let _lease = storage
            .try_acquire_app_operation_lease("process-kill-app")
            .unwrap();

        publish_durable_test_marker(&marker_path);

        // The parent terminates this process with SIGKILL after observing the
        // durable marker. No Rust destructor or application cleanup can run.
        std::future::pending::<()>().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn app_operation_lease_recovers_after_process_kill() {
        struct ChildGuard(Option<std::process::Child>);

        impl Drop for ChildGuard {
            fn drop(&mut self) {
                if let Some(child) = &mut self.0 {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
        }

        let dir = TempDir::new().unwrap();
        let marker_path = dir.path().join("child-holds-lease");
        let settings = StorageSettings {
            db_path: dir.path().join("storage"),
            lmdb_max_dbs: 8,
            lmdb_map_size: default_map_size(),
        };
        let storage = Storage::new(&settings).await.unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("state_store::storage::tests::app_operation_lease_child_process")
            .arg("--nocapture")
            .env("SYNOR_LIVE_LEASE_CHILD_DB", &settings.db_path)
            .env("SYNOR_LIVE_LEASE_CHILD_MARKER", &marker_path)
            .spawn()
            .unwrap();
        let mut child = ChildGuard(Some(child));

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while !marker_path.exists() {
            let child_process = child.0.as_mut().unwrap();
            if let Some(status) = child_process.try_wait().unwrap() {
                panic!("lease child exited before publishing its marker: {status}");
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for lease child"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        let error = storage
            .try_acquire_app_operation_lease("process-kill-app")
            .expect_err("the child process must fence a second owner");
        assert!(
            error
                .to_string()
                .contains("another process already owns the app-operation lease")
        );
        assert!(
            storage
                .try_acquire_environment_operation_lease_once()
                .unwrap()
                .is_none(),
            "the child app operation must fence an environment snapshot"
        );

        // Child::kill sends SIGKILL on Unix. The child cannot unlock in user
        // code, so successful reacquisition proves kernel-owned recovery.
        let mut killed_child = child.0.take().unwrap();
        killed_child.kill().unwrap();
        let status = killed_child.wait().unwrap();
        assert!(!status.success());

        storage
            .try_acquire_app_operation_lease("process-kill-app")
            .expect("the OS must release the app-operation lease after process death");
        storage
            .try_acquire_environment_operation_lease_once()
            .unwrap()
            .expect("the OS must release the environment lease after process death");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn native_effect_crash_child_process() {
        let Some(db_path) = std::env::var_os("SYNOR_NATIVE_CRASH_CHILD_DB") else {
            return;
        };
        let phase = std::env::var("SYNOR_NATIVE_CRASH_CHILD_PHASE").unwrap();
        let marker_path =
            PathBuf::from(std::env::var_os("SYNOR_NATIVE_CRASH_CHILD_MARKER").unwrap());
        let apply_path =
            PathBuf::from(std::env::var_os("SYNOR_NATIVE_CRASH_APPLY_MARKER").unwrap());
        let storage = Storage::new(&StorageSettings {
            db_path: PathBuf::from(db_path),
            lmdb_max_dbs: 8,
            lmdb_map_size: default_map_size(),
        })
        .await
        .unwrap();
        let app_store = storage.create_app_store("native-crash-app").await.unwrap();
        let intent = effect_intent("cleanup:crash-boundary");
        let effect_id = intent.descriptor.action_id.clone();

        let store_for_txn = app_store.clone();
        storage
            .run_txn(move |wtxn| {
                let app_store = store_for_txn.clone();
                let intent = intent.clone();
                Box::pin(async move {
                    app_store
                        .db()
                        .put(&mut **wtxn, b"crash-tracking", b"must-survive")?;
                    app_store
                        .upsert_native_effect_intent_in_txn(wtxn, &intent)
                        .await
                })
            })
            .await
            .unwrap();
        if phase == "after-precommit" {
            publish_durable_test_marker(&marker_path);
            std::future::pending::<()>().await;
        }

        publish_durable_test_marker(&apply_path);
        if phase == "after-apply" {
            publish_durable_test_marker(&marker_path);
            std::future::pending::<()>().await;
        }

        app_store
            .mark_native_effects_verified(std::slice::from_ref(&effect_id))
            .await
            .unwrap();
        if phase == "after-verification" {
            publish_durable_test_marker(&marker_path);
            std::future::pending::<()>().await;
        }

        assert_eq!(phase, "during-final-commit");
        let store_for_txn = app_store.clone();
        storage
            .run_txn(move |wtxn| {
                let app_store = store_for_txn.clone();
                let effect_id = effect_id.clone();
                let marker_path = marker_path.clone();
                Box::pin(async move {
                    app_store
                        .finalize_native_effects_in_txn(wtxn, &[effect_id])
                        .await?;
                    app_store.db().delete(&mut **wtxn, b"crash-tracking")?;
                    publish_durable_test_marker(&marker_path);
                    std::future::pending::<Result<()>>().await
                })
            })
            .await
            .unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn native_effect_lifecycle_recovers_at_process_kill_boundaries() {
        struct ChildGuard(Option<std::process::Child>);

        impl Drop for ChildGuard {
            fn drop(&mut self) {
                if let Some(child) = &mut self.0 {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
        }

        for (phase, expected_status, apply_preexists) in [
            ("after-precommit", NativeEffectStatus::Pending, false),
            ("after-apply", NativeEffectStatus::Pending, true),
            ("after-verification", NativeEffectStatus::Verified, true),
            ("during-final-commit", NativeEffectStatus::Verified, true),
        ] {
            let dir = TempDir::new().unwrap();
            let db_path = dir.path().join("storage");
            let marker_path = dir.path().join("ready-to-kill");
            let apply_path = dir.path().join("external-effect-applied");
            let child = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("state_store::storage::tests::native_effect_crash_child_process")
                .arg("--nocapture")
                .env("SYNOR_NATIVE_CRASH_CHILD_DB", &db_path)
                .env("SYNOR_NATIVE_CRASH_CHILD_PHASE", phase)
                .env("SYNOR_NATIVE_CRASH_CHILD_MARKER", &marker_path)
                .env("SYNOR_NATIVE_CRASH_APPLY_MARKER", &apply_path)
                .spawn()
                .unwrap();
            let mut child = ChildGuard(Some(child));

            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
            while !marker_path.exists() {
                let child_process = child.0.as_mut().unwrap();
                if let Some(status) = child_process.try_wait().unwrap() {
                    panic!(
                        "native crash child exited before {phase} marker was published: {status}"
                    );
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "timed out waiting for native crash child at {phase}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            let mut killed_child = child.0.take().unwrap();
            killed_child.kill().unwrap();
            let status = killed_child.wait().unwrap();
            assert!(!status.success(), "{phase} child was not killed");

            let storage = Storage::new(&StorageSettings {
                db_path,
                lmdb_max_dbs: 8,
                lmdb_map_size: default_map_size(),
            })
            .await
            .unwrap();
            let app_store = storage
                .open_app_store_by_name("native-crash-app")
                .await
                .unwrap()
                .unwrap();
            let effect_id = "cleanup:crash-boundary".to_owned();
            let effect = app_store.native_effect(&effect_id).await.unwrap().unwrap();
            assert_eq!(effect.status, expected_status, "phase: {phase}");
            let summary = app_store.native_effect_obligation_counts().await.unwrap();
            assert_eq!(
                (
                    summary.pending,
                    summary.verified,
                    summary.failed,
                    summary.blocked,
                    summary.completed,
                ),
                match expected_status {
                    NativeEffectStatus::Pending => (1, 0, 0, 0, 0),
                    NativeEffectStatus::Verified => (0, 1, 0, 0, 0),
                    _ => unreachable!("unexpected crash-boundary status"),
                },
                "transactional summary diverged at {phase}"
            );
            let rtxn = app_store.read_txn().await.unwrap();
            assert_eq!(
                app_store.db().get(&*rtxn, b"crash-tracking").unwrap(),
                Some(&b"must-survive"[..]),
                "tracking disappeared at {phase}"
            );
            drop(rtxn);
            assert_eq!(apply_path.exists(), apply_preexists, "phase: {phase}");

            // Recover exactly as a fresh controlled run would: ensure the
            // external postcondition, persist verification, then atomically
            // finalize evidence with tracking removal.
            if !apply_path.exists() {
                publish_durable_test_marker(&apply_path);
            }
            if expected_status == NativeEffectStatus::Pending {
                app_store
                    .mark_native_effects_verified(std::slice::from_ref(&effect_id))
                    .await
                    .unwrap();
            }
            let store_for_txn = app_store.clone();
            let effect_for_txn = effect_id.clone();
            storage
                .run_txn(move |wtxn| {
                    let app_store = store_for_txn.clone();
                    let effect_id = effect_for_txn.clone();
                    Box::pin(async move {
                        app_store
                            .finalize_native_effects_in_txn(wtxn, &[effect_id])
                            .await?;
                        app_store.db().delete(&mut **wtxn, b"crash-tracking")?;
                        Ok(())
                    })
                })
                .await
                .unwrap();

            assert_eq!(
                app_store
                    .native_effect(&effect_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                NativeEffectStatus::Completed,
                "phase: {phase}"
            );
            let rtxn = app_store.read_txn().await.unwrap();
            assert!(
                app_store
                    .db()
                    .get(&*rtxn, b"crash-tracking")
                    .unwrap()
                    .is_none(),
                "recovery left tracking at {phase}"
            );
        }
    }

    #[tokio::test]
    async fn copied_pre_native_database_runs_compatibility_and_native_migration_lifecycle() {
        let page_size = page_size::get();
        assert!(
            matches!(page_size, 4096 | 16384),
            "no certified pre-native fixture for {page_size}-byte pages"
        );
        let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/pre_native_63df53f")
            .join(page_size.to_string())
            .join("data.mdb");
        let fixture_bytes = std::fs::read(&fixture_path).unwrap();
        let expected_digest = match page_size {
            4096 => "fcfdae440098563ee91939e77b535971034969d554a8a477d543fb61d20554bb",
            16384 => "2153128f58e1b5ce2c667c8da86d963373cd8a2216c049c8d8ca281d21a25048",
            _ => unreachable!(),
        };
        assert_eq!(
            format!("{:x}", Sha256::digest(&fixture_bytes)),
            expected_digest,
            "pre-native fixture provenance digest changed"
        );

        let dir = TempDir::new().unwrap();
        let mdb_path = dir.path().join("mdb");
        std::fs::create_dir(&mdb_path).unwrap();
        std::fs::write(mdb_path.join("data.mdb"), fixture_bytes).unwrap();
        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: default_map_size(),
        };
        let storage = Storage::new(&settings).await.unwrap();
        let app_store = storage
            .open_app_store_by_name("pre_native_fixture")
            .await
            .unwrap()
            .expect("fixture app database is missing");

        assert_eq!(app_store.validate_native_schema().await.unwrap(), None);
        let operational_before = raw_app_entries(&app_store).await;
        assert!(
            operational_before.len() >= 6,
            "fixture does not contain the expected real app state: {} entries",
            operational_before.len()
        );
        let counts = app_store.native_effect_counts().await.unwrap();
        assert_eq!(
            (
                counts.pending,
                counts.verified,
                counts.failed,
                counts.blocked,
                counts.completed,
            ),
            (0, 0, 0, 0, 0)
        );
        assert_eq!(
            raw_app_entries(&app_store).await,
            operational_before,
            "read-only native inspection mutated the pre-feature database"
        );

        let proposed = effect_intent("cleanup:pre-native-migration");
        let store_for_txn = app_store.clone();
        let evidence_id = storage
            .run_txn(move |wtxn| {
                let app_store = store_for_txn.clone();
                let proposed = proposed.clone();
                Box::pin(async move {
                    let bound = app_store
                        .bind_native_effect_lineage_in_txn(wtxn, proposed)
                        .await?;
                    let evidence_id = bound.evidence_id().to_owned();
                    app_store
                        .upsert_native_effect_intent_in_txn(wtxn, &bound)
                        .await?;
                    Ok(evidence_id)
                })
            })
            .await
            .unwrap();
        assert_eq!(
            app_store.validate_native_schema().await.unwrap(),
            Some(NativeSchemaVersion::CURRENT)
        );
        let migrated_entries = raw_app_entries(&app_store).await;
        for original in &operational_before {
            assert!(
                migrated_entries.contains(original),
                "native migration rewrote pre-feature operational state"
            );
        }

        app_store
            .mark_native_effects_verified(std::slice::from_ref(&evidence_id))
            .await
            .unwrap();
        let store_for_txn = app_store.clone();
        let evidence_for_txn = evidence_id.clone();
        storage
            .run_txn(move |wtxn| {
                let app_store = store_for_txn.clone();
                let evidence_id = evidence_for_txn.clone();
                Box::pin(async move {
                    app_store
                        .finalize_native_effects_in_txn(wtxn, &[evidence_id])
                        .await
                })
            })
            .await
            .unwrap();

        storage.drop_app("pre_native_fixture").await.unwrap();
        assert_eq!(
            app_store
                .native_effect(&evidence_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            NativeEffectStatus::Completed
        );
        let retained_entries = raw_app_entries(&app_store).await;
        for (original_key, _) in &operational_before {
            assert!(
                retained_entries
                    .iter()
                    .all(|(retained_key, _)| retained_key != original_key),
                "app drop retained a pre-feature operational key"
            );
        }

        drop(app_store);
        drop(storage);
        let reopened = Storage::new(&settings).await.unwrap();
        let reopened_app = reopened
            .open_app_store_by_name("pre_native_fixture")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            reopened_app.validate_native_schema().await.unwrap(),
            Some(NativeSchemaVersion::CURRENT)
        );
        assert_eq!(
            reopened_app
                .native_effect(&evidence_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            NativeEffectStatus::Completed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_effect_concurrent_single_writer_stress() {
        const WRITER_COUNT: usize = 64;
        const EFFECTS_PER_WRITER: usize = 64;
        const TOTAL_EFFECTS: usize = WRITER_COUNT * EFFECTS_PER_WRITER;

        let dir = TempDir::new().unwrap();
        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: default_map_size(),
        };
        let storage = Storage::new(&settings).await.unwrap();
        let app_store = storage
            .create_app_store("native_writer_stress")
            .await
            .unwrap();
        let start = Arc::new(tokio::sync::Barrier::new(WRITER_COUNT));

        let mut tasks = Vec::with_capacity(WRITER_COUNT);
        for writer in 0..WRITER_COUNT {
            let storage = storage.clone();
            let app_store = app_store.clone();
            let start = start.clone();
            tasks.push(tokio::spawn(async move {
                let proposed: Vec<(NativeEffectIntent, Vec<u8>)> = (0..EFFECTS_PER_WRITER)
                    .map(|effect| {
                        let action_id = format!("stress:{writer}:{effect}");
                        (
                            effect_intent(&action_id),
                            format!("stress-tracking:{writer}:{effect}").into_bytes(),
                        )
                    })
                    .collect();

                start.wait().await;
                let store_for_txn = app_store.clone();
                let proposed_for_txn = proposed.clone();
                let evidence_ids = storage
                    .run_txn(move |wtxn| {
                        let app_store = store_for_txn.clone();
                        let proposed = proposed_for_txn.clone();
                        Box::pin(async move {
                            let mut evidence_ids = Vec::with_capacity(proposed.len());
                            for (intent, tracking_key) in proposed {
                                app_store.db().put(
                                    &mut **wtxn,
                                    &tracking_key,
                                    b"must-survive-until-finalization",
                                )?;
                                let bound = app_store
                                    .bind_native_effect_lineage_in_txn(wtxn, intent)
                                    .await?;
                                evidence_ids.push(bound.evidence_id().to_owned());
                                app_store
                                    .upsert_native_effect_intent_in_txn(wtxn, &bound)
                                    .await?;
                            }
                            Ok(evidence_ids)
                        })
                    })
                    .await?;

                tokio::task::yield_now().await;
                app_store
                    .mark_native_effects_verified(&evidence_ids)
                    .await?;
                tokio::task::yield_now().await;

                let store_for_txn = app_store.clone();
                let evidence_for_txn = evidence_ids.clone();
                let tracking_keys: Vec<Vec<u8>> = proposed
                    .iter()
                    .map(|(_, tracking_key)| tracking_key.clone())
                    .collect();
                storage
                    .run_txn(move |wtxn| {
                        let app_store = store_for_txn.clone();
                        let evidence_ids = evidence_for_txn.clone();
                        let tracking_keys = tracking_keys.clone();
                        Box::pin(async move {
                            app_store
                                .finalize_native_effects_in_txn(wtxn, &evidence_ids)
                                .await?;
                            for tracking_key in tracking_keys {
                                app_store.db().delete(&mut **wtxn, &tracking_key)?;
                            }
                            Ok(())
                        })
                    })
                    .await?;
                Ok::<_, Error>((evidence_ids, proposed))
            }));
        }

        let mut all_evidence_ids = HashSet::with_capacity(TOTAL_EFFECTS);
        let mut all_tracking_keys = Vec::with_capacity(TOTAL_EFFECTS);
        for task in tasks {
            let (evidence_ids, proposed) = task.await.unwrap().unwrap();
            for evidence_id in evidence_ids {
                assert!(
                    all_evidence_ids.insert(evidence_id),
                    "duplicate evidence identity under concurrent allocation"
                );
            }
            all_tracking_keys.extend(proposed.into_iter().map(|(_, tracking_key)| tracking_key));
        }
        assert_eq!(all_evidence_ids.len(), TOTAL_EFFECTS);

        let counts = app_store.native_effect_counts().await.unwrap();
        assert_eq!(
            (
                counts.pending,
                counts.verified,
                counts.failed,
                counts.blocked,
                counts.completed,
            ),
            (0, 0, 0, 0, TOTAL_EFFECTS as u64)
        );
        assert_eq!(
            app_store.validate_native_schema().await.unwrap(),
            Some(NativeSchemaVersion::CURRENT)
        );
        let rtxn = app_store.read_txn().await.unwrap();
        for tracking_key in all_tracking_keys {
            assert!(
                app_store.db().get(&*rtxn, &tracking_key).unwrap().is_none(),
                "final commit left concurrent tracking behind"
            );
        }
        drop(rtxn);

        for evidence_id in all_evidence_ids.iter().take(32) {
            assert_eq!(
                app_store
                    .native_effect(evidence_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                NativeEffectStatus::Completed
            );
        }
    }

    #[tokio::test]
    async fn batched_precommit_updates_obligation_summary_through_lifecycle() {
        const TOTAL_EFFECTS: usize = 257;

        let dir = TempDir::new().unwrap();
        let storage = Storage::new(&StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: default_map_size(),
        })
        .await
        .unwrap();
        let app_store = storage
            .create_app_store("batched_precommit_summary")
            .await
            .unwrap();
        let component_path = component_path("batched-precommit-summary");
        let proposed = (0..TOTAL_EFFECTS)
            .map(|index| effect_intent(&format!("batch-summary:{index}")))
            .collect();

        let evidence_ids = precommit_native_effect_batch(&app_store, &component_path, proposed)
            .await
            .unwrap();
        assert_eq!(evidence_ids.len(), TOTAL_EFFECTS);
        assert_eq!(
            app_store.native_effect_obligation_counts().await.unwrap(),
            NativeEffectCounts {
                pending: TOTAL_EFFECTS as u64,
                ..NativeEffectCounts::default()
            }
        );

        app_store
            .mark_native_effects_verified(&evidence_ids)
            .await
            .unwrap();
        assert_eq!(
            app_store.native_effect_obligation_counts().await.unwrap(),
            NativeEffectCounts {
                verified: TOTAL_EFFECTS as u64,
                ..NativeEffectCounts::default()
            }
        );

        let store_for_txn = app_store.clone();
        let evidence_for_txn = evidence_ids.clone();
        storage
            .run_txn(move |txn| {
                let store = store_for_txn.clone();
                let evidence_ids = evidence_for_txn.clone();
                Box::pin(async move {
                    store
                        .finalize_native_effects_in_txn(txn, &evidence_ids)
                        .await
                })
            })
            .await
            .unwrap();
        let summary = app_store.native_effect_obligation_counts().await.unwrap();
        assert_eq!(
            summary,
            NativeEffectCounts {
                completed: TOTAL_EFFECTS as u64,
                ..NativeEffectCounts::default()
            }
        );
        assert_eq!(
            app_store.native_effect_counts().await.unwrap(),
            summary,
            "transactional summary must match the retained evidence after every batch transition"
        );
    }

    #[tokio::test]
    #[ignore = "phase 6 million-action scale certification"]
    async fn million_action_descriptor_receipt_native_correlation() {
        const TOTAL_EFFECTS: usize = 1_000_000;
        const BATCH_SIZE: usize = 10_000;
        const COMPLETION_CHECK_ITERATIONS: usize = 1_000;

        fn digest_hex(domain: &str, index: usize) -> String {
            format!("{:x}", Sha256::digest(format!("{domain}:{index}")))
        }

        fn receipt_digest(intent: &NativeEffectIntent) -> [u8; 32] {
            let descriptor = &intent.descriptor;
            let mut hasher = Sha256::new();
            hasher.update(b"synor-phase6-scale-receipt-v1\0");
            hasher.update(descriptor.action_id.as_bytes());
            hasher.update([0]);
            hasher.update(match descriptor.operation {
                NativeEffectOperation::Delete => b"delete".as_slice(),
                NativeEffectOperation::Isolate => b"isolate".as_slice(),
                NativeEffectOperation::Cleanup => b"cleanup".as_slice(),
            });
            hasher.update([0]);
            hasher.update(descriptor.source_digest.as_bytes());
            hasher.update(descriptor.source_generation.to_be_bytes());
            hasher.update(descriptor.target_locator_digest.as_bytes());
            hasher.finalize().into()
        }

        fn accumulate(xor: &mut [u8; 32], wrapping_sum: &mut u128, digest: [u8; 32]) {
            for (current, byte) in xor.iter_mut().zip(digest) {
                *current ^= byte;
            }
            *wrapping_sum =
                wrapping_sum.wrapping_add(u128::from_be_bytes(digest[..16].try_into().unwrap()));
        }

        let dir = TempDir::new().unwrap();
        let storage = Storage::new(&StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: default_map_size(),
        })
        .await
        .unwrap();
        let app_store = storage
            .create_app_store("million_action_correlation")
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let mut expected_xor = [0_u8; 32];
        let mut expected_sum = 0_u128;
        let component_path = component_path("million-action-precommit");

        for batch_start in (0..TOTAL_EFFECTS).step_by(BATCH_SIZE) {
            let batch_end = (batch_start + BATCH_SIZE).min(TOTAL_EFFECTS);
            let proposed: Vec<NativeEffectIntent> = (batch_start..batch_end)
                .map(|index| {
                    let action_id = format!("scale:{index}");
                    let intent = NativeEffectIntent::new(
                        NativeEffectDescriptor {
                            action_id,
                            operation: NativeEffectOperation::Delete,
                            source_digest: digest_hex("source", index),
                            source_generation: 1,
                            target_locator_digest: digest_hex("target", index),
                        },
                        Fingerprint::from_bytes(format!("tracking:{index}").as_bytes()),
                        NativeVerificationPolicy::QueryVerified,
                    )
                    .unwrap();
                    accumulate(
                        &mut expected_xor,
                        &mut expected_sum,
                        receipt_digest(&intent),
                    );
                    intent
                })
                .collect();

            let evidence_ids = precommit_native_effect_batch(&app_store, &component_path, proposed)
                .await
                .unwrap();
            assert_eq!(evidence_ids.len(), batch_end - batch_start);
            app_store
                .mark_native_effects_verified(&evidence_ids)
                .await
                .unwrap();
            let store_for_txn = app_store.clone();
            let evidence_for_txn = evidence_ids.clone();
            storage
                .run_txn(move |txn| {
                    let store = store_for_txn.clone();
                    let evidence_ids = evidence_for_txn.clone();
                    Box::pin(async move {
                        store
                            .finalize_native_effects_in_txn(txn, &evidence_ids)
                            .await
                    })
                })
                .await
                .unwrap();

            if batch_end % 100_000 == 0 {
                eprintln!("million-action certification: completed {batch_end}/{TOTAL_EFFECTS}");
            }
        }

        // Exercise the exact no-op strict-completion boundary repeatedly
        // before the intentional evidence audit below. Both reads are backed
        // by fixed-size transactional summary records, so this measurement is
        // independent of the retained million-record history.
        let completion_checks_started = std::time::Instant::now();
        let mut summary_counts = NativeEffectCounts::default();
        for _ in 0..COMPLETION_CHECK_ITERATIONS {
            summary_counts = app_store.native_effect_obligation_counts().await.unwrap();
            assert!(!summary_counts.has_unresolved());
            assert!(
                !app_store
                    .has_query_verified_tombstone_obligations()
                    .await
                    .unwrap()
            );
        }
        let completion_checks_elapsed = completion_checks_started.elapsed();

        let evidence_scan_started = std::time::Instant::now();
        let counts = app_store.native_effect_counts().await.unwrap();
        let evidence_scan_elapsed = evidence_scan_started.elapsed();
        assert_eq!(
            summary_counts, counts,
            "transactional obligation summary diverged from the retained evidence scan"
        );
        assert_eq!(
            (
                counts.pending,
                counts.verified,
                counts.failed,
                counts.blocked,
                counts.completed,
            ),
            (0, 0, 0, 0, TOTAL_EFFECTS as u64)
        );

        let effect_prefix = DbEntryKey::NativeEffectPrefix.encode().unwrap();
        let rtxn = app_store.read_txn().await.unwrap();
        let mut observed_count = 0_usize;
        let mut observed_xor = [0_u8; 32];
        let mut observed_sum = 0_u128;
        for entry in app_store.db().prefix_iter(&*rtxn, &effect_prefix).unwrap() {
            let (_, raw_value) = entry.unwrap();
            let intent: NativeEffectIntent = from_msgpack_slice(raw_value).unwrap();
            intent.validate().unwrap();
            assert_eq!(intent.status, NativeEffectStatus::Completed);
            let index: usize = intent
                .descriptor
                .action_id
                .strip_prefix("scale:")
                .unwrap()
                .parse()
                .unwrap();
            assert!(index < TOTAL_EFFECTS);
            assert_eq!(intent.descriptor.source_digest, digest_hex("source", index));
            assert_eq!(
                intent.descriptor.target_locator_digest,
                digest_hex("target", index)
            );
            assert_eq!(
                intent.tracking_locator,
                Fingerprint::from_bytes(format!("tracking:{index}").as_bytes())
            );
            accumulate(
                &mut observed_xor,
                &mut observed_sum,
                receipt_digest(&intent),
            );
            observed_count += 1;
        }
        assert_eq!(observed_count, TOTAL_EFFECTS);
        assert_eq!(observed_xor, expected_xor);
        assert_eq!(observed_sum, expected_sum);
        eprintln!(
            "million-action certification passed in {:.3}s; \
             strict_completion_checks={} in {:.6}s ({:.9}s/check); \
             full_effect_evidence_scan={:.6}s; \
             summary_counts={summary_counts:?}; data.mdb={} bytes",
            started.elapsed().as_secs_f64(),
            COMPLETION_CHECK_ITERATIONS,
            completion_checks_elapsed.as_secs_f64(),
            completion_checks_elapsed.as_secs_f64() / COMPLETION_CHECK_ITERATIONS as f64,
            evidence_scan_elapsed.as_secs_f64(),
            std::fs::metadata(dir.path().join("mdb/data.mdb"))
                .unwrap()
                .len()
        );
    }

    #[tokio::test]
    async fn drop_retains_evidence_and_unresolved_drop_is_non_mutating() {
        let dir = TempDir::new().unwrap();
        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: default_map_size(),
        };
        let storage = Storage::new(&settings).await.unwrap();
        let app_store = storage.create_app_store("drop_evidence").await.unwrap();
        let intent = effect_intent("cleanup:completed");
        let action_id = intent.descriptor.action_id.clone();
        let app_for_txn = app_store.clone();
        storage
            .run_txn(move |wtxn| {
                let app_store = app_for_txn.clone();
                let intent = intent.clone();
                Box::pin(async move {
                    app_store.db().put(&mut **wtxn, b"operational", b"value")?;
                    app_store
                        .upsert_native_effect_intent_in_txn(wtxn, &intent)
                        .await
                })
            })
            .await
            .unwrap();
        app_store
            .mark_native_effects_verified(std::slice::from_ref(&action_id))
            .await
            .unwrap();
        let app_for_txn = app_store.clone();
        let action_for_txn = action_id.clone();
        storage
            .run_txn(move |wtxn| {
                let app_store = app_for_txn.clone();
                let action_id = action_for_txn.clone();
                Box::pin(async move {
                    app_store
                        .finalize_native_effects_in_txn(wtxn, &[action_id])
                        .await
                })
            })
            .await
            .unwrap();

        storage.drop_app("drop_evidence").await.unwrap();
        let rtxn = app_store.read_txn().await.unwrap();
        assert!(
            app_store
                .db()
                .get(&*rtxn, b"operational")
                .unwrap()
                .is_none()
        );
        drop(rtxn);
        assert_eq!(
            app_store
                .native_effect(&action_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            NativeEffectStatus::Completed
        );

        let verified_store = storage
            .create_app_store("drop_verified_uncommitted")
            .await
            .unwrap();
        let verified = effect_intent("cleanup:verified-uncommitted");
        let verified_id = verified.descriptor.action_id.clone();
        let verified_for_txn = verified_store.clone();
        storage
            .run_txn(move |wtxn| {
                let app_store = verified_for_txn.clone();
                let verified = verified.clone();
                Box::pin(async move {
                    app_store.db().put(&mut **wtxn, b"operational", b"value")?;
                    app_store
                        .upsert_native_effect_intent_in_txn(wtxn, &verified)
                        .await
                })
            })
            .await
            .unwrap();
        verified_store
            .mark_native_effects_verified(std::slice::from_ref(&verified_id))
            .await
            .unwrap();
        assert!(storage.drop_app("drop_verified_uncommitted").await.is_err());
        let rtxn = verified_store.read_txn().await.unwrap();
        assert_eq!(
            verified_store
                .db()
                .get(&*rtxn, b"operational")
                .unwrap()
                .unwrap(),
            b"value"
        );
        drop(rtxn);
        assert_eq!(
            verified_store
                .native_effect(&verified_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            NativeEffectStatus::Verified
        );

        let blocked_store = storage.create_app_store("drop_blocked").await.unwrap();
        let blocked = effect_intent("cleanup:blocked");
        let blocked_for_txn = blocked_store.clone();
        storage
            .run_txn(move |wtxn| {
                let app_store = blocked_for_txn.clone();
                let blocked = blocked.clone();
                Box::pin(async move {
                    app_store.db().put(&mut **wtxn, b"operational", b"value")?;
                    app_store
                        .upsert_blocked_native_effect_in_txn(
                            wtxn,
                            &blocked,
                            NativeEffectErrorCode::ProviderMissing,
                        )
                        .await
                })
            })
            .await
            .unwrap();
        assert!(storage.drop_app("drop_blocked").await.is_err());
        let rtxn = blocked_store.read_txn().await.unwrap();
        assert_eq!(
            blocked_store
                .db()
                .get(&*rtxn, b"operational")
                .unwrap()
                .unwrap(),
            b"value"
        );
    }

    #[tokio::test]
    async fn query_verified_tombstone_makes_drop_byte_for_byte_non_mutating() {
        let dir = TempDir::new().unwrap();
        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: default_map_size(),
        };
        let storage = Storage::new(&settings).await.unwrap();
        let app_store = storage
            .create_app_store("drop_query_verified_tombstone")
            .await
            .unwrap();
        let parent = component_path("owner");
        let relative = component_path("removed-child");
        let child_key = StableKey::Str(Arc::from("live-child"));
        let tombstone = ChildTombstoneInfo::new(
            ChildTombstoneCause::ComponentOrphan,
            None,
            Some(8),
            NativeVerificationPolicy::QueryVerified,
        )
        .unwrap();
        let store_for_txn = app_store.clone();
        let parent_for_txn = parent.clone();
        let relative_for_txn = relative.clone();
        storage
            .run_txn(move |wtxn| {
                let app_store = store_for_txn.clone();
                let parent = parent_for_txn.clone();
                let relative = relative_for_txn.clone();
                let child_key = child_key.clone();
                let tombstone = tombstone.clone();
                Box::pin(async move {
                    app_store
                        .write_child_existence(
                            wtxn,
                            &StablePath::root(),
                            &child_key,
                            &ChildExistenceInfo {
                                node_type: StablePathNodeType::Component,
                                generation: Some(9),
                            },
                        )
                        .await?;
                    app_store
                        .write_id_sequence(
                            wtxn,
                            &StableKey::Symbol("operational-sequence".into()),
                            44,
                        )
                        .await?;
                    app_store
                        .write_tombstone(wtxn, &parent, &relative, &tombstone)
                        .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let before = raw_app_entries(&app_store).await;
        assert!(before.len() >= 3);
        assert!(
            storage
                .drop_app("drop_query_verified_tombstone")
                .await
                .is_err()
        );
        assert_eq!(raw_app_entries(&app_store).await, before);
        assert!(app_store.has_query_verified_tombstones().await.unwrap());
    }

    #[tokio::test]
    async fn map_size_observation_waits_for_resize_exclusion() {
        let dir = TempDir::new().unwrap();
        let initial_map_size = align_map_size_to_page(page_size::get() * 16);
        let storage = Storage::new(&StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: initial_map_size,
        })
        .await
        .unwrap();
        let runner = TxnRunner {
            db_env: storage.inner.db_env.clone(),
            coord: storage.inner.coord.clone(),
            map_resize: storage.inner.map_resize.clone(),
        };

        // A resize/adoption owns the coordinator write lock while LMDB unmaps
        // and remaps its metadata. Polling the observation under that lock must
        // deterministically remain pending rather than entering mdb_env_info.
        let resize_guard = runner.coord.clone().write_owned().await;
        let mut observation = Box::pin(runner.observed_map_size());
        assert!(futures::poll!(&mut observation).is_pending());

        drop(resize_guard);
        assert_eq!(observation.await, initial_map_size);
    }

    /// Integration test for `MDB_MAP_FULL` auto-resize on the `Storage::run_txn`
    /// path: one batched write txn is filled past the map limit, the runner
    /// doubles the map, retries the same body, and commits. Run via
    /// `dev/test_lmdb_auto_resize.sh`.
    #[tokio::test]
    async fn auto_resizes_on_map_full() {
        let dir = TempDir::new().unwrap();
        let page = page_size::get();
        // Deliberately tiny, page-aligned map. Large enough for env metadata and
        // the initial named-database creation without a preliminary resize.
        let initial_map_size = align_map_size_to_page(page * 16);

        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: initial_map_size,
        };
        let storage = Storage::new(&settings).await.unwrap();
        let app_store = storage.create_app_store("resize_test").await.unwrap();
        assert_eq!(
            app_store.env.info().map_size,
            initial_map_size,
            "initial map size should match configured value"
        );

        // One payload pattern; 16 KiB per key. Total raw value bytes exceed the
        // initial map once LMDB btree/metadata overhead is included, so a single
        // `run_txn` body should hit MapFull on put or commit, trigger resize,
        // and succeed only after retry.
        const PAYLOAD_LEN: usize = 16 * 1024;
        const WRITE_COUNT: usize = 64;
        let payload = vec![0xAB_u8; PAYLOAD_LEN];
        let entries: Vec<(String, Vec<u8>)> = (0..WRITE_COUNT)
            .map(|i| (format!("key_{i:04}"), payload.clone()))
            .collect();

        let app_store_for_txn = app_store.clone();
        let entries_for_txn = entries.clone();
        storage
            .run_txn(move |wtxn| {
                let app_store = app_store_for_txn.clone();
                let entries = entries_for_txn.clone();
                Box::pin(async move {
                    for (key, value) in &entries {
                        app_store.db().put(wtxn, key.as_bytes(), value)?;
                    }
                    Ok(())
                })
            })
            .await
            .expect("single run_txn should succeed after MapFull resize-and-retry");

        let final_map_size = app_store.env.info().map_size;
        let expected_min_final = align_map_size_to_page(initial_map_size * MAP_SIZE_GROWTH_FACTOR);
        assert!(
            final_map_size > initial_map_size,
            "map size must grow after MapFull: initial={initial_map_size}, final={final_map_size}"
        );
        assert!(
            final_map_size >= expected_min_final,
            "map size should at least double: initial={initial_map_size}, \
             final={final_map_size}, expected>={expected_min_final}"
        );

        eprintln!(
            "auto_resizes_on_map_full: initial_map_size={initial_map_size} \
             final_map_size={final_map_size} writes={WRITE_COUNT} \
             bytes_per_key={PAYLOAD_LEN}"
        );

        // Read back first, middle, and last keys; verify full payload bytes.
        let rtxn = app_store.read_txn().await.unwrap();
        for key in ["key_0000", "key_0031", "key_0063"] {
            let bytes = app_store
                .db()
                .get(&*rtxn, key.as_bytes())
                .unwrap()
                .unwrap_or_else(|| panic!("{key} should exist after successful commit"));
            assert_eq!(
                bytes.as_ref(),
                payload.as_slice(),
                "{key} payload should match what was written"
            );
        }
    }

    /// Verifies the coordinator blocks `Env::resize` until every guarded read
    /// transaction has ended:
    ///
    /// 1. Open a [`ReadTxn`] and keep it alive.
    /// 2. Start a concurrent `Storage::run_txn` write large enough to hit MapFull.
    /// 3. Confirm the write has not finished while the read txn is still open.
    /// 4. Drop the read txn.
    /// 5. Confirm the write completes, the map grows, and data is intact.
    #[tokio::test]
    async fn resize_waits_for_active_reader() {
        use tokio::sync::oneshot;

        let dir = TempDir::new().unwrap();
        let page = page_size::get();
        let initial_map_size = align_map_size_to_page(page * 16);
        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: initial_map_size,
        };
        let storage = Storage::new(&settings).await.unwrap();
        let app_store = storage.create_app_store("coord_test").await.unwrap();
        assert!(Arc::ptr_eq(
            &storage.txn_coordinator(),
            &txn_coordinator_for(storage.inner.db_env.path()),
        ));
        let coord = storage.txn_coordinator();

        // Step 1: hold a guarded read transaction open.
        let reader = app_store.read_txn().await.unwrap();

        const PAYLOAD_LEN: usize = 16 * 1024;
        const WRITE_COUNT: usize = 64;
        let payload = vec![0xCD_u8; PAYLOAD_LEN];
        let entries: Vec<(String, Vec<u8>)> = (0..WRITE_COUNT)
            .map(|i| (format!("key_{i:04}"), payload.clone()))
            .collect();

        // Step 2: concurrent write that will trigger MapFull + resize.
        let (write_started_tx, write_started_rx) = oneshot::channel();
        let storage_for_write = storage.clone();
        let app_store_for_write = app_store.clone();
        let entries_for_write = entries.clone();
        let write_handle = tokio::spawn(async move {
            write_started_tx.send(()).ok();
            storage_for_write
                .run_txn(move |wtxn| {
                    let app_store = app_store_for_write.clone();
                    let entries = entries_for_write.clone();
                    Box::pin(async move {
                        for (key, value) in &entries {
                            app_store.db().put(wtxn, key.as_bytes(), value)?;
                        }
                        Ok(())
                    })
                })
                .await
        });

        write_started_rx.await.unwrap();

        // Step 3: wait until the resize path holds (or waits for) the coordinator
        // write lock — impossible while our read guard is still alive.
        let mut resize_blocked = false;
        while !write_handle.is_finished() {
            if coord.try_write().is_err() {
                resize_blocked = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            resize_blocked,
            "write should reach MapFull and block on resize while reader is held"
        );
        assert!(
            !write_handle.is_finished(),
            "write should not finish before the read txn is dropped"
        );

        // Step 4: release the read transaction (txn drops before its guard).
        drop(reader);

        // Step 5: write completes, map grows, data is readable.
        write_handle
            .await
            .expect("write task panicked")
            .expect("write should succeed after reader released");

        let final_map_size = app_store.env.info().map_size;
        let expected_min_final = align_map_size_to_page(initial_map_size * MAP_SIZE_GROWTH_FACTOR);
        assert!(
            final_map_size > initial_map_size,
            "map size must grow: initial={initial_map_size}, final={final_map_size}"
        );
        assert!(
            final_map_size >= expected_min_final,
            "map size should at least double: initial={initial_map_size}, \
             final={final_map_size}, expected>={expected_min_final}"
        );

        eprintln!(
            "resize_waits_for_active_reader: initial_map_size={initial_map_size} \
             final_map_size={final_map_size}"
        );

        let rtxn = app_store.read_txn().await.unwrap();
        for key in ["key_0000", "key_0031", "key_0063"] {
            let bytes = app_store
                .db()
                .get(&*rtxn, key.as_bytes())
                .unwrap()
                .unwrap_or_else(|| panic!("{key} should exist after successful write"));
            assert_eq!(
                bytes.as_ref(),
                payload.as_slice(),
                "{key} payload should match what was written"
            );
        }
    }

    #[tokio::test]
    async fn obligation_summary_migrates_on_open_and_rolls_back_atomically() {
        let dir = TempDir::new().unwrap();
        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: 16 * 1024 * 1024,
        };
        let storage = Storage::new(&settings).await.unwrap();
        let store = storage.create_app_store("summary-migration").await.unwrap();

        let completed = effect_intent("summary:completed");
        let pending = effect_intent("summary:pending");
        let store_for_txn = store.clone();
        let (completed_id, pending_id) = storage
            .run_txn(move |txn| {
                let store = store_for_txn.clone();
                let completed = completed.clone();
                let pending = pending.clone();
                Box::pin(async move {
                    let completed = store
                        .bind_native_effect_lineage_in_txn(txn, completed)
                        .await?;
                    let pending = store
                        .bind_native_effect_lineage_in_txn(txn, pending)
                        .await?;
                    let completed_id = completed.evidence_id().to_owned();
                    let pending_id = pending.evidence_id().to_owned();
                    store
                        .upsert_native_effect_intent_in_txn(txn, &completed)
                        .await?;
                    store
                        .upsert_native_effect_intent_in_txn(txn, &pending)
                        .await?;
                    Ok((completed_id, pending_id))
                })
            })
            .await
            .unwrap();
        store
            .mark_native_effects_verified(std::slice::from_ref(&completed_id))
            .await
            .unwrap();
        let store_for_txn = store.clone();
        let completed_for_txn = completed_id.clone();
        storage
            .run_txn(move |txn| {
                let store = store_for_txn.clone();
                let completed_id = completed_for_txn.clone();
                Box::pin(async move {
                    store
                        .finalize_native_effects_in_txn(txn, &[completed_id])
                        .await
                })
            })
            .await
            .unwrap();

        let tombstone_parent = StablePath::root();
        let tombstone_relative = component_path("summary-tombstone");
        let tombstone = ChildTombstoneInfo::new(
            ChildTombstoneCause::ComponentOrphan,
            None,
            Some(7),
            NativeVerificationPolicy::QueryVerified,
        )
        .unwrap();
        let store_for_txn = store.clone();
        let parent_for_txn = tombstone_parent.clone();
        let relative_for_txn = tombstone_relative.clone();
        storage
            .run_txn(move |txn| {
                let store = store_for_txn.clone();
                let parent = parent_for_txn.clone();
                let relative = relative_for_txn.clone();
                let tombstone = tombstone.clone();
                Box::pin(async move {
                    store
                        .write_tombstone(txn, &parent, &relative, &tombstone)
                        .await
                        .map(|_| ())
                })
            })
            .await
            .unwrap();

        // Simulate a v3 app written before transactional summaries existed.
        // Evidence and tombstones stay untouched; only the additive v4 row is
        // absent and the schema marker names the prior compatible version.
        let summary_key = DbEntryKey::NativeObligationSummary.encode().unwrap();
        let schema_key = DbEntryKey::NativeSchemaVersion.encode().unwrap();
        let schema_v3 = rmp_serde::to_vec_named(&NativeSchemaVersion(3)).unwrap();
        let store_for_txn = store.clone();
        storage
            .run_txn(move |txn| {
                let store = store_for_txn.clone();
                let summary_key = summary_key.clone();
                let schema_key = schema_key.clone();
                let schema_v3 = schema_v3.clone();
                Box::pin(async move {
                    assert!(store.db().delete(&mut **txn, &summary_key)?);
                    store.db().put(&mut **txn, &schema_key, &schema_v3)?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let reopened = storage
            .open_app_store_by_name("summary-migration")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            reopened.validate_native_schema().await.unwrap(),
            Some(NativeSchemaVersion::CURRENT)
        );
        let counts = reopened.native_effect_obligation_counts().await.unwrap();
        assert_eq!(counts.completed, 1);
        assert_eq!(counts.pending, 1);
        assert!(
            reopened
                .has_query_verified_tombstone_obligations()
                .await
                .unwrap()
        );

        // A current marker without its summary can only result from external
        // corruption (the real migration is atomic), but open heals it by a
        // validated rebuild instead of trusting zero counts.
        let summary_key = DbEntryKey::NativeObligationSummary.encode().unwrap();
        let reopened_for_txn = reopened.clone();
        storage
            .run_txn(move |txn| {
                let store = reopened_for_txn.clone();
                let summary_key = summary_key.clone();
                Box::pin(async move {
                    assert!(store.db().delete(&mut **txn, &summary_key)?);
                    Ok(())
                })
            })
            .await
            .unwrap();
        let recovered = storage
            .open_app_store_by_name("summary-migration")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            recovered.native_effect_obligation_counts().await.unwrap(),
            counts
        );

        // Counter changes share the exact write transaction with evidence and
        // tombstone changes. An aborted final commit must roll all three back.
        recovered
            .mark_native_effects_verified(std::slice::from_ref(&pending_id))
            .await
            .unwrap();
        let recovered_for_txn = recovered.clone();
        let pending_for_txn = pending_id.clone();
        let parent_for_txn = tombstone_parent.clone();
        let relative_for_txn = tombstone_relative.clone();
        let error = storage
            .run_txn(move |txn| {
                let store = recovered_for_txn.clone();
                let pending_id = pending_for_txn.clone();
                let parent = parent_for_txn.clone();
                let relative = relative_for_txn.clone();
                Box::pin(async move {
                    store
                        .finalize_native_effects_in_txn(txn, &[pending_id])
                        .await?;
                    assert!(
                        store
                            .delete_tombstone(txn, &parent, &relative, Some(7))
                            .await?
                    );
                    Err::<(), _>(internal_error!("intentional summary rollback"))
                })
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("intentional summary rollback"));
        let rolled_back = recovered.native_effect_obligation_counts().await.unwrap();
        assert_eq!(rolled_back.completed, 1);
        assert_eq!(rolled_back.verified, 1);
        assert!(
            recovered
                .has_query_verified_tombstone_obligations()
                .await
                .unwrap()
        );
        assert_eq!(
            recovered.native_effect_counts().await.unwrap(),
            rolled_back,
            "validated evidence scan and transactional summary must agree"
        );
    }

    /// Child side of `adopts_cross_process_map_resize_on_all_open_paths`.
    /// The parent opens the environment first; this process then publishes a
    /// larger map through the same cross-process resize protocol and exits.
    #[cfg(unix)]
    #[tokio::test]
    async fn external_map_resize_child_process() {
        let Some(db_path) = std::env::var_os("SYNOR_MAP_RESIZE_CHILD_DB") else {
            return;
        };
        let initial_map_size: usize = std::env::var("SYNOR_MAP_RESIZE_INITIAL")
            .unwrap()
            .parse()
            .unwrap();
        let round = std::env::var("SYNOR_MAP_RESIZE_ROUND").unwrap();
        let storage = Storage::new(&StorageSettings {
            db_path: PathBuf::from(db_path),
            lmdb_max_dbs: 8,
            lmdb_map_size: initial_map_size,
        })
        .await
        .unwrap();
        let app_store = storage.create_app_store("external-resize").await.unwrap();
        let observed = storage.inner.db_env.info().map_size;
        assert!(
            observed <= 64 * 1024 * 1024,
            "test fixture unexpectedly opened a map larger than 64 MiB"
        );
        const PAYLOAD_LEN: usize = 16 * 1024;
        let write_count = observed.checked_mul(2).unwrap().div_ceil(PAYLOAD_LEN);
        let payload = vec![0xE7; PAYLOAD_LEN];
        let entries: Vec<(String, Vec<u8>)> = (0..write_count)
            .map(|index| (format!("external-{round}-{index:08}"), payload.clone()))
            .collect();
        let app_store_for_txn = app_store.clone();
        storage
            .run_txn(move |txn| {
                let app_store = app_store_for_txn.clone();
                let entries = entries.clone();
                Box::pin(async move {
                    for (key, value) in &entries {
                        app_store.db().put(&mut **txn, key.as_bytes(), value)?;
                    }
                    Ok(())
                })
            })
            .await
            .unwrap();
        let grown = storage.inner.db_env.info().map_size;
        assert!(grown > observed);
        storage.inner.db_env.force_sync().unwrap();
    }

    #[cfg(unix)]
    async fn grow_map_in_child(db_path: &Path, initial_map_size: usize, round: usize) {
        let status = tokio::task::spawn_blocking({
            let db_path = db_path.to_path_buf();
            move || {
                std::process::Command::new(std::env::current_exe().unwrap())
                    .arg("--exact")
                    .arg("state_store::storage::tests::external_map_resize_child_process")
                    .arg("--nocapture")
                    .env("SYNOR_MAP_RESIZE_CHILD_DB", db_path)
                    .env("SYNOR_MAP_RESIZE_INITIAL", initial_map_size.to_string())
                    .env("SYNOR_MAP_RESIZE_ROUND", round.to_string())
                    .status()
                    .unwrap()
            }
        })
        .await
        .unwrap();
        assert!(
            status.success(),
            "external map-resize child failed: {status}"
        );
    }

    /// Deterministic two-process regression for `MDB_MAP_RESIZED`:
    ///
    /// 1. Keep one environment mapped in the parent.
    /// 2. Grow the same LMDB environment from a child process.
    /// 3. Prove async read, batched write, and blocking streaming-read paths
    ///    each adopt the externally published size and retry successfully.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adopts_cross_process_map_resize_on_all_open_paths() {
        let dir = TempDir::new().unwrap();
        let initial_map_size = align_map_size_to_page(page_size::get() * 32);
        let settings = StorageSettings {
            db_path: dir.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: initial_map_size,
        };
        let storage = Storage::new(&settings).await.unwrap();
        let app_store = storage.create_app_store("external-resize").await.unwrap();

        grow_map_in_child(&settings.db_path, initial_map_size, 1).await;
        let rtxn = app_store
            .read_txn()
            .await
            .expect("read path must adopt an external resize");
        let first_child_size = app_store.env.info().map_size;
        assert!(first_child_size > initial_map_size);
        drop(rtxn);

        grow_map_in_child(&settings.db_path, initial_map_size, 2).await;
        let app_store_for_write = app_store.clone();
        storage
            .run_txn(move |txn| {
                let app_store = app_store_for_write.clone();
                Box::pin(async move {
                    app_store
                        .db()
                        .put(&mut **txn, b"after-external-resize", b"committed")?;
                    Ok(())
                })
            })
            .await
            .expect("write path must adopt an external resize");
        let second_child_size = app_store.env.info().map_size;
        assert!(second_child_size > first_child_size);

        grow_map_in_child(&settings.db_path, initial_map_size, 3).await;
        let mut receiver = storage.spawn_stable_path_iter(app_store.clone()).await;
        assert!(
            receiver.recv().await.is_none(),
            "empty streaming read should complete after adopting the external resize"
        );
        assert!(app_store.env.info().map_size > second_child_size);

        let rtxn = app_store.read_txn().await.unwrap();
        assert_eq!(
            app_store
                .db()
                .get(&*rtxn, b"after-external-resize")
                .unwrap(),
            Some(&b"committed"[..])
        );
    }

    /// LMDB's compact-copy API opens its own read transaction internally, so it
    /// needs the same cross-process map-resize adoption as explicit read paths.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn downgrade_compact_copy_adopts_cross_process_map_resize() {
        let source = TempDir::new().unwrap();
        let staging_parent = TempDir::new().unwrap();
        let initial_map_size = align_map_size_to_page(page_size::get() * 32);
        let settings = StorageSettings {
            db_path: source.path().to_path_buf(),
            lmdb_max_dbs: 8,
            lmdb_map_size: initial_map_size,
        };
        let storage = Storage::new(&settings).await.unwrap();
        storage.create_app_store("external-resize").await.unwrap();

        grow_map_in_child(&settings.db_path, initial_map_size, 1).await;
        assert_eq!(
            storage.inner.db_env.info().map_size,
            initial_map_size,
            "parent must still have the stale map size before compact copy"
        );

        let staging_path = staging_parent.path().join("downgrade-copy");
        let result = storage
            .prepare_native_downgrade_copy(&staging_path)
            .await
            .expect("compact copy must adopt an externally published map size and retry");

        assert!(storage.inner.db_env.info().map_size > initial_map_size);
        assert!(staging_path.join("mdb/data.mdb").is_file());
        assert_eq!(result.apps.len(), 1);
        assert_eq!(result.apps[0].app_name, "external-resize");
    }
}
