use std::pin::Pin;
use std::sync::Arc;

use crate::{
    app::PyApp,
    environment::PyEnvironment,
    prelude::*,
    stable_path::{PyStableKey, PyStablePath},
};

use futures::stream::Stream;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3_async_runtimes::tokio::future_into_py;
use synor_core::engine::runtime::get_runtime;
use synor_core::inspect::db_inspect;
use synor_core::inspect::db_inspect::StablePathNodeType;
use synor_core::state::native_effect::{
    NativeEffectAppSnapshot, NativeEffectCause, NativeEffectCompactionResult, NativeEffectCounts,
    NativeEffectDowngradeResult, NativeEffectErrorCode, NativeEffectIntent, NativeEffectOperation,
    NativeEffectStatus, NativeVerificationPolicy,
};
use synor_core::state::stable_path::StableKey;

#[pyclass(name = "StablePathNodeType", skip_from_py_object)]
#[derive(Clone, Copy, Debug)]
pub struct PyStablePathNodeType(pub StablePathNodeType);

#[pymethods]
impl PyStablePathNodeType {
    #[staticmethod]
    pub fn directory() -> Self {
        Self(StablePathNodeType::Directory)
    }

    #[staticmethod]
    pub fn component() -> Self {
        Self(StablePathNodeType::Component)
    }

    pub fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    pub fn __str__(&self) -> String {
        match self.0 {
            StablePathNodeType::Directory => "Directory".to_string(),
            StablePathNodeType::Component => "Component".to_string(),
        }
    }

    pub fn __repr__(&self) -> String {
        format!("StablePathNodeType.{}", self.__str__())
    }
}

#[pyclass(name = "StablePathInfo", skip_from_py_object)]
#[derive(Clone)]
pub struct PyStablePathInfo {
    #[pyo3(get)]
    pub path: PyStablePath,
    #[pyo3(get)]
    pub node_type: PyStablePathNodeType,
}

/// Python async iterator that yields `StablePathInfo` items one-by-one (no blocking calls, no forwarder).
#[pyclass(name = "StablePathInfoAsyncIterator")]
pub struct PyStablePathInfoAsyncIterator {
    /// Stream wrapped in async Mutex to allow &self access without blocking Python thread.
    /// Pin<Box<...>> is needed because streams are not Unpin.
    stream: Arc<
        tokio::sync::Mutex<Pin<Box<dyn Stream<Item = Result<db_inspect::StablePathInfo>> + Send>>>,
    >,
}

#[pymethods]
impl PyStablePathInfoAsyncIterator {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        use futures::StreamExt;

        let stream = Arc::clone(&self.stream);
        future_into_py(py, async move {
            let mut guard = stream.lock().await;
            match StreamExt::next(&mut *guard).await {
                None => Err(PyStopAsyncIteration::new_err(())),
                Some(result) => {
                    let item = result.into_py_result()?;
                    Python::attach(|py| {
                        Py::new(
                            py,
                            PyStablePathInfo {
                                path: PyStablePath(item.path),
                                node_type: PyStablePathNodeType(item.node_type),
                            },
                        )
                        .map(|p| p.into_any())
                    })
                    .map_err(|e| e.into())
                }
            }
        })
    }
}

#[pyfunction]
pub fn iter_stable_paths<'py>(app: &PyApp, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let app_clone = app.0.clone();
    let stream = py.detach(|| {
        get_runtime().block_on(async move { db_inspect::iter_stable_paths(&app_clone).await })
    });
    wrap_stream_as_async_iterator(stream, py)
}

#[pyfunction]
pub fn iter_stable_paths_by_name<'py>(
    env: &PyEnvironment,
    app_name: &str,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let env_clone = env.0.clone();
    let app_name = app_name.to_string();
    let stream = py
        .detach(|| {
            get_runtime().block_on(async move {
                db_inspect::iter_stable_paths_by_name(&env_clone, &app_name).await
            })
        })
        .into_py_result()?;
    wrap_stream_as_async_iterator(stream, py)
}

fn wrap_stream_as_async_iterator<'py>(
    stream: impl Stream<Item = Result<db_inspect::StablePathInfo>> + Send + 'static,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    // Box and pin the stream to store it in the iterator.
    // No forwarder task needed - we poll the stream directly.
    let stream: Pin<Box<dyn Stream<Item = Result<db_inspect::StablePathInfo>> + Send>> =
        Box::pin(stream);

    let iterator = PyStablePathInfoAsyncIterator {
        stream: Arc::new(tokio::sync::Mutex::new(stream)),
    };
    Ok(Py::new(py, iterator)?.into_any().into_bound(py))
}

#[pyfunction]
pub fn list_app_names(py: Python<'_>, env: &PyEnvironment) -> PyResult<Vec<String>> {
    let env_clone = env.0.clone();
    py.detach(|| {
        get_runtime().block_on(async move { db_inspect::list_app_names(&env_clone).await })
    })
    .into_py_result()
}

/// Privacy-safe aggregate native effect statuses.
#[pyclass(name = "NativeEffectCounts", frozen, skip_from_py_object)]
#[derive(Clone, Copy)]
pub struct PyNativeEffectCounts {
    #[pyo3(get)]
    pub pending: u64,
    #[pyo3(get)]
    pub verified: u64,
    #[pyo3(get)]
    pub failed: u64,
    #[pyo3(get)]
    pub blocked: u64,
    #[pyo3(get)]
    pub completed: u64,
}

impl From<NativeEffectCounts> for PyNativeEffectCounts {
    fn from(counts: NativeEffectCounts) -> Self {
        Self {
            pending: counts.pending,
            verified: counts.verified,
            failed: counts.failed,
            blocked: counts.blocked,
            completed: counts.completed,
        }
    }
}

#[pyfunction]
pub fn native_effect_counts(py: Python<'_>, app: &PyApp) -> PyResult<PyNativeEffectCounts> {
    let app = app.0.clone();
    let counts = py
        .detach(|| {
            get_runtime().block_on(async move { db_inspect::native_effect_counts(&app).await })
        })
        .into_py_result()?;
    Ok(counts.into())
}

#[pyfunction]
pub fn native_effect_counts_by_name(
    py: Python<'_>,
    env: &PyEnvironment,
    app_name: &str,
) -> PyResult<Option<PyNativeEffectCounts>> {
    let env = env.0.clone();
    let app_name = app_name.to_string();
    let counts = py
        .detach(|| {
            get_runtime().block_on(async move {
                db_inspect::native_effect_counts_by_name(&env, &app_name).await
            })
        })
        .into_py_result()?;
    Ok(counts.map(Into::into))
}

#[pyclass(name = "_NativeEffectRecord", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyNativeEffectRecord {
    #[pyo3(get)]
    pub record_version: u16,
    #[pyo3(get)]
    pub evidence_id: String,
    #[pyo3(get)]
    pub action_id: String,
    #[pyo3(get)]
    pub operation: String,
    #[pyo3(get)]
    pub source_digest: String,
    #[pyo3(get)]
    pub source_generation: u64,
    #[pyo3(get)]
    pub target_locator_digest: String,
    #[pyo3(get)]
    pub tracking_locator: String,
    #[pyo3(get)]
    pub verification_policy: String,
    #[pyo3(get)]
    pub cause: String,
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub created_at_unix_ms: u64,
    #[pyo3(get)]
    pub updated_at_unix_ms: u64,
    #[pyo3(get)]
    pub attempt_count: u64,
    #[pyo3(get)]
    pub last_error_code: Option<String>,
}

impl From<NativeEffectIntent> for PyNativeEffectRecord {
    fn from(effect: NativeEffectIntent) -> Self {
        let evidence_id = effect
            .evidence_id
            .clone()
            .unwrap_or_else(|| effect.descriptor.action_id.clone());
        Self {
            record_version: effect.record_version,
            evidence_id,
            action_id: effect.descriptor.action_id,
            operation: match effect.descriptor.operation {
                NativeEffectOperation::Delete => "delete",
                NativeEffectOperation::Isolate => "isolate",
                NativeEffectOperation::Cleanup => "cleanup",
            }
            .to_string(),
            source_digest: effect.descriptor.source_digest,
            source_generation: effect.descriptor.source_generation,
            target_locator_digest: effect.descriptor.target_locator_digest,
            tracking_locator: effect.tracking_locator.to_base64(),
            verification_policy: match effect.verification_policy {
                NativeVerificationPolicy::LegacyUnverified => "legacy_unverified",
                NativeVerificationPolicy::QueryVerified => "query_verified",
            }
            .to_string(),
            cause: match effect.cause {
                NativeEffectCause::Undeclared => "undeclared",
                NativeEffectCause::Explicit => "explicit",
                NativeEffectCause::OrphanedComponent => "orphaned_component",
                NativeEffectCause::ProviderMissing => "provider_missing",
            }
            .to_string(),
            status: match effect.status {
                NativeEffectStatus::Pending => "pending",
                NativeEffectStatus::Verified => "verified",
                NativeEffectStatus::Failed => "failed",
                NativeEffectStatus::Blocked => "blocked",
                NativeEffectStatus::Completed => "completed",
            }
            .to_string(),
            created_at_unix_ms: effect.created_at_unix_ms,
            updated_at_unix_ms: effect.updated_at_unix_ms,
            attempt_count: effect.attempt_count,
            last_error_code: effect.last_error_code.map(|code| {
                match code {
                    NativeEffectErrorCode::ProviderMissing => "provider_missing",
                    NativeEffectErrorCode::LegacySinkRejected => "legacy_sink_rejected",
                    NativeEffectErrorCode::SinkApplyFailed => "sink_apply_failed",
                    NativeEffectErrorCode::VerificationFailed => "verification_failed",
                    NativeEffectErrorCode::VerificationTimeout => "verification_timeout",
                    NativeEffectErrorCode::FinalCommitFailed => "final_commit_failed",
                    NativeEffectErrorCode::CleanupFailed => "cleanup_failed",
                    NativeEffectErrorCode::UnsupportedCapability => "unsupported_capability",
                    NativeEffectErrorCode::InternalState => "internal_state",
                }
                .to_string()
            }),
        }
    }
}

#[pyclass(name = "_NativeEffectSnapshot", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyNativeEffectSnapshot {
    #[pyo3(get)]
    pub schema_version: Option<u32>,
    #[pyo3(get)]
    pub effects: Vec<PyNativeEffectRecord>,
}

#[pyclass(name = "_NativeEffectAppSnapshot", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyNativeEffectAppSnapshot {
    #[pyo3(get)]
    pub app_name: String,
    #[pyo3(get)]
    pub schema_version: Option<u32>,
    #[pyo3(get)]
    pub effects: Vec<PyNativeEffectRecord>,
}

impl From<NativeEffectAppSnapshot> for PyNativeEffectAppSnapshot {
    fn from(snapshot: NativeEffectAppSnapshot) -> Self {
        Self {
            app_name: snapshot.app_name,
            schema_version: snapshot.schema_version,
            effects: snapshot.effects.into_iter().map(Into::into).collect(),
        }
    }
}

#[pyclass(name = "_NativeEffectCompactionResult", frozen, skip_from_py_object)]
#[derive(Clone, Copy)]
pub struct PyNativeEffectCompactionResult {
    #[pyo3(get)]
    pub requested: u64,
    #[pyo3(get)]
    pub deleted: u64,
    #[pyo3(get)]
    pub protected: u64,
    #[pyo3(get)]
    pub already_absent: u64,
}

impl From<NativeEffectCompactionResult> for PyNativeEffectCompactionResult {
    fn from(result: NativeEffectCompactionResult) -> Self {
        Self {
            requested: result.requested,
            deleted: result.deleted,
            protected: result.protected,
            already_absent: result.already_absent,
        }
    }
}

#[pyclass(name = "_NativeEffectDowngradeResult", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyNativeEffectDowngradeResult {
    #[pyo3(get)]
    pub apps: Vec<PyNativeEffectAppSnapshot>,
    #[pyo3(get)]
    pub removed_schema_markers: u64,
    #[pyo3(get)]
    pub removed_effects: u64,
    #[pyo3(get)]
    pub removed_obligation_cursors: u64,
    #[pyo3(get)]
    pub removed_lineage_cursors: u64,
    #[pyo3(get)]
    pub removed_live_generation_keys: u64,
}

impl From<NativeEffectDowngradeResult> for PyNativeEffectDowngradeResult {
    fn from(result: NativeEffectDowngradeResult) -> Self {
        Self {
            apps: result.apps.into_iter().map(Into::into).collect(),
            removed_schema_markers: result.removed_schema_markers,
            removed_effects: result.removed_effects,
            removed_obligation_cursors: result.removed_obligation_cursors,
            removed_lineage_cursors: result.removed_lineage_cursors,
            removed_live_generation_keys: result.removed_live_generation_keys,
        }
    }
}

#[pyfunction]
pub fn native_effect_snapshot_by_name(
    py: Python<'_>,
    env: &PyEnvironment,
    app_name: &str,
) -> PyResult<Option<PyNativeEffectSnapshot>> {
    let env = env.0.clone();
    let app_name = app_name.to_string();
    let snapshot = py
        .detach(|| {
            get_runtime().block_on(async move {
                db_inspect::native_effect_snapshot_by_name(&env, &app_name).await
            })
        })
        .into_py_result()?;
    Ok(snapshot.map(|snapshot| PyNativeEffectSnapshot {
        schema_version: snapshot.schema_version,
        effects: snapshot.effects.into_iter().map(Into::into).collect(),
    }))
}

#[pyfunction]
pub fn compact_native_effects_by_name(
    py: Python<'_>,
    env: &PyEnvironment,
    app_name: &str,
    evidence_ids: Vec<String>,
) -> PyResult<PyNativeEffectCompactionResult> {
    let env = env.0.clone();
    let app_name = app_name.to_string();
    py.detach(|| {
        get_runtime().block_on(async move {
            db_inspect::compact_native_effects_by_name(&env, &app_name, &evidence_ids).await
        })
    })
    .into_py_result()
    .map(Into::into)
}

#[pyfunction]
pub fn prepare_native_downgrade(
    py: Python<'_>,
    env: &PyEnvironment,
    staging_path: std::path::PathBuf,
) -> PyResult<PyNativeEffectDowngradeResult> {
    let env = env.0.clone();
    py.detach(|| {
        get_runtime().block_on(async move {
            db_inspect::prepare_native_downgrade(&env, &staging_path).await
        })
    })
    .into_py_result()
    .map(Into::into)
}

#[pyclass(name = "TargetStateVersion", skip_from_py_object)]
#[derive(Clone)]
pub struct PyTargetStateVersion {
    #[pyo3(get)]
    pub version: u64,
    #[pyo3(get)]
    pub state: String,
}

#[pyclass(name = "ProviderGeneration", skip_from_py_object)]
#[derive(Clone)]
pub struct PyProviderGeneration {
    #[pyo3(get)]
    pub provider_id: u64,
    #[pyo3(get)]
    pub provider_schema_version: u64,
}

#[pyclass(name = "TargetStateInfoItemSummary", skip_from_py_object)]
#[derive(Clone)]
pub struct PyTargetStateInfoItemSummary {
    #[pyo3(get)]
    pub target_state_path: String,
    #[pyo3(get)]
    pub fingerprint_path: String,
    pub key: StableKey,
    #[pyo3(get)]
    pub states: Vec<PyTargetStateVersion>,
    #[pyo3(get)]
    pub provider_schema_version: u64,
    #[pyo3(get)]
    pub provider_generation: Option<PyProviderGeneration>,
}

#[pymethods]
impl PyTargetStateInfoItemSummary {
    #[getter]
    fn key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        PyStableKey(self.key.clone()).into_pyobject(py)
    }
}

#[pyclass(name = "StablePathDetail", skip_from_py_object)]
#[derive(Clone)]
pub struct PyStablePathDetail {
    #[pyo3(get)]
    pub path: PyStablePath,
    #[pyo3(get)]
    pub node_type: PyStablePathNodeType,
    #[pyo3(get)]
    pub version: u64,
    #[pyo3(get)]
    pub processor_name: String,
    #[pyo3(get)]
    pub target_state_count: usize,
    #[pyo3(get)]
    pub has_memoization: bool,
    #[pyo3(get)]
    pub target_state_items: Vec<PyTargetStateInfoItemSummary>,
}

fn convert_detail(
    _py: Python<'_>,
    d: db_inspect::StablePathDetail,
) -> PyResult<PyStablePathDetail> {
    Ok(PyStablePathDetail {
        path: PyStablePath(d.path),
        node_type: PyStablePathNodeType(d.node_type),
        version: d.version,
        processor_name: d.processor_name,
        target_state_count: d.target_state_count,
        has_memoization: d.has_memoization,
        target_state_items: d
            .target_state_items
            .into_iter()
            .map(|item| -> PyResult<PyTargetStateInfoItemSummary> {
                Ok(PyTargetStateInfoItemSummary {
                    target_state_path: item.target_state_path,
                    fingerprint_path: item.fingerprint_path,
                    key: item.key,
                    states: item
                        .states
                        .into_iter()
                        .map(|s| PyTargetStateVersion {
                            version: s.version,
                            state: s.state,
                        })
                        .collect(),
                    provider_schema_version: item.provider_schema_version,
                    provider_generation: item.provider_generation.map(|g| PyProviderGeneration {
                        provider_id: g.provider_id,
                        provider_schema_version: g.provider_schema_version,
                    }),
                })
            })
            .collect::<PyResult<Vec<_>>>()?,
    })
}

#[pyfunction]
pub fn get_stable_path_detail(
    py: Python<'_>,
    app: &PyApp,
    path: &PyStablePath,
) -> PyResult<Option<PyStablePathDetail>> {
    let app = app.0.clone();
    let path_owned = path.0.clone();
    let detail = py
        .detach(|| {
            get_runtime().block_on(async move {
                db_inspect::get_stable_path_detail(&app, &path_owned).await
            })
        })
        .into_py_result()?;
    detail.map(|d| convert_detail(py, d)).transpose()
}

#[pyfunction]
pub fn get_stable_path_detail_by_name(
    py: Python<'_>,
    env: &PyEnvironment,
    app_name: &str,
    path: &PyStablePath,
) -> PyResult<Option<PyStablePathDetail>> {
    let env = env.0.clone();
    let app_name = app_name.to_string();
    let path_owned = path.0.clone();
    let detail = py
        .detach(|| {
            get_runtime().block_on(async move {
                db_inspect::get_stable_path_detail_by_name(&env, &app_name, &path_owned).await
            })
        })
        .into_py_result()?;
    detail.map(|d| convert_detail(py, d)).transpose()
}

/// Python async iterator that yields `StablePathDetail` items one-by-one
/// (same shape as [`PyStablePathInfoAsyncIterator`]).
#[pyclass(name = "StablePathDetailAsyncIterator")]
pub struct PyStablePathDetailAsyncIterator {
    stream: Arc<
        tokio::sync::Mutex<
            Pin<Box<dyn Stream<Item = Result<db_inspect::StablePathDetail>> + Send>>,
        >,
    >,
}

#[pymethods]
impl PyStablePathDetailAsyncIterator {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        use futures::StreamExt;

        let stream = Arc::clone(&self.stream);
        future_into_py(py, async move {
            let mut guard = stream.lock().await;
            match StreamExt::next(&mut *guard).await {
                None => Err(PyStopAsyncIteration::new_err(())),
                Some(result) => {
                    let detail = result.into_py_result()?;
                    Python::attach(|py| {
                        let converted = convert_detail(py, detail)?;
                        Py::new(py, converted).map(|p| p.into_any())
                    })
                    .map_err(|e| e.into())
                }
            }
        })
    }
}

fn wrap_detail_stream_as_async_iterator<'py>(
    stream: impl Stream<Item = Result<db_inspect::StablePathDetail>> + Send + 'static,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let stream: Pin<Box<dyn Stream<Item = Result<db_inspect::StablePathDetail>> + Send>> =
        Box::pin(stream);
    let iterator = PyStablePathDetailAsyncIterator {
        stream: Arc::new(tokio::sync::Mutex::new(stream)),
    };
    Ok(Py::new(py, iterator)?.into_any().into_bound(py))
}

#[pyfunction]
pub fn iter_stable_path_details<'py>(app: &PyApp, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let app_clone = app.0.clone();
    let stream = py.detach(|| {
        get_runtime()
            .block_on(async move { db_inspect::iter_stable_path_details(&app_clone).await })
    });
    wrap_detail_stream_as_async_iterator(stream, py)
}

#[pyfunction]
pub fn iter_stable_path_details_by_name<'py>(
    env: &PyEnvironment,
    app_name: &str,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let env_clone = env.0.clone();
    let app_name = app_name.to_string();
    let stream = py
        .detach(|| {
            get_runtime().block_on(async move {
                db_inspect::iter_stable_path_details_by_name(&env_clone, &app_name).await
            })
        })
        .into_py_result()?;
    wrap_detail_stream_as_async_iterator(stream, py)
}

#[pyclass(name = "TargetStateEntry", skip_from_py_object)]
#[derive(Clone)]
pub struct PyTargetStateEntry {
    #[pyo3(get)]
    pub fingerprint_path: String,
    #[pyo3(get)]
    pub readable_path: String,
    #[pyo3(get)]
    pub readable_segments: Vec<String>,
    #[pyo3(get)]
    pub owner_component_path: PyStablePath,
    #[pyo3(get)]
    pub dangling: bool,
}

fn convert_target_state_entry(e: db_inspect::TargetStateEntry) -> PyTargetStateEntry {
    PyTargetStateEntry {
        fingerprint_path: e.fingerprint_path,
        readable_path: e.readable_path,
        readable_segments: e.readable_segments,
        owner_component_path: PyStablePath(e.owner_component_path),
        dangling: e.dangling,
    }
}

/// Python async iterator that yields `TargetStateEntry` items one-by-one
/// (same shape as [`PyStablePathInfoAsyncIterator`]).
#[pyclass(name = "TargetStateEntryAsyncIterator")]
pub struct PyTargetStateEntryAsyncIterator {
    stream: Arc<
        tokio::sync::Mutex<
            Pin<Box<dyn Stream<Item = Result<db_inspect::TargetStateEntry>> + Send>>,
        >,
    >,
}

#[pymethods]
impl PyTargetStateEntryAsyncIterator {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        use futures::StreamExt;

        let stream = Arc::clone(&self.stream);
        future_into_py(py, async move {
            let mut guard = stream.lock().await;
            match StreamExt::next(&mut *guard).await {
                None => Err(PyStopAsyncIteration::new_err(())),
                Some(result) => {
                    let entry = result.into_py_result()?;
                    Python::attach(|py| {
                        Py::new(py, convert_target_state_entry(entry)).map(|p| p.into_any())
                    })
                    .map_err(|e| e.into())
                }
            }
        })
    }
}

fn wrap_target_state_stream_as_async_iterator<'py>(
    stream: impl Stream<Item = Result<db_inspect::TargetStateEntry>> + Send + 'static,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let stream: Pin<Box<dyn Stream<Item = Result<db_inspect::TargetStateEntry>> + Send>> =
        Box::pin(stream);
    let iterator = PyTargetStateEntryAsyncIterator {
        stream: Arc::new(tokio::sync::Mutex::new(stream)),
    };
    Ok(Py::new(py, iterator)?.into_any().into_bound(py))
}

#[pyfunction]
pub fn iter_target_states<'py>(app: &PyApp, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let app_clone = app.0.clone();
    let stream = py.detach(|| {
        get_runtime().block_on(async move { db_inspect::iter_target_states(&app_clone).await })
    });
    wrap_target_state_stream_as_async_iterator(stream, py)
}

#[pyfunction]
pub fn iter_target_states_by_name<'py>(
    env: &PyEnvironment,
    app_name: &str,
    py: Python<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let env_clone = env.0.clone();
    let app_name = app_name.to_string();
    let stream = py
        .detach(|| {
            get_runtime().block_on(async move {
                db_inspect::iter_target_states_by_name(&env_clone, &app_name).await
            })
        })
        .into_py_result()?;
    wrap_target_state_stream_as_async_iterator(stream, py)
}

#[pyfunction]
pub fn query_stable_path_details(
    py: Python<'_>,
    app: &PyApp,
    path: &PyStablePath,
    include_children: bool,
    recursive: bool,
    include_parents: bool,
) -> PyResult<Vec<PyStablePathDetail>> {
    let app = app.0.clone();
    let path_owned = path.0.clone();
    let details = py
        .detach(|| {
            get_runtime().block_on(async move {
                db_inspect::query_stable_path_details(
                    &app,
                    &path_owned,
                    include_children,
                    recursive,
                    include_parents,
                )
                .await
            })
        })
        .into_py_result()?;
    details.into_iter().map(|d| convert_detail(py, d)).collect()
}

#[pyfunction]
pub fn query_stable_path_details_by_name(
    py: Python<'_>,
    env: &PyEnvironment,
    app_name: &str,
    path: &PyStablePath,
    include_children: bool,
    recursive: bool,
    include_parents: bool,
) -> PyResult<Vec<PyStablePathDetail>> {
    let env = env.0.clone();
    let app_name = app_name.to_string();
    let path_owned = path.0.clone();
    let details = py
        .detach(|| {
            get_runtime().block_on(async move {
                db_inspect::query_stable_path_details_by_name(
                    &env,
                    &app_name,
                    &path_owned,
                    include_children,
                    recursive,
                    include_parents,
                )
                .await
            })
        })
        .into_py_result()?;
    details.into_iter().map(|d| convert_detail(py, d)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_effect_counts_preserve_each_status_total() {
        let counts = PyNativeEffectCounts::from(NativeEffectCounts {
            pending: 1,
            verified: 2,
            failed: 3,
            blocked: 4,
            completed: 5,
        });

        assert_eq!(counts.pending, 1);
        assert_eq!(counts.verified, 2);
        assert_eq!(counts.failed, 3);
        assert_eq!(counts.blocked, 4);
        assert_eq!(counts.completed, 5);
    }
}
