use std::{
    collections::BTreeSet,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex, OnceLock},
};

use crate::prelude::*;

use futures::FutureExt;
use pyo3::{call::PyCallArgs, exceptions::PyException};
use pyo3_async_runtimes::TaskLocals;
use synor_core::engine::runtime::{
    cancel_all, get_runtime, reset_global_cancellation, shutdown_runtime,
};
use synor_py_utils::from_py_future;
use tokio_util::task::AbortOnDropHandle;

struct CallbackTrackerState {
    last_issued: u64,
    active: BTreeSet<u64>,
    accepting: bool,
}

impl Default for CallbackTrackerState {
    fn default() -> Self {
        Self {
            last_issued: 0,
            active: BTreeSet::new(),
            accepting: true,
        }
    }
}

#[derive(Default)]
struct CallbackTracker {
    state: Mutex<CallbackTrackerState>,
    changed: tokio::sync::Notify,
}

impl CallbackTracker {
    fn acquire(self: &Arc<Self>) -> Option<CallbackRegistration> {
        let id = {
            let mut state = self.state.lock().unwrap();
            if !state.accepting {
                return None;
            }
            state.last_issued = state
                .last_issued
                .checked_add(1)
                .expect("Python callback lease ID overflowed");
            let id = state.last_issued;
            state.active.insert(id);
            id
        };
        Some(CallbackRegistration {
            id,
            tracker: self.clone(),
        })
    }

    async fn drain_started(&self) {
        let barrier = self.state.lock().unwrap().last_issued;
        self.drain_through_with_before_wait(barrier, || {}).await;
    }

    fn close_admission(&self) {
        self.state.lock().unwrap().accepting = false;
    }

    async fn drain_through_with_before_wait<F>(&self, barrier: u64, mut before_wait: F)
    where
        F: FnMut(),
    {
        loop {
            // Register before checking to avoid missing a completion between
            // the state observation and awaiting the notification. `enable()`
            // is essential: an unpolled Notified is not registered and
            // notify_waiters() in that window would otherwise be lost.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let drained = self
                .state
                .lock()
                .unwrap()
                .active
                .first()
                .is_none_or(|first| *first > barrier);
            if drained {
                return;
            }
            before_wait();
            changed.await;
        }
    }
}

struct OperationTrackerState {
    active: usize,
    accepting: bool,
}

impl Default for OperationTrackerState {
    fn default() -> Self {
        Self {
            active: 0,
            accepting: true,
        }
    }
}

#[derive(Default)]
struct OperationTracker {
    state: Mutex<OperationTrackerState>,
    changed: tokio::sync::Notify,
}

impl OperationTracker {
    fn acquire(self: &Arc<Self>) -> Option<HostOperationLease> {
        let mut state = self.state.lock().unwrap();
        if !state.accepting {
            return None;
        }
        state.active = state
            .active
            .checked_add(1)
            .expect("Python host operation count overflowed");
        Some(HostOperationLease {
            tracker: self.clone(),
        })
    }

    fn close_admission(&self) {
        self.state.lock().unwrap().accepting = false;
    }

    async fn drain(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.state.lock().unwrap().active == 0 {
                return;
            }
            changed.await;
        }
    }
}

struct CallbackRegistration {
    id: u64,
    tracker: Arc<CallbackTracker>,
}

impl Drop for CallbackRegistration {
    fn drop(&mut self) {
        self.tracker.state.lock().unwrap().active.remove(&self.id);
        self.tracker.changed.notify_waiters();
    }
}

/// One host callback registration in both its app-local drain scope and the
/// environment-wide shutdown scope.
pub struct CallbackLease {
    _scope: CallbackRegistration,
    _environment: Option<CallbackRegistration>,
}

impl CallbackLease {
    fn acquire(scope: &Arc<CallbackTracker>, environment: &Arc<CallbackTracker>) -> Option<Self> {
        // The environment gate is authoritative. Acquire it first so shutdown
        // cannot close admission between a local registration and its global
        // counterpart.
        let environment_registration = environment.acquire()?;
        if Arc::ptr_eq(scope, environment) {
            return Some(Self {
                _scope: environment_registration,
                _environment: None,
            });
        }
        let scope_registration = scope.acquire()?;
        Some(Self {
            _scope: scope_registration,
            _environment: Some(environment_registration),
        })
    }
}

pub struct HostOperationLease {
    tracker: Arc<OperationTracker>,
}

impl Drop for HostOperationLease {
    fn drop(&mut self) {
        let mut state = self.tracker.state.lock().unwrap();
        state.active = state
            .active
            .checked_sub(1)
            .expect("Python host operation lease released twice");
        drop(state);
        self.tracker.changed.notify_waiters();
    }
}

#[pyclass(name = "OperationLease")]
pub struct PyHostOperationLease(Option<HostOperationLease>);

impl PyHostOperationLease {
    pub fn take(&mut self) -> Result<HostOperationLease> {
        self.0
            .take()
            .ok_or_else(|| Error::internal_msg("host operation lease was already consumed"))
    }
}

pub struct PythonObjects {
    pub serialize_fn: Py<PyAny>,
    pub handler_wrapper_fn: Py<PyAny>,
    pub non_existence: Py<PyAny>,
    pub verified_sink_type: Py<PyAny>,
    pub verified_sink_describe_fn: Py<PyAny>,
    pub verified_sink_apply_bound_fn: Py<PyAny>,
    pub enter_environment_callback_fn: Py<PyAny>,
    pub exit_environment_callback_fn: Py<PyAny>,
}

impl PythonObjects {
    pub fn serialize<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
    ) -> Result<bytes::Bytes> {
        (|| -> PyResult<bytes::Bytes> {
            Ok(self
                .serialize_fn
                .call(py, (value,), None)?
                .extract::<bytes::Bytes>(py)?)
        })()
        .from_py_result()
    }
}

static PY_OBJECTS: OnceLock<std::mem::ManuallyDrop<PythonObjects>> = OnceLock::new();

#[pyfunction]
pub fn init_runtime(
    py: Python<'_>,
    package_id: String,
    lang: String,
    serialize_fn: Py<PyAny>,
    handler_wrapper_fn: Py<PyAny>,
    non_existence: Py<PyAny>,
    not_set: Py<PyAny>,
    verified_sink_type: Py<PyAny>,
    enter_environment_callback_fn: Py<PyAny>,
    exit_environment_callback_fn: Py<PyAny>,
) -> PyResult<()> {
    // Kept in the Python-facing signature for compatibility. They previously
    // identified telemetry events; telemetry is intentionally disabled.
    let _ = (package_id, lang, not_set);

    if let Err(_) = pyo3_async_runtimes::tokio::init_with_runtime(get_runtime()) {
        return Err(PyException::new_err(
            "Failed to initialize Tokio runtime: already initialized",
        ));
    }
    // Pin the authentic class and method objects before application code can
    // replace module attributes used by verified-sink construction.
    let verified_sink_describe_fn = verified_sink_type
        .getattr(py, "_describe_for_core")?
        .extract(py)?;
    let verified_sink_apply_bound_fn = verified_sink_type
        .getattr(py, "_call_bound_for_core")?
        .extract(py)?;
    PY_OBJECTS
        .set(std::mem::ManuallyDrop::new(PythonObjects {
            serialize_fn,
            handler_wrapper_fn,
            non_existence,
            verified_sink_type,
            verified_sink_describe_fn,
            verified_sink_apply_bound_fn,
            enter_environment_callback_fn,
            exit_environment_callback_fn,
        }))
        .map_err(|_| PyException::new_err("Failed to set Python objects: already initialized"))?;
    Ok(())
}

#[pyfunction]
pub fn shutdown_tokio_runtime() {
    shutdown_runtime();
}

/// Cancel the global cancellation token, causing all in-flight operations to
/// exit promptly.  Safe to call from signal handlers.
#[pyfunction]
#[pyo3(name = "cancel_all")]
pub fn py_cancel_all() {
    cancel_all();
}

/// Replace the cancelled global token with a fresh one so new operations can
/// proceed.  Called automatically at the start of each CLI command.
#[pyfunction]
#[pyo3(name = "reset_global_cancellation")]
pub fn py_reset_global_cancellation() {
    reset_global_cancellation();
}

pub fn python_objects() -> &'static PythonObjects {
    // ManuallyDrop<T> implements Deref<Target = T>, so &**x coerces to &T.
    &**PY_OBJECTS.get().expect("Python objects not initialized")
}

/// Wrap a Python target handler with _TypedTargetHandlerWrapper for typed deserialization.
pub fn wrap_target_handler(py: Python<'_>, handler: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    python_objects()
        .handler_wrapper_fn
        .call(py, (handler,), None)
}

#[pyclass(name = "AsyncContext", from_py_object)]
#[derive(Clone)]
pub struct PyAsyncContext(
    pub Arc<TaskLocals>,
    /// App-local callback tracker. For the environment root context this is
    /// the same Arc as the environment-wide tracker below.
    Arc<CallbackTracker>,
    Arc<OperationTracker>,
    tokio_util::sync::CancellationToken,
    Option<u64>,
    /// Shared tracker covering every callback scope in this environment.
    Arc<CallbackTracker>,
);

impl PyAsyncContext {
    pub fn acquire_callback(&self) -> Result<CallbackLease> {
        CallbackLease::acquire(&self.1, &self.5).ok_or_else(|| {
            Error::internal_msg("Python callback rejected because its environment is shutting down")
        })
    }

    /// Derive an app-owned callback drain scope while retaining the same event
    /// loop, operation admission, live-shutdown signal, callback owner, and
    /// environment-wide callback tracker.
    pub fn derive_callback_scope(&self) -> Self {
        Self(
            self.0.clone(),
            Arc::new(CallbackTracker::default()),
            self.2.clone(),
            self.3.clone(),
            self.4,
            self.5.clone(),
        )
    }

    pub fn acquire_operation_guard(&self) -> Result<HostOperationLease> {
        self.2.acquire().ok_or_else(|| {
            Error::internal_msg("host operation rejected because its environment is shutting down")
        })
    }

    pub async fn drain_callbacks(&self) {
        self.1.drain_started().await;
    }

    pub async fn live_operations_cancelled(&self) {
        self.3.cancelled().await;
    }

    fn with_environment_callback_scope<T>(
        &self,
        py: Python<'_>,
        callback: impl FnOnce() -> PyResult<T>,
    ) -> PyResult<T> {
        let token = self
            .4
            .map(|owner| {
                python_objects()
                    .enter_environment_callback_fn
                    .call1(py, (owner,))
            })
            .transpose()?;
        let result = callback();
        if let Some(token) = token
            && let Err(reset_error) = python_objects()
                .exit_environment_callback_fn
                .call1(py, (token.bind(py),))
        {
            if result.is_ok() {
                return Err(reset_error);
            }
            error!("failed to reset Python callback environment: {reset_error:?}");
        }
        result
    }
}

impl PartialEq for PyAsyncContext {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) && Arc::ptr_eq(&self.1, &other.1)
    }
}

impl Eq for PyAsyncContext {}

impl Hash for PyAsyncContext {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
        Arc::as_ptr(&self.1).hash(state);
    }
}

#[pymethods]
impl PyAsyncContext {
    #[new]
    #[pyo3(signature = (event_loop, callback_owner=None))]
    pub fn new(event_loop: Bound<PyAny>, callback_owner: Option<u64>) -> Self {
        let callback_tracker = Arc::new(CallbackTracker::default());
        Self(
            Arc::new(pyo3_async_runtimes::TaskLocals::new(event_loop)),
            callback_tracker.clone(),
            Arc::new(OperationTracker::default()),
            tokio_util::sync::CancellationToken::new(),
            callback_owner,
            callback_tracker,
        )
    }

    pub fn close_operation_admission(&self) {
        // Stop new top-level operations first. Already-admitted operations are
        // allowed to create Python callbacks until their operation leases have
        // drained; closing both gates here would strand such an operation.
        self.2.close_admission();
    }

    pub fn close_callback_admission(&self) {
        self.5.close_admission();
    }

    pub fn cancel_live_operations(&self) {
        self.3.cancel();
    }

    pub fn acquire_operation(&self) -> PyResult<PyHostOperationLease> {
        self.acquire_operation_guard()
            .map(|lease| PyHostOperationLease(Some(lease)))
            .into_py_result()
    }

    pub fn drain_operations_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let context = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            context.2.drain().await;
            Ok(())
        })
    }

    pub fn drain_callbacks_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let context = self.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            context.1.drain_started().await;
            Ok(())
        })
    }
}

#[derive(Clone)]
pub enum PyCallback {
    Sync(Arc<Py<PyAny>>),
    Async(Arc<Py<PyAny>>),
}

impl PyCallback {
    pub fn call<A>(
        &self,
        host_runtime_ctx: &PyAsyncContext,
        args: A,
    ) -> Result<impl Future<Output = Result<Py<PyAny>>> + Send + 'static>
    where
        A: for<'py> PyCallArgs<'py> + Send + 'static,
    {
        let boxed_fut = match self {
            PyCallback::Sync(sync_fn) => {
                let sync_fn = sync_fn.clone();
                let callback_lease = host_runtime_ctx.acquire_callback()?;
                let host_runtime_ctx = host_runtime_ctx.clone();
                let result_fut = AbortOnDropHandle::new(get_runtime().spawn_blocking(move || {
                    let _callback_lease = callback_lease;
                    Python::attach(|py| {
                        host_runtime_ctx
                            .with_environment_callback_scope(py, || sync_fn.call(py, args, None))
                    })
                }));
                async move {
                    result_fut.await.map_err(|err| {
                        PyException::new_err(format!("Failed to call Python function: {err:?}"))
                    })?
                }
                .boxed()
            }
            PyCallback::Async(async_fn) => {
                let callback_lease = host_runtime_ctx.acquire_callback()?;
                Python::attach(|py| {
                    host_runtime_ctx.with_environment_callback_scope(py, || {
                        let result_coro = async_fn.call(py, args, None)?;
                        from_py_future(
                            py,
                            &host_runtime_ctx.0,
                            result_coro.into_bound(py),
                            callback_lease,
                        )
                    })
                })?
                .boxed()
            }
        };
        Ok(boxed_fut.map(|r| r.from_py_result()))
    }
}

/// Wrap an optional Python async callback `(err_str) -> Awaitable[None]`
/// as the Rust `OnError` closure expected by `Component::run_in_background`,
/// `Component::delete`, and the live-component controller.
///
/// Shared by `mount_async` (single-shot background mount), `update_full_async`,
/// `update_async`, and `delete_async` (live-component ops). The propagation
/// semantics are uniform across all of them:
///
/// - Coroutine returns normally → the failure was reported successfully.
/// - Coroutine raises → reporting itself failed and the core logs that error.
/// - Dispatch-level failures (couldn't schedule the coroutine) are logged
///   and converted to `Err`.
///
/// The callback is observational: the core preserves the original component
/// failure for `handle.ready()` and the enclosing app result regardless of the
/// callback's return value.
pub fn build_on_error(
    host_runtime_ctx: PyAsyncContext,
    handler_callback: Option<Py<PyAny>>,
) -> Option<synor_core::engine::component::OnError> {
    let handler_callback = handler_callback?;
    let cb = PyCallback::Async(Arc::new(handler_callback));
    Some(Arc::new(move |err: synor_utils::prelude::Error| {
        let cb = cb.clone();
        let host_runtime_ctx = host_runtime_ctx.clone();
        Box::pin(async move {
            let err_str = format!("{err:?}");
            let fut = match cb.call(&host_runtime_ctx, (err_str,)) {
                Ok(fut) => fut,
                Err(e) => {
                    error!("exception handler dispatch failed:\n{e:?}");
                    return Err(synor_utils::prelude::Error::internal_msg(format!(
                        "exception handler dispatch failed: {e:?}"
                    )));
                }
            };
            match fut.await {
                Ok(_) => Ok(()),
                Err(e) => Err(synor_utils::prelude::Error::internal_msg(format!("{e:?}"))),
            }
        })
    }))
}

#[cfg(test)]
mod tests {
    use super::{CallbackLease, CallbackTracker, OperationTracker};
    use std::sync::Arc;

    #[tokio::test]
    async fn callback_drain_cannot_lose_completion_before_await() {
        let tracker = Arc::new(CallbackTracker::default());
        let mut lease = tracker.acquire();
        let barrier = tracker.state.lock().unwrap().last_issued;

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tracker.drain_through_with_before_wait(barrier, || drop(lease.take())),
        )
        .await
        .expect("callback drain lost the completion wakeup");
    }

    #[tokio::test]
    async fn closing_callback_gate_rejects_late_work_and_drains_existing_work() {
        let tracker = Arc::new(CallbackTracker::default());
        let lease = tracker
            .acquire()
            .expect("initial callback must be accepted");
        tracker.close_admission();
        let draining_tracker = tracker.clone();
        let drain = tokio::spawn(async move {
            draining_tracker.drain_started().await;
        });

        assert!(tracker.acquire().is_none());
        assert!(!drain.is_finished());

        drop(lease);
        drain.await.unwrap();
    }

    #[tokio::test]
    async fn app_callback_drain_excludes_sibling_scope_but_environment_tracks_both() {
        let environment = Arc::new(CallbackTracker::default());
        let app_a = Arc::new(CallbackTracker::default());
        let app_b = Arc::new(CallbackTracker::default());
        let lease_a = CallbackLease::acquire(&app_a, &environment).unwrap();
        let lease_b = CallbackLease::acquire(&app_b, &environment).unwrap();

        let drain_a = tokio::spawn({
            let app_a = app_a.clone();
            async move { app_a.drain_started().await }
        });
        let drain_environment = tokio::spawn({
            let environment = environment.clone();
            async move { environment.drain_started().await }
        });
        tokio::task::yield_now().await;
        assert!(!drain_a.is_finished());
        assert!(!drain_environment.is_finished());

        drop(lease_a);
        tokio::time::timeout(std::time::Duration::from_secs(1), drain_a)
            .await
            .expect("app A drain waited for app B")
            .unwrap();
        assert!(!drain_environment.is_finished());

        drop(lease_b);
        drain_environment.await.unwrap();
    }

    #[tokio::test]
    async fn closing_operation_gate_rejects_late_work_and_drains_existing_work() {
        let tracker = Arc::new(OperationTracker::default());
        let lease = tracker
            .acquire()
            .expect("initial operation must be accepted");
        tracker.close_admission();
        let draining_tracker = tracker.clone();
        let drain = tokio::spawn(async move {
            draining_tracker.drain().await;
        });

        assert!(tracker.acquire().is_none());
        assert!(!drain.is_finished());

        drop(lease);
        drain.await.unwrap();
    }
}
