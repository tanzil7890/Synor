use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::BoxFuture;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_async_runtimes::TaskLocals;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::error;

struct CancelOnDropPy {
    inner: BoxFuture<'static, PyResult<Py<PyAny>>>,
    cancellation: Arc<CancellationHandshake<Py<PyAny>>>,
    event_loop: Py<PyAny>,
    ctx: Py<PyAny>,
    done: AtomicBool,
}

struct CancellationState<T> {
    requested: bool,
    task: Option<T>,
}

/// Linearizes task installation with cancellation requests.
///
/// `call_soon_threadsafe` may not have run by the time the Rust future is
/// dropped.  A plain `Option<Task>` loses that cancellation: the callback later
/// installs and starts an orphan task.  This handshake guarantees that either
/// the dropper obtains the installed task or the installer observes the prior
/// request and cancels the task synchronously on the event-loop thread.
struct CancellationHandshake<T> {
    state: Mutex<CancellationState<T>>,
}

impl<T> CancellationHandshake<T> {
    fn new() -> Self {
        Self {
            state: Mutex::new(CancellationState {
                requested: false,
                task: None,
            }),
        }
    }

    /// Install a task, returning it when cancellation was already requested.
    fn install(&self, task: T) -> Option<T> {
        let mut state = self.state.lock().unwrap();
        if state.requested {
            Some(task)
        } else {
            state.task = Some(task);
            None
        }
    }

    /// Request cancellation, returning an already-installed task if present.
    fn request(&self) -> Option<T> {
        let mut state = self.state.lock().unwrap();
        state.requested = true;
        state.task.take()
    }

    fn clear_task(&self) {
        self.state.lock().unwrap().task = None;
    }
}

impl Future for CancelOnDropPy {
    type Output = PyResult<Py<PyAny>>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.inner).poll(cx) {
            Poll::Ready(out) => {
                self.done.store(true, Ordering::SeqCst);
                Poll::Ready(out)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for CancelOnDropPy {
    fn drop(&mut self) {
        if self.done.load(Ordering::SeqCst) {
            return;
        }
        if unsafe { pyo3::ffi::Py_IsInitialized() == 0 } {
            return;
        }
        let task = self.cancellation.request();
        if let Some(task) = task {
            Python::attach(|py| {
                let kwargs = PyDict::new(py);
                let result = || -> PyResult<()> {
                    // pass context so cancellation runs under the right contextvars
                    kwargs.set_item("context", self.ctx.bind(py))?;
                    self.event_loop.bind(py).call_method(
                        "call_soon_threadsafe",
                        (task.bind(py).getattr("cancel")?,),
                        Some(&kwargs),
                    )?;
                    Ok(())
                }();
                if let Err(e) = result {
                    error!("Error cancelling task: {e:?}");
                }
            });
        }
    }
}

/// Callback scheduled on the event loop thread via `call_soon_threadsafe`.
/// Creates an asyncio Task from the awaitable and sets up result forwarding.
#[pyclass]
struct CreateTaskAndBridge {
    awaitable: Option<Py<PyAny>>,
    result_tx: Option<oneshot::Sender<PyResult<Py<PyAny>>>>,
    cancellation: Arc<CancellationHandshake<Py<PyAny>>>,
    completion_guard: Option<Box<dyn Send + Sync>>,
}

#[pymethods]
impl CreateTaskAndBridge {
    fn __call__(&mut self) -> PyResult<()> {
        Python::attach(|py| {
            let awaitable = self.awaitable.take().unwrap();
            let asyncio = py.import(pyo3::intern!(py, "asyncio"))?;
            let task =
                asyncio.call_method1(pyo3::intern!(py, "ensure_future"), (awaitable.bind(py),))?;
            if let Some(task) = self.cancellation.install(task.clone().unbind()) {
                // The Rust future was dropped before task installation.  We are
                // already on the event-loop thread, so cancel synchronously;
                // asyncio will process cancellation before the coroutine's first
                // scheduled step can run.
                task.bind(py).call_method0(pyo3::intern!(py, "cancel"))?;
            }

            // Keep our own reference until registration succeeds. If a custom
            // Future rejects add_done_callback, recover the sender/guard, cancel
            // synchronously, and report the setup error without orphaning work.
            let forwarder = match Py::new(
                py,
                TaskResultForwarder {
                    tx: self.result_tx.take(),
                    cancellation: self.cancellation.clone(),
                    completion_guard: self.completion_guard.take(),
                },
            ) {
                Ok(forwarder) => forwarder,
                Err(setup_error) => {
                    self.cancellation.request();
                    if let Err(cancellation_error) = task.call_method0(pyo3::intern!(py, "cancel"))
                    {
                        return Err(cancellation_error);
                    }
                    return Err(setup_error);
                }
            };
            if let Err(setup_error) = task.call_method1(
                pyo3::intern!(py, "add_done_callback"),
                (forwarder.clone_ref(py),),
            ) {
                self.cancellation.request();
                let cancellation_error = task.call_method0(pyo3::intern!(py, "cancel")).err();
                let mut forwarder = forwarder.bind(py).borrow_mut();
                if let Some(tx) = forwarder.tx.take() {
                    let _ = tx.send(Err(setup_error));
                }
                // Cancellation happened synchronously in the same event-loop
                // callback that created the task, before its first poll.
                forwarder.completion_guard.take();
                if let Some(cancellation_error) = cancellation_error {
                    return Err(cancellation_error);
                }
            }
            Ok(())
        })
    }
}

/// Done callback added to the asyncio Task. Forwards the task result
/// through the oneshot channel when the task completes.
#[pyclass]
struct TaskResultForwarder {
    tx: Option<oneshot::Sender<PyResult<Py<PyAny>>>>,
    cancellation: Arc<CancellationHandshake<Py<PyAny>>>,
    completion_guard: Option<Box<dyn Send + Sync>>,
}

#[pymethods]
impl TaskResultForwarder {
    fn __call__(&mut self, task: Bound<PyAny>) -> PyResult<()> {
        self.cancellation.clear_task();
        if let Some(tx) = self.tx.take() {
            let result = task
                .call_method0(pyo3::intern!(task.py(), "result"))
                .map(|v| v.unbind());
            let _ = tx.send(result);
        }
        // Keep the host callback lease until the asyncio Task has reached its
        // done callback, including cancellation-before-first-poll.
        self.completion_guard.take();
        Ok(())
    }
}

pub fn from_py_future<'py, 'fut, Guard>(
    py: Python<'py>,
    locals: &TaskLocals,
    awaitable: Bound<'py, PyAny>,
    completion_guard: Guard,
) -> pyo3::PyResult<impl Future<Output = pyo3::PyResult<Py<PyAny>>> + Send + use<'fut, Guard>>
where
    Guard: Send + Sync + 'static,
{
    // 1) Capture loop + context from TaskLocals for thread-safe cancellation
    let event_loop: Bound<'py, PyAny> = locals.event_loop(py).into();
    let ctx: Bound<'py, PyAny> = locals.context(py);

    let (result_tx, result_rx) = oneshot::channel();
    let cancellation = Arc::new(CancellationHandshake::new());

    // 2) Schedule task creation on the event loop thread (thread-safe).
    //    This avoids calling event_loop.create_task() from a non-event-loop thread,
    //    which raises RuntimeError when PYTHONASYNCIODEBUG=1.
    let kwargs = PyDict::new(py);
    kwargs.set_item("context", &ctx)?;
    event_loop.call_method(
        pyo3::intern!(py, "call_soon_threadsafe"),
        (CreateTaskAndBridge {
            awaitable: Some(awaitable.unbind()),
            result_tx: Some(result_tx),
            cancellation: cancellation.clone(),
            completion_guard: Some(Box::new(completion_guard)),
        },),
        Some(&kwargs),
    )?;

    // 3) Bridge the result channel to a Rust Future
    let fut = async move {
        match result_rx.await {
            Ok(result) => result,
            Err(_) => Python::attach(|py| {
                Err(PyErr::from_value(
                    py.import(pyo3::intern!(py, "asyncio"))?
                        .call_method0(pyo3::intern!(py, "CancelledError"))?,
                ))
            }),
        }
    }
    .boxed();

    Ok(CancelOnDropPy {
        inner: fut,
        cancellation,
        event_loop: event_loop.unbind(),
        ctx: ctx.unbind(),
        done: AtomicBool::new(false),
    })
}

#[cfg(test)]
mod tests {
    use super::CancellationHandshake;

    #[test]
    fn cancellation_before_install_is_delivered_to_installer() {
        let handshake = CancellationHandshake::new();

        assert_eq!(handshake.request(), None);
        assert_eq!(handshake.install(42), Some(42));
        assert_eq!(handshake.request(), None);
    }

    #[test]
    fn cancellation_after_install_takes_installed_task() {
        let handshake = CancellationHandshake::new();

        assert_eq!(handshake.install(42), None);
        assert_eq!(handshake.request(), Some(42));
        assert_eq!(handshake.request(), None);
    }

    #[test]
    fn completed_task_is_not_retained() {
        let handshake = CancellationHandshake::new();

        assert_eq!(handshake.install(42), None);
        handshake.clear_task();
        assert_eq!(handshake.request(), None);
    }
}
