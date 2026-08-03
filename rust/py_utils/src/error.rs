use pyo3::exceptions::{PyRuntimeError, PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule, PyNone, PyString};
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Debug, Display};
use synor_utils::error::{CError, CResult};

pyo3::create_exception!(synor_py_utils, DeadlineExceededError, PyTimeoutError);

// A core `Reported` wrapper cannot be represented directly by Python's
// exception hierarchy. Preserve that internal delivery state on the exception
// object while it crosses a foreground Python call, then restore the wrapper
// when the exception returns to Rust. The marker is a one-crossing capability:
// Rust consumes it on re-entry, and public operation results remove it before
// handing an exception back to application code. This attribute is deliberately
// private; it does not alter the visible exception type, args, or traceback.
const REPORTED_MARKER_ATTR: &str = "__synor_failure_reported_v1__";

pub struct PythonExecutionContext {
    pub event_loop: Py<PyAny>,
}

impl PythonExecutionContext {
    pub fn new(_py: Python<'_>, event_loop: Py<PyAny>) -> Self {
        Self { event_loop }
    }
}

pub struct HostedPyErr(PyErr);

impl Display for HostedPyErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Debug for HostedPyErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let err = &self.0;
        Python::attach(|py| {
            let full_trace: PyResult<String> = (|| {
                let exc = err.value(py);
                let traceback = PyModule::import(py, "traceback")?;
                let tbe_class = traceback.getattr("TracebackException")?;
                let tbe = tbe_class.call_method1("from_exception", (exc,))?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("chain", true)?;
                let lines = tbe.call_method("format", (), Some(&kwargs))?;
                let joined = PyString::new(py, "").call_method1("join", (lines,))?;
                joined.extract::<String>()
            })();

            match full_trace {
                Ok(trace) => {
                    write!(f, "Error calling Python function:\n{trace}")?;
                }
                Err(_) => {
                    write!(f, "Error calling Python function: {err}")?;
                    if let Some(tb) = err.traceback(py) {
                        write!(f, "\n{}", tb.format().unwrap_or_default())?;
                    }
                }
            };
            Ok(())
        })
    }
}

impl std::error::Error for HostedPyErr {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl synor_utils::error::HostError for HostedPyErr {
    fn is_cancelled(&self) -> bool {
        Python::attach(|py| {
            let Ok(asyncio) = PyModule::import(py, "asyncio") else {
                return false;
            };
            let Ok(cancelled_cls) = asyncio.getattr("CancelledError") else {
                return false;
            };
            self.0.is_instance(py, &cancelled_cls)
        })
    }

    fn try_clone(&self) -> Option<Box<dyn synor_utils::error::HostError>> {
        // `PyErr::clone_ref` shares the underlying Python exception object
        // (type + value + traceback), so every batch residual recipient gets
        // the original error — catchable by type, with the full Python
        // traceback intact — instead of a flattened string.
        Python::attach(|py| Some(Box::new(HostedPyErr(self.0.clone_ref(py))) as _))
    }
}

fn set_reported_marker(err: PyErr, reported: bool) -> PyErr {
    if reported {
        Python::attach(|py| {
            // BaseException instances normally carry an attribute dictionary.
            // If an unusual host exception refuses attributes, retain its
            // observable identity and fall back to ordinary reporting.
            let _ = err.value(py).setattr(REPORTED_MARKER_ATTR, true);
        });
    }
    err
}

fn take_reported_marker(err: &PyErr) -> bool {
    Python::attach(|py| {
        let value = err.value(py);
        let reported = value
            .getattr(REPORTED_MARKER_ATTR)
            .and_then(|value| value.extract::<bool>())
            .unwrap_or(false);
        // The delivery state belongs to this single Rust -> Python -> Rust
        // crossing, not to the user's exception object for the rest of its
        // lifetime. Delete even malformed marker values so they cannot leak
        // into a later, independent raise of the same exception instance.
        let _ = value.delattr(REPORTED_MARKER_ATTR);
        reported
    })
}

fn clear_reported_marker(err: PyErr) -> PyErr {
    Python::attach(|py| {
        let _ = err.value(py).delattr(REPORTED_MARKER_ATTR);
    });
    err
}

fn cerror_to_pyerr(err: CError) -> PyErr {
    let reported = err.is_reported();
    set_reported_marker(cerror_to_unmarked_pyerr(err), reported)
}

/// Convert an error at an application-facing terminal boundary.
///
/// `Reported` is internal routing state. It is needed while an exception
/// tunnels through nested component calls, but it must not become persistent
/// state on an exception returned to user code: the user may legitimately
/// raise that same exception object in a later, independent operation.
fn cerror_to_public_pyerr(err: CError) -> PyErr {
    clear_reported_marker(cerror_to_unmarked_pyerr(err))
}

fn cerror_to_unmarked_pyerr(err: CError) -> PyErr {
    let inner = err.without_contexts();
    if let CError::HostLang(host_err) = inner {
        // Pass through tunneled Python errors as-is — preserves the
        // original exception object including traceback and any subclass
        // attributes. This applies to a tunneled `asyncio.CancelledError`
        // too: don't synthesize a fresh one from the cancellation branch
        // below; return the original.
        let any: &dyn Any = host_err.as_ref();
        if let Some(hosted_py_err) = any.downcast_ref::<HostedPyErr>() {
            return Python::attach(|py| hosted_py_err.0.clone_ref(py));
        }
        if let Some(py_err) = any.downcast_ref::<PyErr>() {
            return Python::attach(|py| py_err.clone_ref(py));
        }
    }
    // Cancellation-flavored errors that aren't tunneled Python exceptions
    // (e.g. Rust-constructed `Error::cancelled()` → `CancelledError` HostError)
    // → fresh `asyncio.CancelledError`. This lets Python callers
    // `except CancelledError` uniformly without string-matching the
    // Rust error message.
    if err.is_cancelled() {
        return Python::attach(|py| {
            let msg = format!("{}", err);
            match py
                .import("asyncio")
                .and_then(|m| m.getattr("CancelledError"))
                .and_then(|c| c.call1((msg,)))
            {
                Ok(exc) => PyErr::from_value(exc),
                Err(import_err) => import_err,
            }
        });
    }
    if err.is_deadline_exceeded() {
        return DeadlineExceededError::new_err(format!("{}", err));
    }
    if let CError::Client { .. } = inner {
        return PyValueError::new_err(format!("{}", err));
    }
    PyRuntimeError::new_err(format!("{}", err))
}

/// Convert a core error at a background-readiness boundary without retaining
/// host traceback frame locals.
///
/// A tunneled `PyErr` can retain the component's Python build frame, whose
/// locals own the Rust component context. If user code stores that exception,
/// a finished live component never becomes inactive. Preserve the original
/// exception object (including custom constructor state, subclass attributes,
/// and traceback topology), attach a rendered frame-free traceback note, and
/// clear frame locals on it and its exception chain.
fn cerror_to_detached_pyerr(err: CError) -> PyErr {
    let reported = err.is_reported();
    set_reported_marker(cerror_to_unmarked_detached_pyerr(err), reported)
}

/// Convert a detached background failure for inspection as public data.
///
/// Unlike [`cerror_to_detached_pyerr`], this deliberately removes Synor's
/// internal "already reported" routing marker. A caller may retain and later
/// raise the returned exception object as part of an independent operation;
/// internal delivery state must not leak into that later failure.
fn cerror_to_public_detached_pyerr(err: CError) -> PyErr {
    clear_reported_marker(cerror_to_unmarked_detached_pyerr(err))
}

fn traceback_frames_are_cleared(traceback: &Bound<'_, PyAny>) -> bool {
    let mut current = traceback.clone();
    while !current.is_none() {
        let Ok(frame) = current.getattr("tb_frame") else {
            return false;
        };
        let Ok(locals) = frame.getattr("f_locals") else {
            return false;
        };
        if locals.len().unwrap_or(1) != 0 {
            return false;
        }
        let Ok(next) = current.getattr("tb_next") else {
            return false;
        };
        current = next;
    }
    true
}

fn clear_traceback_frame_locals_directly(traceback: &Bound<'_, PyAny>) {
    let mut current = traceback.clone();
    let mut seen = HashSet::new();
    while !current.is_none() && seen.insert(current.as_ptr() as usize) {
        let Ok(frame) = current.getattr("tb_frame") else {
            break;
        };
        // Traceback frames are exact interpreter objects, so this invokes the
        // built-in frame.clear implementation rather than application-level
        // exception hooks. Ignore an executing-frame RuntimeError here; the
        // slot is still removed below, and legitimate readiness frames have
        // already completed by this boundary.
        let _ = frame.call_method0("clear");
        let Ok(next) = current.getattr("tb_next") else {
            break;
        };
        current = next;
    }
}

fn clear_python_exception_traceback_slot(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    normalized: &PyErr,
) {
    // Clear both PyO3's normalized error triple and the BaseException slot.
    // On Python <3.12 PyErr::set_traceback only updates the former, so the C
    // API call is required too. It bypasses hostile __setattr__ hooks.
    normalized.set_traceback(py, None);
    let status =
        unsafe { pyo3::ffi::PyException_SetTraceback(value.as_ptr(), PyNone::get(py).as_ptr()) };
    if status != 0 || PyErr::occurred(py) {
        // A valid BaseException + None cannot ordinarily fail. Consume any
        // error defensively: this helper runs while recovering from failed
        // Python-level introspection and must never leak a pending PyErr into
        // an otherwise successful readiness result conversion.
        let _ = PyErr::take(py);
    }
}

fn base_exception_group_children<'py>(
    py: Python<'py>,
    value: &Bound<'py, PyAny>,
) -> Option<Bound<'py, PyAny>> {
    // Read BaseExceptionGroup.exceptions through the built-in descriptor, not
    // through the instance. A subclass may override __getattribute__ or the
    // `exceptions` property, but cannot hide the C-managed child tuple from
    // its base descriptor.
    let group_type =
        unsafe { Bound::<PyAny>::from_borrowed_ptr(py, pyo3::ffi::PyExc_BaseExceptionGroup) };
    if !value.is_instance(&group_type).unwrap_or(false) {
        return None;
    }
    group_type
        .getattr("exceptions")
        .and_then(|descriptor| descriptor.call_method1("__get__", (value, &group_type)))
        .ok()
}

fn detach_python_exception_traceback_frames(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    seen: &mut HashSet<usize>,
) -> bool {
    if !seen.insert(value.as_ptr() as usize) {
        return true;
    }

    let normalized = PyErr::from_value(value.clone());

    // Capture the chain before clearing this traceback. PyErr's accessors use
    // CPython's exception APIs directly, so hostile __getattribute__ hooks
    // cannot conceal linked traceback frames.
    for linked in [normalized.cause(py), normalized.context(py)]
        .into_iter()
        .flatten()
    {
        detach_python_exception_traceback_frames(py, linked.value(py), seen);
    }
    // Python 3.11 exception groups retain child exceptions outside the normal
    // cause/context chain. Traverse the built-in child tuple when present.
    if let Some(children) = base_exception_group_children(py, value)
        && let Ok(children) = children.try_iter()
    {
        for child in children.flatten() {
            detach_python_exception_traceback_frames(py, &child, seen);
        }
    }

    let Some(traceback) = normalized.traceback(py) else {
        return true;
    };

    // Keep the traceback topology and line metadata for ordinary exception
    // inspection, while releasing the completed Python frames' locals. Those
    // locals can own SpawnHandle -> native component context chains and would
    // otherwise keep a finished live component active indefinitely.
    let cleared = PyModule::import(py, "traceback")
        .and_then(|module| module.call_method1("clear_frames", (&traceback,)))
        .is_ok()
        && traceback_frames_are_cleared(&traceback);

    if !cleared {
        // `traceback.clear_frames` deliberately skips an executing frame. A
        // readiness traceback should contain completed component frames only,
        // but a monkeypatched module or failed introspection can also prevent
        // it from running. Clear the shared frame objects directly first so
        // other cloned PyErr triples cannot retain their locals, then fail
        // closed by removing this exception's traceback slot. The rendered
        // note above retains diagnostics.
        clear_traceback_frame_locals_directly(&traceback);
        clear_python_exception_traceback_slot(py, value, &normalized);
    }
    cleared
}

fn cerror_to_unmarked_detached_pyerr(err: CError) -> PyErr {
    let rendered = format!("{err:?}");
    let inner = err.without_contexts();
    if let CError::HostLang(host_err) = inner {
        let any: &dyn Any = host_err.as_ref();
        let original = if let Some(hosted_py_err) = any.downcast_ref::<HostedPyErr>() {
            Some(&hosted_py_err.0)
        } else {
            any.downcast_ref::<PyErr>()
        };
        if let Some(original) = original {
            return Python::attach(|py| {
                let value = original.value(py);
                const NOTE_PREFIX: &str = "Original component traceback:\n";
                let already_noted = value
                    .getattr("__notes__")
                    .ok()
                    .and_then(|notes| notes.extract::<Vec<String>>().ok())
                    .is_some_and(|notes| notes.iter().any(|note| note.starts_with(NOTE_PREFIX)));
                if !already_noted {
                    let _ = value.call_method1("add_note", (format!("{NOTE_PREFIX}{rendered}"),));
                }
                let traceback_preserved =
                    detach_python_exception_traceback_frames(py, value, &mut HashSet::new());
                if !traceback_preserved {
                    // On Python <3.12 PyErr retains a normalized traceback
                    // pointer independently from value.__traceback__. The core
                    // may still own this original HostedPyErr after the public
                    // exception value is detached, so clear both stores.
                    original.set_traceback(py, None);
                }
                PyErr::from_value(value.clone().into_any())
            });
        }
    }
    cerror_to_unmarked_pyerr(err)
}

pub fn add_error_classes(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(
        "DeadlineExceededError",
        m.py().get_type::<DeadlineExceededError>(),
    )
}

pub trait FromPyResult<T> {
    fn from_py_result(self) -> CResult<T>;
}

impl<T> FromPyResult<T> for PyResult<T> {
    fn from_py_result(self) -> CResult<T> {
        self.map_err(|err| {
            let reported = take_reported_marker(&err);
            let err = CError::host(HostedPyErr(err));
            if reported { err.reported() } else { err }
        })
    }
}

pub trait IntoPyResult<T> {
    fn into_py_result(self) -> PyResult<T>;
}

impl<T> IntoPyResult<T> for CResult<T> {
    fn into_py_result(self) -> PyResult<T> {
        self.map_err(cerror_to_pyerr)
    }
}

pub trait IntoPublicPyResult<T> {
    fn into_public_py_result(self) -> PyResult<T>;
}

impl<T> IntoPublicPyResult<T> for CResult<T> {
    fn into_public_py_result(self) -> PyResult<T> {
        self.map_err(cerror_to_public_pyerr)
    }
}

pub trait IntoDetachedPyResult<T> {
    fn into_detached_py_result(self) -> PyResult<T>;
}

impl<T> IntoDetachedPyResult<T> for CResult<T> {
    fn into_detached_py_result(self) -> PyResult<T> {
        self.map_err(cerror_to_detached_pyerr)
    }
}

pub trait IntoPublicDetachedPyResult<T> {
    fn into_public_detached_py_result(self) -> PyResult<T>;
}

impl<T> IntoPublicDetachedPyResult<T> for CResult<T> {
    fn into_public_detached_py_result(self) -> PyResult<T> {
        self.map_err(cerror_to_public_detached_pyerr)
    }
}
