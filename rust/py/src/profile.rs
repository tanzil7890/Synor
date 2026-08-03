use std::collections::HashSet;

use pyo3::types::{
    PyAnyMethods, PyBool, PyByteArray, PyBytes, PyCFunction, PyCode, PyDict, PyFloat, PyFrozenSet,
    PyFunction, PyInt, PyList, PyModule, PySet, PyString, PyTuple, PyType,
};
use synor_core::engine::profile::EngineProfile;

use crate::{
    component::PyComponentProcessor,
    prelude::*,
    target_state::{
        PyTargetActionSinkInner, PyTargetHandler, bound_verified_action_retained_members,
    },
};

fn try_python_buffer_len_bytes(value: &Bound<'_, PyAny>, flags: std::ffi::c_int) -> Option<usize> {
    // Some exporters make fields in `Py_buffer` point back into the view, so
    // keep its address stable between acquisition and release.
    let mut view = Box::new(pyo3::ffi::Py_buffer::new());
    // SAFETY: `value` is live and GIL-bound, `view` points to writable storage,
    // and a successful acquisition is paired with exactly one release below.
    let status = unsafe { pyo3::ffi::PyObject_GetBuffer(value.as_ptr(), view.as_mut(), flags) };
    if status != 0 {
        // A buffer-capable object may legitimately reject a particular request
        // shape. Clear that request's exception before trying a less/differently
        // structured view. `take` still resumes a PyO3 PanicException.
        drop(PyErr::take(value.py()));
        return None;
    }

    let len_bytes = usize::try_from(view.len).ok();
    // SAFETY: the acquisition above succeeded, and this is its one matching
    // release. No buffer fields are used after this call.
    unsafe { pyo3::ffi::PyBuffer_Release(view.as_mut()) };
    len_bytes
}

fn python_buffer_len_bytes(value: &Bound<'_, PyAny>) -> usize {
    // `PyBUF_SIMPLE` is the least demanding request and covers contiguous
    // exporters. Non-contiguous and pointer-indirect layouts reject it, so
    // retry with structural metadata but without the unnecessary FORMAT flag.
    // Preserve compatibility with exporters that only accept the fully
    // formatted request PyO3 historically used as a final fallback.
    // `PyObject_CheckBuffer` does not guarantee any acquisition succeeds; an
    // unmeasurable native buffer must fail closed rather than count as zero.
    [
        pyo3::ffi::PyBUF_SIMPLE,
        pyo3::ffi::PyBUF_INDIRECT,
        pyo3::ffi::PyBUF_FULL_RO,
    ]
    .into_iter()
    .find_map(|flags| try_python_buffer_len_bytes(value, flags))
    .unwrap_or(usize::MAX)
}

fn shallow_python_size(value: &Bound<'_, PyAny>) -> usize {
    // Calling ``obj.__sizeof__()`` on an arbitrary user type dispatches user
    // code at a backpressure boundary. Restrict that call to exact built-in
    // types whose implementation is CPython-owned; custom objects receive a
    // conservative header estimate and their retained graph is still walked
    // through ``gc.get_referents`` below.
    let has_builtin_size = value.is_none()
        || value.is_exact_instance_of::<PyBool>()
        || value.is_exact_instance_of::<PyInt>()
        || value.is_exact_instance_of::<PyFloat>()
        || value.is_exact_instance_of::<PyString>()
        || value.is_exact_instance_of::<PyBytes>()
        || value.is_exact_instance_of::<PyByteArray>()
        || value.is_exact_instance_of::<PyList>()
        || value.is_exact_instance_of::<PyDict>()
        || value.is_exact_instance_of::<PySet>()
        || value.is_exact_instance_of::<PyFrozenSet>()
        || value.is_exact_instance_of::<PyTuple>();
    let object_header_size = std::mem::size_of::<pyo3::ffi::PyObject>()
        .max(std::mem::size_of::<Py<PyAny>>())
        .max(1);
    let object_size = if has_builtin_size {
        value
            .call_method0(pyo3::intern!(value.py(), "__sizeof__"))
            .and_then(|size| size.extract::<usize>())
            .unwrap_or(object_header_size)
    } else {
        object_header_size
    };

    // Many extension types own substantial memory outside Python's GC graph.
    // NumPy arrays, `array.array`, mmap objects, and non-contiguous array views
    // expose that storage through the native buffer protocol but may report no
    // referents at all. Include the logical buffer extent so byte admission
    // cannot be bypassed with a pointer-sized Python wrapper. Exact bytes and
    // bytearray sizes already include their allocation; other buffer exporters
    // retain both their Python wrapper and the exposed buffer extent.
    let has_separately_sized_buffer =
        if value.is_exact_instance_of::<PyBytes>() || value.is_exact_instance_of::<PyByteArray>() {
            false
        } else {
            // SAFETY: `value` is a live, GIL-bound object and this predicate only
            // inspects its type's buffer slot.
            (unsafe { pyo3::ffi::PyObject_CheckBuffer(value.as_ptr()) }) != 0
        };
    let buffer_size = if has_separately_sized_buffer {
        python_buffer_len_bytes(value)
    } else {
        0
    };

    object_size.saturating_add(buffer_size).max(1)
}

/// Estimate memory retained by a Python value without dispatching arbitrary
/// iteration, attribute, or `__sizeof__` implementations. Exact built-in
/// containers are sized and traversed directly. Buffer-capable objects are
/// acquired through Python's native buffer protocol so extension-owned payloads
/// participate in admission; acquisition invokes the exporter's buffer hook but
/// the payload is never read. Non-callable user objects use `gc.get_referents`,
/// which inspects the native GC graph without dispatching `__iter__`, `__dict__`,
/// properties, or custom attribute access. This covers ordinary objects and
/// slot-based dataclasses while deliberately excluding globally shared runtime
/// objects such as types, modules, function implementations, and code objects.
/// Callable user objects are still traversed because their instance fields may
/// retain the payload.
///
/// An explicit stack prevents deeply nested values from overflowing the Rust
/// stack, while object identities make shared references and cycles count once.
pub(crate) fn python_retained_size_bytes(root: &Bound<'_, PyAny>) -> usize {
    let py = root.py();
    let get_referents = PyModule::import(py, "gc")
        .and_then(|module| module.getattr("get_referents"))
        .ok();
    let mut pending = vec![root.clone()];
    let mut seen = HashSet::new();
    let mut total = 0usize;

    while let Some(value) = pending.pop() {
        let identity = value.as_ptr() as usize;
        if !seen.insert(identity) {
            continue;
        }

        total = total.saturating_add(shallow_python_size(&value));

        if let Some((action, descriptor_heap_bytes)) =
            bound_verified_action_retained_members(py, &value)
        {
            total = total.saturating_add(descriptor_heap_bytes);
            pending.push(action.into_bound(py));
            continue;
        }

        if value.is_exact_instance_of::<PyList>() {
            // The exact-type check keeps this on CPython's built-in container
            // path instead of executing a user-defined iteration protocol.
            if let Ok(list) = value.cast::<PyList>() {
                pending.extend(list.iter());
            }
            continue;
        }

        if value.is_exact_instance_of::<PyDict>() {
            if let Ok(dict) = value.cast::<PyDict>() {
                for (key, item) in dict.iter() {
                    pending.push(key);
                    pending.push(item);
                }
            }
            continue;
        }

        if value.is_exact_instance_of::<PySet>() {
            if let Ok(set) = value.cast::<PySet>() {
                pending.extend(set.iter());
            }
            continue;
        }

        if value.is_exact_instance_of::<PyFrozenSet>() {
            if let Ok(set) = value.cast::<PyFrozenSet>() {
                pending.extend(set.iter());
            }
            continue;
        }

        // NamedTuple values are tuple subclasses. Traversing every tuple
        // instance is safe here because PyTupleMethods reads the native tuple
        // storage directly and does not dispatch to an overridden iterator.
        if value.is_instance_of::<PyTuple>()
            && let Ok(tuple) = value.cast::<PyTuple>()
        {
            pending.extend(tuple.iter());
            continue;
        }

        // Atomic built-ins report their owned payload in `__sizeof__` and do
        // not need a comparatively expensive GC-graph query.
        if value.is_none()
            || value.is_exact_instance_of::<PyBool>()
            || value.is_exact_instance_of::<PyInt>()
            || value.is_exact_instance_of::<PyFloat>()
            || value.is_exact_instance_of::<PyString>()
            || value.is_exact_instance_of::<PyBytes>()
            || value.is_exact_instance_of::<PyByteArray>()
        {
            continue;
        }

        if is_shared_runtime_object(&value) {
            continue;
        }

        if let Some(get_referents) = &get_referents
            && let Ok(referents) = get_referents.call1((value,))
            && let Ok(referents) = referents.cast::<PyList>()
        {
            pending.extend(
                referents
                    .iter()
                    .filter(|referent| !is_shared_runtime_object(referent)),
            );
        }
    }

    total.max(1)
}

fn is_shared_runtime_object(value: &Bound<'_, PyAny>) -> bool {
    value.is_instance_of::<PyType>()
        || value.is_instance_of::<PyModule>()
        || value.is_instance_of::<PyFunction>()
        || value.is_instance_of::<PyCFunction>()
        || value.is_instance_of::<PyCode>()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct PyEngineProfile;

impl EngineProfile for PyEngineProfile {
    type HostRuntimeCtx = crate::runtime::PyAsyncContext;
    type HostCtx = Py<PyAny>;

    type ComponentProc = PyComponentProcessor;
    type FunctionData = crate::value::PyStoredValue;

    type TargetHdl = PyTargetHandler;
    type TargetStateTrackingRecord = crate::value::PyStoredValue;
    type TargetAction = Py<PyAny>;
    type TargetActionSink = PyTargetActionSinkInner;
    type TargetStateValue = Py<PyAny>;

    fn derive_host_callback_context(
        host_runtime_ctx: &Self::HostRuntimeCtx,
    ) -> Self::HostRuntimeCtx {
        host_runtime_ctx.derive_callback_scope()
    }

    fn target_state_value_size_bytes(value: &Self::TargetStateValue) -> usize {
        Python::attach(|py| python_retained_size_bytes(value.bind(py)))
    }

    fn target_action_size_bytes(action: &Self::TargetAction) -> usize {
        Python::attach(|py| python_retained_size_bytes(action.bind(py)))
    }

    fn acquire_host_operation(
        host_runtime_ctx: &Self::HostRuntimeCtx,
    ) -> Result<Box<dyn Send + Sync>> {
        Ok(Box::new(host_runtime_ctx.acquire_operation_guard()?))
    }

    fn host_live_operation_cancelled(
        host_runtime_ctx: &Self::HostRuntimeCtx,
    ) -> futures::future::BoxFuture<'static, ()> {
        let host_runtime_ctx = host_runtime_ctx.clone();
        Box::pin(async move {
            host_runtime_ctx.live_operations_cancelled().await;
        })
    }

    fn drain_host_callbacks(
        host_runtime_ctx: &Self::HostRuntimeCtx,
    ) -> futures::future::BoxFuture<'static, ()> {
        let host_runtime_ctx = host_runtime_ctx.clone();
        Box::pin(async move {
            host_runtime_ctx.drain_callbacks().await;
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::{c_int, c_void},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use pyo3::{
        exceptions::PyBufferError,
        ffi,
        prelude::*,
        types::{PyAnyMethods, PyBytes, PyDictMethods, PyListMethods, PyModule},
    };
    use synor_core::engine::profile::EngineProfile;

    use super::{PyEngineProfile, python_retained_size_bytes};
    use crate::target_state::bind_verified_target_action;

    #[pyclass]
    struct SimpleOnlyBuffer {
        payload: Vec<u8>,
        releases: Arc<AtomicUsize>,
    }

    #[pymethods]
    impl SimpleOnlyBuffer {
        unsafe fn __getbuffer__(
            slf: Bound<'_, Self>,
            view: *mut ffi::Py_buffer,
            flags: c_int,
        ) -> PyResult<()> {
            if flags != ffi::PyBUF_SIMPLE {
                return Err(PyBufferError::new_err(
                    "test exporter only supports PyBUF_SIMPLE",
                ));
            }
            if view.is_null() {
                return Err(PyBufferError::new_err("buffer view is null"));
            }

            let borrowed = slf.borrow();
            let payload_ptr = borrowed.payload.as_ptr() as *mut c_void;
            let payload_len = borrowed.payload.len() as isize;
            drop(borrowed);
            // SAFETY: the payload is owned by `slf`, is never mutated, and
            // PyBuffer_FillInfo retains the exporter until release.
            let status = unsafe {
                ffi::PyBuffer_FillInfo(view, slf.as_ptr(), payload_ptr, payload_len, 1, flags)
            };
            if status == 0 {
                Ok(())
            } else {
                Err(PyErr::fetch(slf.py()))
            }
        }

        unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {
            self.releases.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[pyclass]
    struct RejectingBuffer;

    #[pymethods]
    impl RejectingBuffer {
        unsafe fn __getbuffer__(
            _slf: Bound<'_, Self>,
            view: *mut ffi::Py_buffer,
            _flags: c_int,
        ) -> PyResult<()> {
            if !view.is_null() {
                // SAFETY: the interpreter supplied a valid output view. The
                // buffer protocol requires `obj` to be null on failure.
                unsafe { (*view).obj = std::ptr::null_mut() };
            }
            Err(PyBufferError::new_err(
                "test exporter rejects every request",
            ))
        }
    }

    #[test]
    fn named_tuple_payload_counts_toward_retained_size_limit() {
        const TEST_LIMIT_BYTES: usize = 4 * 1024;

        Python::attach(|py| {
            let collections = PyModule::import(py, "collections").unwrap();
            let record_type = collections
                .getattr("namedtuple")
                .unwrap()
                .call1(("Record", "payload"))
                .unwrap();
            let payload = PyBytes::new(py, &vec![0; TEST_LIMIT_BYTES + 1]);
            let record = record_type.call1((payload,)).unwrap();

            let retained = python_retained_size_bytes(&record);
            assert!(
                retained > TEST_LIMIT_BYTES,
                "NamedTuple payload must be included so the engine's declaration cap rejects it"
            );

            let owned = record.unbind();
            assert_eq!(
                PyEngineProfile::target_state_value_size_bytes(&owned),
                retained
            );
        });
    }

    #[test]
    fn slot_dataclass_payload_counts_toward_retained_size_limit() {
        const TEST_LIMIT_BYTES: usize = 4 * 1024;

        Python::attach(|py| {
            let dataclasses = PyModule::import(py, "dataclasses").unwrap();
            let fields = pyo3::types::PyList::new(py, ["payload"]).unwrap();
            let kwargs = pyo3::types::PyDict::new(py);
            kwargs.set_item("slots", true).unwrap();
            let record_type = dataclasses
                .getattr("make_dataclass")
                .unwrap()
                .call(("SlotRecord", fields), Some(&kwargs))
                .unwrap();
            let payload = PyBytes::new(py, &vec![0; TEST_LIMIT_BYTES + 1]);
            let record = record_type.call1((payload,)).unwrap();

            assert!(python_retained_size_bytes(&record) > TEST_LIMIT_BYTES);
        });
    }

    #[test]
    fn callable_user_object_payload_is_not_mistaken_for_shared_function_state() {
        const TEST_LIMIT_BYTES: usize = 4 * 1024;

        Python::attach(|py| {
            let namespace = pyo3::types::PyDict::new(py);
            py.run(
                pyo3::ffi::c_str!(
                    "class CallableRecord:\n\
                     \x20   __slots__ = ('payload',)\n\
                     \x20   def __init__(self, payload): self.payload = payload\n\
                     \x20   def __call__(self): return None"
                ),
                None,
                Some(&namespace),
            )
            .unwrap();
            let record_type = namespace.get_item("CallableRecord").unwrap().unwrap();
            let payload = PyBytes::new(py, &vec![0; TEST_LIMIT_BYTES + 1]);
            let record = record_type.call1((payload,)).unwrap();

            assert!(python_retained_size_bytes(&record) > TEST_LIMIT_BYTES);
        });
    }

    #[test]
    fn retained_size_does_not_dispatch_user_defined_sizeof() {
        const TEST_LIMIT_BYTES: usize = 4 * 1024;

        Python::attach(|py| {
            let namespace = pyo3::types::PyDict::new(py);
            py.run(
                pyo3::ffi::c_str!(
                    "sizeof_calls = []\n\
                     class HostileSize:\n\
                     \x20   __slots__ = ('payload',)\n\
                     \x20   def __init__(self, payload): self.payload = payload\n\
                     \x20   def __sizeof__(self):\n\
                     \x20       sizeof_calls.append(True)\n\
                     \x20       raise AssertionError('user __sizeof__ must not run')"
                ),
                None,
                Some(&namespace),
            )
            .unwrap();
            let record_type = namespace.get_item("HostileSize").unwrap().unwrap();
            let payload = PyBytes::new(py, &vec![0; TEST_LIMIT_BYTES + 1]);
            let record = record_type.call1((payload,)).unwrap();

            assert!(python_retained_size_bytes(&record) > TEST_LIMIT_BYTES);
            let calls = namespace
                .get_item("sizeof_calls")
                .unwrap()
                .unwrap()
                .cast_into::<pyo3::types::PyList>()
                .unwrap();
            assert!(calls.is_empty());
        });
    }

    #[test]
    fn memoryview_counts_its_backing_buffer() {
        const TEST_LIMIT_BYTES: usize = 4 * 1024;

        Python::attach(|py| {
            let builtins = PyModule::import(py, "builtins").unwrap();
            let payload = PyBytes::new(py, &vec![0; TEST_LIMIT_BYTES + 1]);
            let view = builtins
                .getattr("memoryview")
                .unwrap()
                .call1((payload,))
                .unwrap();

            assert!(python_retained_size_bytes(&view) > TEST_LIMIT_BYTES);
        });
    }

    #[test]
    fn simple_only_native_buffer_is_counted_and_released_once() {
        const TEST_LIMIT_BYTES: usize = 4 * 1024;

        Python::attach(|py| {
            let releases = Arc::new(AtomicUsize::new(0));
            let exporter = Py::new(
                py,
                SimpleOnlyBuffer {
                    payload: vec![0; TEST_LIMIT_BYTES + 1],
                    releases: Arc::clone(&releases),
                },
            )
            .unwrap();

            assert!(python_retained_size_bytes(exporter.bind(py).as_any()) > TEST_LIMIT_BYTES);
            assert_eq!(releases.load(Ordering::Relaxed), 1);
            assert!(!PyErr::occurred(py));
        });
    }

    #[test]
    fn strided_memoryview_uses_structured_buffer_fallback() {
        const TEST_LIMIT_BYTES: usize = 4 * 1024;

        Python::attach(|py| {
            let builtins = PyModule::import(py, "builtins").unwrap();
            let payload = PyBytes::new(py, &vec![0; (TEST_LIMIT_BYTES + 1) * 2]);
            let full_view = builtins
                .getattr("memoryview")
                .unwrap()
                .call1((payload,))
                .unwrap();
            let stride = builtins
                .getattr("slice")
                .unwrap()
                .call1((0, (TEST_LIMIT_BYTES + 1) * 2, 2))
                .unwrap();
            let strided_view = full_view.get_item(stride).unwrap();

            assert!(python_retained_size_bytes(&strided_view) > TEST_LIMIT_BYTES);
            assert!(!PyErr::occurred(py));
        });
    }

    #[test]
    fn unmeasurable_native_buffer_fails_closed_and_clears_errors() {
        Python::attach(|py| {
            let exporter = Py::new(py, RejectingBuffer).unwrap();

            assert_eq!(
                python_retained_size_bytes(exporter.bind(py).as_any()),
                usize::MAX
            );
            assert!(!PyErr::occurred(py));
        });
    }

    #[test]
    fn native_buffer_owner_counts_bytes_hidden_from_python_gc() {
        const TEST_LIMIT_BYTES: usize = 4 * 1024;

        Python::attach(|py| {
            let array_type = PyModule::import(py, "array")
                .unwrap()
                .getattr("array")
                .unwrap();
            let payload = PyBytes::new(py, &vec![0; TEST_LIMIT_BYTES + 1]);
            let array = array_type.call1(("B", payload)).unwrap();

            // `array.array` retains only its shared type in the Python GC graph;
            // its owned allocation is visible through the buffer protocol.
            assert!(python_retained_size_bytes(&array) > TEST_LIMIT_BYTES);
        });
    }

    #[test]
    fn numpy_buffer_owner_and_view_count_logical_bytes_when_available() {
        const TEST_LIMIT_BYTES: usize = 4 * 1024;

        Python::attach(|py| {
            let Ok(numpy) = PyModule::import(py, "numpy") else {
                return;
            };
            let kwargs = pyo3::types::PyDict::new(py);
            kwargs.set_item("dtype", "uint8").unwrap();
            let owner = numpy
                .getattr("zeros")
                .unwrap()
                .call((TEST_LIMIT_BYTES * 2 + 2,), Some(&kwargs))
                .unwrap();
            let builtins = PyModule::import(py, "builtins").unwrap();
            let slice = builtins
                .getattr("slice")
                .unwrap()
                .call1((0, TEST_LIMIT_BYTES * 2 + 2, 2))
                .unwrap();
            let view = owner.get_item(slice).unwrap();

            assert!(python_retained_size_bytes(&owner) > TEST_LIMIT_BYTES);
            assert!(python_retained_size_bytes(&view) > TEST_LIMIT_BYTES);
        });
    }

    #[test]
    fn pydantic_model_payload_counts_when_optional_dependency_is_available() {
        const TEST_LIMIT_BYTES: usize = 4 * 1024;

        Python::attach(|py| {
            let Ok(pydantic) = PyModule::import(py, "pydantic") else {
                return;
            };
            let field = (py.get_type::<PyBytes>(), py.Ellipsis());
            let fields = pyo3::types::PyDict::new(py);
            fields.set_item("payload", field).unwrap();
            // Pydantic otherwise infers ``__module__`` with sys._getframe(1),
            // but a Rust unit test enters Python without a Python caller frame.
            fields
                .set_item("__module__", "synor._retained_size_test")
                .unwrap();
            let record_type = pydantic
                .getattr("create_model")
                .unwrap()
                .call(("RetainedSizeRecord",), Some(&fields))
                .unwrap();
            let kwargs = pyo3::types::PyDict::new(py);
            kwargs
                .set_item("payload", PyBytes::new(py, &vec![0; TEST_LIMIT_BYTES + 1]))
                .unwrap();
            let record = record_type.call((), Some(&kwargs)).unwrap();

            assert!(python_retained_size_bytes(&record) > TEST_LIMIT_BYTES);
        });
    }

    #[test]
    fn recursive_size_counts_shared_values_once_and_terminates_on_cycles() {
        Python::attach(|py| {
            let payload = PyBytes::new(py, &[0; 1024]);
            let list = pyo3::types::PyList::empty(py);
            list.append(&payload).unwrap();
            list.append(&payload).unwrap();
            list.append(&list).unwrap();

            let retained = python_retained_size_bytes(list.as_any());
            let expected = super::shallow_python_size(list.as_any())
                + super::shallow_python_size(payload.as_any());
            assert_eq!(retained, expected);
        });
    }

    #[test]
    fn verified_action_envelope_includes_its_hidden_action_payload() {
        Python::attach(|py| {
            let payload = PyBytes::new(py, &[0; 4096]);
            let raw_size = python_retained_size_bytes(payload.as_any());
            let envelope = bind_verified_target_action(py, payload.unbind().into_any()).unwrap();
            let envelope_size = python_retained_size_bytes(envelope.bind(py));

            assert!(envelope_size > raw_size);
        });
    }
}
