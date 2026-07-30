use std::mem::ManuallyDrop;
use std::sync::{LazyLock, Mutex};

use pyo3::types::{PyList, PySequence, PyTuple};
use synor_core::engine::target_state::{
    ChildInvalidation, ChildTargetDef, SinkAssurance, TargetActionSink, TargetActionSinkKeeper,
    TargetHandler, TargetReconcileOutput, TargetStateProvider, TargetStateProviderRegistry,
};
use synor_core::state::native_effect::{
    NativeEffectDescriptor, NativeEffectOperation, NativeVerificationPolicy,
};

use crate::context::{PyComponentProcessorContext, PyFnCallContext};
use crate::prelude::*;

use crate::stable_path::PyStableKey;

use crate::runtime::{PyAsyncContext, PyCallback, python_objects, wrap_target_handler};
use crate::value::PyStoredValue;

#[pyclass(name = "TargetActionSink", from_py_object)]
#[derive(Clone)]
pub struct PyTargetActionSink {
    keeper: TargetActionSinkKeeper<PyEngineProfile>,
}

/// Engine-only envelope binding one raw action to its native precommit proof.
/// It is intentionally not registered in the Python module and has no constructor.
#[pyclass(frozen)]
struct PyBoundVerifiedTargetAction {
    action: Py<PyAny>,
    descriptor: Mutex<PyBoundDescriptorState>,
}

enum PyBoundDescriptorState {
    Unbound,
    Binding,
    Bound(NativeEffectDescriptor),
    Failed,
}

pub struct PyTargetActionSinkInner {
    callback: PyCallback,
    describe_callback: Option<Arc<Py<PyAny>>>,
}

#[pymethods]
impl PyTargetActionSink {
    #[staticmethod]
    pub fn new_sync(callback: Py<PyAny>) -> Self {
        let inner = PyTargetActionSinkInner {
            callback: PyCallback::Sync(Arc::new(callback)),
            describe_callback: None,
        };
        Self {
            keeper: TargetActionSinkKeeper::new(inner),
        }
    }

    #[staticmethod]
    pub fn new_async(callback: Py<PyAny>) -> Self {
        let inner = PyTargetActionSinkInner {
            callback: PyCallback::Async(Arc::new(callback)),
            describe_callback: None,
        };
        Self {
            keeper: TargetActionSinkKeeper::new(inner),
        }
    }

    #[staticmethod]
    pub fn _new_verified_wrapper(py: Python<'_>, callback: Py<PyAny>) -> PyResult<Self> {
        let objects = python_objects();
        if !callback
            .bind(py)
            .get_type()
            .is(objects.verified_sink_type.bind(py))
        {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "verified core sinks must be created by VerifiedTargetActionSink",
            ));
        }
        let bind_method = |method: &Py<PyAny>| {
            method.call_method1(
                py,
                "__get__",
                (callback.bind(py), objects.verified_sink_type.bind(py)),
            )
        };
        let describe_callback = bind_method(&objects.verified_sink_describe_fn)?;
        let apply_bound_callback = bind_method(&objects.verified_sink_apply_bound_fn)?;
        let inner = PyTargetActionSinkInner {
            callback: PyCallback::Async(Arc::new(apply_bound_callback)),
            describe_callback: Some(Arc::new(describe_callback)),
        };
        Ok(Self {
            keeper: TargetActionSinkKeeper::new(inner),
        })
    }
}

fn get_core_field(py: Python<'_>, obj: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let core_obj = obj.getattr(py, "_core")?;
    let core_py = core_obj.extract::<Py<PyAny>>(py)?;
    Ok(core_py)
}

#[async_trait]
impl TargetActionSink<PyEngineProfile> for PyTargetActionSinkInner {
    fn assurance(&self) -> SinkAssurance {
        if self.describe_callback.is_some() {
            SinkAssurance::Verified(NativeVerificationPolicy::QueryVerified)
        } else {
            SinkAssurance::Legacy
        }
    }

    fn describe_effect(&self, action: &Py<PyAny>) -> Result<Option<NativeEffectDescriptor>> {
        let Some(callback) = &self.describe_callback else {
            return Ok(None);
        };
        let extracted = Python::attach(|py| -> PyResult<NativeEffectDescriptor> {
            let carrier = action
                .extract::<PyRef<'_, PyBoundVerifiedTargetAction>>(py)
                .map_err(|_| {
                    pyo3::exceptions::PyValueError::new_err(
                        "verified target effect descriptor is invalid",
                    )
                })?;
            {
                let mut state = carrier.descriptor.lock().map_err(|_| {
                    pyo3::exceptions::PyValueError::new_err(
                        "verified target effect descriptor is invalid",
                    )
                })?;
                match &*state {
                    PyBoundDescriptorState::Unbound => {
                        *state = PyBoundDescriptorState::Binding;
                    }
                    PyBoundDescriptorState::Bound(descriptor) => {
                        return Ok(descriptor.clone());
                    }
                    PyBoundDescriptorState::Binding | PyBoundDescriptorState::Failed => {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "verified target effect descriptor is invalid",
                        ));
                    }
                }
            }
            let result = (|| {
                let (operation, action_id, source_digest, source_generation, target_locator_digest) =
                    callback.call1(py, (carrier.action.bind(py),))?.extract::<(
                        String,
                        String,
                        String,
                        u64,
                        String,
                    )>(py)?;
                let operation = match operation.as_str() {
                    "delete" => NativeEffectOperation::Delete,
                    "isolate" => NativeEffectOperation::Isolate,
                    _ => {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "verified target effect descriptor is invalid",
                        ));
                    }
                };
                let descriptor = NativeEffectDescriptor {
                    action_id,
                    operation,
                    source_digest,
                    source_generation,
                    target_locator_digest,
                };
                descriptor.validate().map_err(|_| {
                    pyo3::exceptions::PyValueError::new_err(
                        "verified target effect descriptor is invalid",
                    )
                })?;
                Ok(descriptor)
            })();
            let mut state = carrier.descriptor.lock().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err(
                    "verified target effect descriptor is invalid",
                )
            })?;
            match result {
                Ok(descriptor) => {
                    *state = PyBoundDescriptorState::Bound(descriptor.clone());
                    Ok(descriptor)
                }
                Err(error) => {
                    *state = PyBoundDescriptorState::Failed;
                    Err(error)
                }
            }
        });
        extracted
            .map(Some)
            .map_err(|_| Error::client("verified target effect descriptor is invalid"))
    }

    async fn apply(
        &self,
        host_runtime_ctx: &PyAsyncContext,
        host_ctx: Arc<Py<PyAny>>,
        actions: Vec<Py<PyAny>>,
    ) -> Result<Option<Vec<Option<ChildTargetDef<PyEngineProfile>>>>> {
        let context_provider = Python::attach(|py| host_ctx.as_ref().clone_ref(py));
        let ret = if self.describe_callback.is_some() {
            let (raw_actions, descriptors) =
                Python::attach(|py| -> PyResult<(Vec<Py<PyAny>>, Vec<_>)> {
                    let mut raw_actions = Vec::with_capacity(actions.len());
                    let mut descriptors = Vec::with_capacity(actions.len());
                    for action in actions {
                        let carrier = action
                            .extract::<PyRef<'_, PyBoundVerifiedTargetAction>>(py)
                            .map_err(|_| {
                                pyo3::exceptions::PyValueError::new_err(
                                    "verified target effect descriptor is invalid",
                                )
                            })?;
                        let state = carrier.descriptor.lock().map_err(|_| {
                            pyo3::exceptions::PyValueError::new_err(
                                "verified target effect descriptor is invalid",
                            )
                        })?;
                        let descriptor = match &*state {
                            PyBoundDescriptorState::Bound(descriptor) => descriptor,
                            _ => {
                                return Err(pyo3::exceptions::PyValueError::new_err(
                                    "verified target effect descriptor is invalid",
                                ));
                            }
                        };
                        let operation = match descriptor.operation {
                            NativeEffectOperation::Delete => "delete",
                            NativeEffectOperation::Isolate => "isolate",
                            NativeEffectOperation::Cleanup => {
                                return Err(pyo3::exceptions::PyValueError::new_err(
                                    "verified target effect descriptor is invalid",
                                ));
                            }
                        };
                        raw_actions.push(carrier.action.clone_ref(py));
                        descriptors.push((
                            operation,
                            descriptor.action_id.clone(),
                            descriptor.source_digest.clone(),
                            descriptor.source_generation,
                            descriptor.target_locator_digest.clone(),
                        ));
                    }
                    Ok((raw_actions, descriptors))
                })
                .from_py_result()?;
            self.callback
                .call(
                    host_runtime_ctx,
                    (context_provider, raw_actions, descriptors),
                )?
                .await?
        } else {
            self.callback
                .call(host_runtime_ctx, (context_provider, actions))?
                .await?
        };
        Python::attach(|py| -> PyResult<_> {
            if ret.is_none(py) {
                return Ok(None);
            }
            let seq = ret.bind(py).cast::<PySequence>()?;
            let len = seq.len()? as usize;
            let mut results: Vec<Option<ChildTargetDef<PyEngineProfile>>> = Vec::with_capacity(len);
            for i in 0..len {
                let obj = seq.get_item(i)?;
                if obj.is_none() {
                    results.push(None);
                } else {
                    // Extract handler from ChildTargetDef NamedTuple and wrap for typed deserialization
                    let (handler,) = obj.extract::<(Py<PyAny>,)>()?;
                    let wrapped = wrap_target_handler(py, &handler)?;
                    results.push(Some(ChildTargetDef {
                        handler: PyTargetHandler(wrapped),
                    }));
                }
            }
            Ok(Some(results))
        })
        .from_py_result()
    }
}

#[pyclass(name = "TargetHandler")]
pub struct PyTargetHandler(Py<PyAny>);

impl TargetHandler<PyEngineProfile> for PyTargetHandler {
    fn reconcile(
        &self,
        key: synor_core::state::stable_path::StableKey,
        desired_effect: Option<&Py<PyAny>>,
        prev_possible_records: &[PyStoredValue],
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<PyEngineProfile>>> {
        Python::attach(|py| -> PyResult<_> {
            let prev_possible_records = PyList::new(
                py,
                prev_possible_records
                    .iter()
                    .map(|s| Py::new(py, s.clone()).unwrap()),
            )?;
            let non_existence = &python_objects().non_existence;
            // `desired_effect` is a borrow from the engine's MutexGuard;
            // PyO3's `.bind(py)` takes a reference, so no clone needed
            // here. If Python retains the object across the call, its
            // own refcounting handles the lifetime.
            let py_output = self.0.call_method(
                py,
                "reconcile",
                (
                    PyStableKey(key),
                    desired_effect.unwrap_or(non_existence).bind(py),
                    prev_possible_records,
                    prev_may_be_missing,
                ),
                None,
            )?;
            let output = if py_output.is_none(py) {
                None
            } else {
                let (raw_action, sink, state, py_child_invalidation) =
                    py_output.extract::<(Py<PyAny>, Py<PyAny>, Py<PyAny>, Py<PyAny>)>(py)?;
                let sink = get_core_field(py, sink)?.extract::<PyTargetActionSink>(py)?;
                let action = match sink.keeper.assurance() {
                    SinkAssurance::Verified(NativeVerificationPolicy::QueryVerified) => Py::new(
                        py,
                        PyBoundVerifiedTargetAction {
                            action: raw_action,
                            descriptor: Mutex::new(PyBoundDescriptorState::Unbound),
                        },
                    )?
                    .into_any(),
                    _ => raw_action,
                };
                let child_invalidation = if py_child_invalidation.is_none(py) {
                    None
                } else {
                    let s = py_child_invalidation.extract::<String>(py)?;
                    match s.as_str() {
                        "destructive" => Some(ChildInvalidation::Destructive),
                        "lossy" => Some(ChildInvalidation::Lossy),
                        other => {
                            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                                "Invalid child_invalidation value: {other:?}"
                            )));
                        }
                    }
                };
                Some(TargetReconcileOutput {
                    action,
                    sink: sink.keeper,
                    tracking_record: if non_existence.is(&state) {
                        None
                    } else {
                        Some(PyStoredValue::new(state))
                    },
                    child_invalidation,
                })
            };
            Ok(output)
        })
        .from_py_result()
    }

    fn attachments(&self) -> Result<Vec<(Arc<str>, PyTargetHandler)>> {
        Python::attach(|py| -> PyResult<_> {
            let obj = self.0.bind(py);
            if !obj.hasattr("attachments")? {
                return Ok(vec![]);
            }
            let result = obj.call_method0("attachments")?;
            let dict = result.cast::<pyo3::types::PyDict>()?;
            let mut entries = Vec::with_capacity(dict.len());
            for (key, value) in dict.iter() {
                let att_type: String = key.extract()?;
                entries.push((Arc::from(att_type), PyTargetHandler(value.unbind())));
            }
            Ok(entries)
        })
        .from_py_result()
    }
}

#[pyfunction(name = "_unwrap_target_action")]
pub fn unwrap_target_action(py: Python<'_>, action: Py<PyAny>) -> Py<PyAny> {
    match action.extract::<PyRef<'_, PyBoundVerifiedTargetAction>>(py) {
        Ok(carrier) => carrier.action.clone_ref(py),
        Err(_) => action,
    }
}

#[pyclass(name = "TargetStateProvider")]
pub struct PyTargetStateProvider(TargetStateProvider<PyEngineProfile>);

#[pymethods]
impl PyTargetStateProvider {
    pub fn synor_memo_key(&self) -> String {
        let path = self.0.target_state_path().to_string();
        match self.0.provider_generation() {
            Some(g) => format!("{}[{},{}]", path, g.provider_id, g.provider_schema_version),
            None => path,
        }
    }

    pub fn stable_key_chain<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let chain = self.0.stable_key_chain();
        let py_keys: Vec<Bound<'py, PyAny>> = chain
            .into_iter()
            .map(|k| PyStableKey(k).into_pyobject(py))
            .collect::<Result<_, _>>()?;
        PyTuple::new(py, py_keys)
    }

    pub fn register_attachment_provider(
        &self,
        comp_ctx: &PyComponentProcessorContext,
        att_type: &str,
    ) -> PyResult<PyTargetStateProvider> {
        let provider = self
            .0
            .register_attachment_provider(&comp_ctx.0, att_type)
            .into_py_result()?;
        Ok(PyTargetStateProvider(provider))
    }
}

#[pyfunction]
pub fn declare_target_state<'py>(
    comp_ctx: &'py PyComponentProcessorContext,
    fn_ctx: &'py PyFnCallContext,
    provider: &PyTargetStateProvider,
    key: PyStableKey,
    value: Py<PyAny>,
) -> PyResult<()> {
    synor_core::engine::execution::declare_target_state(
        &comp_ctx.0,
        &fn_ctx.0,
        provider.0.clone(),
        key.0,
        value,
    )
    .into_py_result()?;
    Ok(())
}

#[pyfunction]
pub fn declare_target_state_with_child<'py>(
    comp_ctx: &'py PyComponentProcessorContext,
    fn_ctx: &'py PyFnCallContext,
    provider: &PyTargetStateProvider,
    key: PyStableKey,
    value: Py<PyAny>,
) -> PyResult<PyTargetStateProvider> {
    let output = synor_core::engine::execution::declare_target_state_with_child(
        &comp_ctx.0,
        &fn_ctx.0,
        provider.0.clone(),
        key.0,
        value,
    )
    .into_py_result()?;
    Ok(PyTargetStateProvider(output))
}

static ROOT_TARGET_STATE_PROVIDER_REGISTRY: LazyLock<
    ManuallyDrop<Arc<Mutex<TargetStateProviderRegistry<PyEngineProfile>>>>,
> = LazyLock::new(|| ManuallyDrop::new(Default::default()));

pub fn root_target_states_provider_registry()
-> &'static Arc<Mutex<TargetStateProviderRegistry<PyEngineProfile>>> {
    &**ROOT_TARGET_STATE_PROVIDER_REGISTRY
}

#[pyfunction]
pub fn register_root_target_states_provider(
    name: String,
    handler: Py<PyAny>,
) -> PyResult<PyTargetStateProvider> {
    let provider = root_target_states_provider_registry()
        .lock()
        .unwrap()
        .register_root(name, PyTargetHandler(handler))
        .into_py_result()?;
    Ok(PyTargetStateProvider(provider))
}
