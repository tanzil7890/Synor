use crate::prelude::*;

use crate::{
    engine::{context::ComponentProcessorContext, profile::EngineProfile},
    state::{
        native_effect::{NativeEffectDescriptor, NativeVerificationPolicy},
        stable_path::StableKey,
        target_state_path::{TargetStatePath, TargetStateProviderGeneration},
    },
};

use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
};
use synor_utils::batching::{BatchQueue, Batcher, BatchingOptions, Runner};

// Keep independently reconciling component inputs from being coalesced into
// an arbitrarily large sink call. These are internal packing thresholds, not
// compatibility-breaking limits on one component's indivisible action set.
// Connectors publish hard per-call limits explicitly in `SinkCapabilities`.
const DEFAULT_MAX_SINK_BATCH_ACTIONS: usize = 4_096;
const DEFAULT_MAX_SINK_BATCH_BYTES: usize = 8 * 1024 * 1024;

pub struct ChildTargetDef<Prof: EngineProfile> {
    pub handler: Prof::TargetHdl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkAssurance {
    Legacy,
    Verified(NativeVerificationPolicy),
}

/// Whether a sink contract explicitly supports a behavior. `Unknown` is kept
/// distinct from `Unsupported` so legacy connectors remain compatible without
/// accidentally making a stronger claim.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkCapabilitySupport {
    #[default]
    Unknown,
    Unsupported,
    Supported,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkBatchAtomicity {
    #[default]
    Unknown,
    None,
    PerAction,
    PerApply,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkApplyOrdering {
    #[default]
    Unknown,
    Unordered,
    InputOrder,
}

/// Evidence established before a sink reports successful completion.
///
/// `Acknowledged` means the external write API or transaction has accepted the
/// operation. `QueryVerified` is stronger: the sink has also checked the
/// resulting external state against the requested postcondition.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkCompletionVerification {
    #[default]
    Unknown,
    Unverified,
    Acknowledged,
    QueryVerified,
}

/// Versioned, machine-readable operational contract for a target sink.
///
/// All fields default conservatively, preserving existing connectors while
/// making unknown guarantees explicit. Connectors can opt into stronger claims
/// only when their conformance tests establish them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkCapabilities {
    pub schema_version: u16,
    pub batch_atomicity: SinkBatchAtomicity,
    pub idempotent_replay: SinkCapabilitySupport,
    /// Whether the engine may split one component's action set across several
    /// `apply` calls. This weakens external all-at-once visibility and is safe
    /// only when replaying an already-applied prefix is idempotent.
    #[serde(default)]
    pub segmented_replay_safe: SinkCapabilitySupport,
    pub apply_ordering: SinkApplyOrdering,
    pub cancellation_safe: SinkCapabilitySupport,
    pub completion_verification: SinkCompletionVerification,
    pub max_batch_actions: Option<usize>,
    pub max_batch_bytes: Option<usize>,
}

impl Default for SinkCapabilities {
    fn default() -> Self {
        Self {
            schema_version: 1,
            batch_atomicity: SinkBatchAtomicity::Unknown,
            idempotent_replay: SinkCapabilitySupport::Unknown,
            segmented_replay_safe: SinkCapabilitySupport::Unknown,
            apply_ordering: SinkApplyOrdering::Unknown,
            cancellation_safe: SinkCapabilitySupport::Unknown,
            completion_verification: SinkCompletionVerification::Unknown,
            max_batch_actions: None,
            max_batch_bytes: None,
        }
    }
}

impl SinkCapabilities {
    fn validate(&self) -> Result<()> {
        if self.schema_version != 1 {
            client_bail!(
                "unsupported target sink capability schema_version: {}",
                self.schema_version
            );
        }
        if self.max_batch_actions == Some(0) {
            client_bail!("sink capability max_batch_actions must be positive");
        }
        if self.max_batch_bytes == Some(0) {
            client_bail!("sink capability max_batch_bytes must be positive");
        }
        if self.segmented_replay_safe == SinkCapabilitySupport::Supported
            && self.idempotent_replay != SinkCapabilitySupport::Supported
        {
            client_bail!(
                "sink capability segmented_replay_safe requires idempotent_replay=supported"
            );
        }
        Ok(())
    }

    fn validate_declared_batch_limits(
        &self,
        action_count: usize,
        action_bytes: usize,
    ) -> Result<()> {
        self.validate()?;
        if let Some(max_actions) = self.max_batch_actions
            && action_count > max_actions
        {
            client_bail!(
                "target sink batch has {action_count} actions, exceeding its declared limit of {max_actions}"
            );
        }
        if let Some(max_bytes) = self.max_batch_bytes
            && action_bytes > max_bytes
        {
            client_bail!(
                "target sink batch is approximately {action_bytes} bytes, exceeding its declared limit of {max_bytes}"
            );
        }
        Ok(())
    }

    fn packing_limits(&self) -> (usize, usize) {
        (
            self.max_batch_actions
                .unwrap_or(DEFAULT_MAX_SINK_BATCH_ACTIONS)
                .min(DEFAULT_MAX_SINK_BATCH_ACTIONS),
            self.max_batch_bytes
                .unwrap_or(DEFAULT_MAX_SINK_BATCH_BYTES)
                .min(DEFAULT_MAX_SINK_BATCH_BYTES),
        )
    }
}

/// Point-in-time pressure telemetry for one target sink queue.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkQueueStats {
    pub ongoing_batches: usize,
    pub queued_batches: usize,
    pub queued_inputs: usize,
    pub in_flight_inputs: usize,
    pub in_flight_bytes: usize,
    pub capacity_waiters: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EffectMode {
    #[default]
    Compatibility,
    Strict,
}

#[async_trait]
pub trait TargetActionSink<Prof: EngineProfile>: Send + Sync + 'static {
    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Describe the assurance established when [`Self::apply`] returns.
    ///
    /// `Legacy` means no engine-governed native-effect descriptor/evidence
    /// binding is present. A legacy sink may self-declare operational
    /// completion evidence in [`Self::capabilities`], but that declaration
    /// alone does not satisfy strict native-effect policy. Verified wrappers
    /// opt in here and must return normally only after every action's required
    /// postcondition and evidence write have completed.
    fn assurance(&self) -> SinkAssurance {
        SinkAssurance::Legacy
    }

    /// Describe atomicity, replay, segmentation, ordering, cancellation,
    /// completion verification, and batch limits.
    /// Legacy sinks get a conservative all-unknown contract.
    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities::default()
    }

    /// Extract the privacy-safe descriptor for one governed action.
    ///
    /// The default keeps every existing sink compatible. Verified sinks
    /// override this hook and must reject malformed descriptors without
    /// including the action or host exception in the returned error.
    fn describe_effect(
        &self,
        _action: &Prof::TargetAction,
    ) -> Result<Option<NativeEffectDescriptor>> {
        Ok(None)
    }

    /// Run the logic to apply the action.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    async fn apply(
        &self,
        host_runtime_ctx: &Prof::HostRuntimeCtx,
        host_ctx: Arc<Prof::HostCtx>,
        actions: Vec<Prof::TargetAction>,
    ) -> Result<Option<Vec<Option<ChildTargetDef<Prof>>>>>;
}

/// Cloneable handle to a target action sink and its per-sink batcher.
#[derive(Clone)]
pub struct TargetActionSinkKeeper<Prof: EngineProfile> {
    inner: Arc<TargetActionSinkKeeperInner<Prof>>,
}

struct TargetActionSinkKeeperInner<Prof: EngineProfile> {
    sink: Arc<Prof::TargetActionSink>,
    batcher: Batcher<TargetActionRunner<Prof>>,
}

impl<Prof: EngineProfile> TargetActionSinkKeeper<Prof> {
    pub fn new(sink: Prof::TargetActionSink) -> Self {
        let sink = Arc::new(sink);
        Self {
            inner: Arc::new(TargetActionSinkKeeperInner {
                sink: sink.clone(),
                batcher: Batcher::new(
                    TargetActionRunner { sink },
                    Arc::new(BatchQueue::new()),
                    BatchingOptions::default(),
                ),
            }),
        }
    }

    pub fn assurance(&self) -> SinkAssurance {
        self.inner.sink.assurance()
    }

    pub fn capabilities(&self) -> SinkCapabilities {
        self.inner.sink.capabilities()
    }

    pub fn queue_stats(&self) -> SinkQueueStats {
        let stats = self.inner.batcher.stats();
        SinkQueueStats {
            ongoing_batches: stats.ongoing_batches,
            queued_batches: stats.queued_batches,
            queued_inputs: stats.queued_inputs,
            in_flight_inputs: stats.in_flight_inputs,
            in_flight_bytes: stats.in_flight_bytes,
            capacity_waiters: stats.capacity_waiters,
        }
    }

    pub fn describe_effect(
        &self,
        action: &Prof::TargetAction,
    ) -> Result<Option<NativeEffectDescriptor>> {
        self.inner.sink.describe_effect(action)
    }

    pub async fn apply(
        &self,
        host_runtime_ctx: &Prof::HostRuntimeCtx,
        host_ctx: Arc<Prof::HostCtx>,
        actions: Vec<Prof::TargetAction>,
    ) -> Result<Option<Vec<Option<ChildTargetDef<Prof>>>>> {
        if actions.is_empty() {
            return Ok(None);
        }

        let capabilities = self.capabilities();
        capabilities.validate()?;
        let (max_actions, max_bytes) = capabilities.packing_limits();

        // Explicit connector limits are hard contracts. Internal packing
        // thresholds never reject or split one legacy component action set.
        if capabilities.segmented_replay_safe != SinkCapabilitySupport::Supported {
            let action_bytes = actions
                .iter()
                .map(|action| Prof::target_action_size_bytes(action).max(1))
                .fold(0usize, usize::saturating_add);
            capabilities.validate_declared_batch_limits(actions.len(), action_bytes)?;
            return self
                .inner
                .batcher
                .run(TargetActionRunnerInput {
                    host_runtime_ctx: host_runtime_ctx.clone(),
                    host_ctx,
                    actions,
                })
                .await?;
        }

        // For opted-in segmentation, preflight every individual action against
        // connector-declared hard limits before applying the first segment. Do
        // not retain a second O(total actions) size vector: recomputing the
        // best-effort estimate during packing keeps auxiliary segment state
        // bounded by the selected action/byte thresholds. An action larger
        // than an internal packing threshold is sent alone.
        for action in &actions {
            capabilities
                .validate_declared_batch_limits(1, Prof::target_action_size_bytes(action).max(1))?;
        }

        let mut combined_handlers: Option<Vec<Option<ChildTargetDef<Prof>>>> = None;
        let mut handlers_expected: Option<bool> = None;
        let mut chunk = Vec::with_capacity(max_actions.min(actions.len()));
        let mut chunk_bytes = 0usize;

        for action in actions {
            let action_bytes = Prof::target_action_size_bytes(&action).max(1);
            if !chunk.is_empty()
                && (chunk.len() >= max_actions
                    || chunk_bytes.saturating_add(action_bytes) > max_bytes)
            {
                let handlers = self
                    .inner
                    .batcher
                    .run(TargetActionRunnerInput {
                        host_runtime_ctx: host_runtime_ctx.clone(),
                        host_ctx: host_ctx.clone(),
                        actions: std::mem::take(&mut chunk),
                    })
                    .await??;
                merge_child_handlers(&mut combined_handlers, &mut handlers_expected, handlers)?;
                chunk = Vec::with_capacity(max_actions);
                chunk_bytes = 0;
            }
            chunk_bytes = chunk_bytes.saturating_add(action_bytes);
            chunk.push(action);
        }

        if !chunk.is_empty() {
            let handlers = self
                .inner
                .batcher
                .run(TargetActionRunnerInput {
                    host_runtime_ctx: host_runtime_ctx.clone(),
                    host_ctx,
                    actions: chunk,
                })
                .await??;
            merge_child_handlers(&mut combined_handlers, &mut handlers_expected, handlers)?;
        }

        Ok(combined_handlers)
    }
}

fn merge_child_handlers<Prof: EngineProfile>(
    combined: &mut Option<Vec<Option<ChildTargetDef<Prof>>>>,
    expected: &mut Option<bool>,
    handlers: Option<Vec<Option<ChildTargetDef<Prof>>>>,
) -> Result<()> {
    let has_handlers = handlers.is_some();
    if expected.is_some_and(|value| value != has_handlers) {
        client_bail!("target sink returned child handlers for only some segmented batches");
    }
    expected.get_or_insert(has_handlers);
    if let Some(handlers) = handlers {
        combined.get_or_insert_with(Vec::new).extend(handlers);
    }
    Ok(())
}

impl<Prof: EngineProfile> PartialEq for TargetActionSinkKeeper<Prof> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<Prof: EngineProfile> Eq for TargetActionSinkKeeper<Prof> {}

impl<Prof: EngineProfile> Hash for TargetActionSinkKeeper<Prof> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.inner).hash(state);
    }
}

struct TargetActionRunnerInput<Prof: EngineProfile> {
    host_runtime_ctx: Prof::HostRuntimeCtx,
    host_ctx: Arc<Prof::HostCtx>,
    actions: Vec<Prof::TargetAction>,
}

struct TargetActionRunnerContext<Prof: EngineProfile> {
    host_runtime_ctx: Prof::HostRuntimeCtx,
    host_ctx: Arc<Prof::HostCtx>,
}

struct PreparedTargetActionRunnerInput<Prof: EngineProfile> {
    input_idx: usize,
    actions: Vec<Prof::TargetAction>,
    action_bytes: usize,
}

impl<Prof: EngineProfile> PartialEq for TargetActionRunnerContext<Prof> {
    fn eq(&self, other: &Self) -> bool {
        self.host_runtime_ctx == other.host_runtime_ctx
            && Arc::ptr_eq(&self.host_ctx, &other.host_ctx)
    }
}

impl<Prof: EngineProfile> Eq for TargetActionRunnerContext<Prof> {}

impl<Prof: EngineProfile> Hash for TargetActionRunnerContext<Prof> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.host_runtime_ctx.hash(state);
        Arc::as_ptr(&self.host_ctx).hash(state);
    }
}

struct TargetActionRunner<Prof: EngineProfile> {
    sink: Arc<Prof::TargetActionSink>,
}

impl<Prof: EngineProfile> TargetActionRunner<Prof> {
    async fn apply_chunk(
        &self,
        context: &TargetActionRunnerContext<Prof>,
        capabilities: SinkCapabilities,
        actions: Vec<Prof::TargetAction>,
        action_bytes: usize,
    ) -> Result<Option<Vec<Option<ChildTargetDef<Prof>>>>> {
        capabilities.validate_declared_batch_limits(actions.len(), action_bytes)?;
        let actions_len = actions.len();
        let handlers = self
            .sink
            .apply(&context.host_runtime_ctx, context.host_ctx.clone(), actions)
            .await?;
        if let Some(handlers) = &handlers {
            if handlers.len() != actions_len {
                client_bail!(
                    "expect child providers returned by Sink to be the same length as the actions ({}), got {}",
                    actions_len,
                    handlers.len(),
                );
            }
        }
        Ok(handlers)
    }

    async fn apply_packed_inputs(
        &self,
        context: &TargetActionRunnerContext<Prof>,
        capabilities: SinkCapabilities,
        inputs: Vec<PreparedTargetActionRunnerInput<Prof>>,
    ) -> Vec<(usize, Result<Option<Vec<Option<ChildTargetDef<Prof>>>>>)> {
        let mut actions = Vec::new();
        let mut action_bytes = 0usize;
        let mut input_indexes = Vec::with_capacity(inputs.len());
        let mut action_counts = Vec::with_capacity(inputs.len());
        for input in inputs {
            input_indexes.push(input.input_idx);
            action_counts.push(input.actions.len());
            action_bytes = action_bytes.saturating_add(input.action_bytes);
            actions.extend(input.actions);
        }

        match self
            .apply_chunk(context, capabilities, actions, action_bytes)
            .await
        {
            Ok(None) => input_indexes
                .into_iter()
                .map(|input_idx| (input_idx, Ok(None)))
                .collect(),
            Ok(Some(handlers)) => {
                let mut handlers = handlers.into_iter();
                std::iter::zip(input_indexes, action_counts)
                    .map(|(input_idx, count)| {
                        (input_idx, Ok(Some(handlers.by_ref().take(count).collect())))
                    })
                    .collect()
            }
            Err(err) => {
                let mut replicas = input_indexes
                    .iter()
                    .skip(1)
                    .map(|input_idx| (*input_idx, Err(err.replica())))
                    .collect::<Vec<_>>();
                let mut outputs = Vec::with_capacity(input_indexes.len());
                outputs.push((input_indexes[0], Err(err)));
                outputs.append(&mut replicas);
                outputs
            }
        }
    }
}

#[async_trait]
impl<Prof: EngineProfile> Runner for TargetActionRunner<Prof> {
    type Input = TargetActionRunnerInput<Prof>;
    type Output = Result<Option<Vec<Option<ChildTargetDef<Prof>>>>>;

    fn input_size_bytes(&self, input: &Self::Input) -> usize {
        input
            .actions
            .iter()
            .fold(0usize, |total, action| {
                total.saturating_add(Prof::target_action_size_bytes(action))
            })
            .max(1)
    }

    async fn run(
        &self,
        inputs: Vec<Self::Input>,
    ) -> Result<impl ExactSizeIterator<Item = Self::Output>> {
        let num_inputs = inputs.len();
        if num_inputs == 0 {
            return Ok(Vec::new().into_iter());
        }

        let capabilities = self.sink.capabilities();
        capabilities.validate()?;
        let (max_actions, max_bytes) = capabilities.packing_limits();
        let mut groups = HashMap::<
            TargetActionRunnerContext<Prof>,
            Vec<PreparedTargetActionRunnerInput<Prof>>,
        >::new();
        for (input_idx, input) in inputs.into_iter().enumerate() {
            let action_bytes = input
                .actions
                .iter()
                .map(|action| Prof::target_action_size_bytes(action).max(1))
                .fold(0usize, usize::saturating_add);
            // Every queued input is an indivisible component action set or one
            // segment already chosen by the keeper. Enforce only connector-
            // declared hard limits, and never split one again while batching.
            capabilities.validate_declared_batch_limits(input.actions.len(), action_bytes)?;
            let context = TargetActionRunnerContext {
                host_runtime_ctx: input.host_runtime_ctx,
                host_ctx: input.host_ctx,
            };
            groups
                .entry(context)
                .or_default()
                .push(PreparedTargetActionRunnerInput {
                    input_idx,
                    actions: input.actions,
                    action_bytes,
                });
        }

        let mut outputs: Vec<Option<Result<Option<Vec<Option<ChildTargetDef<Prof>>>>>>> =
            std::iter::repeat_with(|| None).take(num_inputs).collect();
        for (context, inputs) in groups {
            let mut packed_inputs = Vec::new();
            let mut packed_actions = 0usize;
            let mut packed_bytes = 0usize;

            for input in inputs {
                if !packed_inputs.is_empty()
                    && (packed_actions.saturating_add(input.actions.len()) > max_actions
                        || packed_bytes.saturating_add(input.action_bytes) > max_bytes)
                {
                    for (input_idx, output) in self
                        .apply_packed_inputs(
                            &context,
                            capabilities,
                            std::mem::take(&mut packed_inputs),
                        )
                        .await
                    {
                        outputs[input_idx] = Some(output);
                    }
                    packed_actions = 0;
                    packed_bytes = 0;
                }
                packed_actions = packed_actions.saturating_add(input.actions.len());
                packed_bytes = packed_bytes.saturating_add(input.action_bytes);
                packed_inputs.push(input);
            }

            if !packed_inputs.is_empty() {
                for (input_idx, output) in self
                    .apply_packed_inputs(&context, capabilities, packed_inputs)
                    .await
                {
                    outputs[input_idx] = Some(output);
                }
            }
        }

        Ok(outputs
            .into_iter()
            .map(|output| {
                output.unwrap_or_else(|| {
                    Err(Error::internal_msg(
                        "target action runner did not produce an output",
                    ))
                })
            })
            .collect::<Vec<_>>()
            .into_iter())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildInvalidation {
    Destructive,
    Lossy,
}

pub struct TargetReconcileOutput<Prof: EngineProfile> {
    pub action: Prof::TargetAction,
    pub sink: TargetActionSinkKeeper<Prof>,
    pub tracking_record: Option<Prof::TargetStateTrackingRecord>,
    pub child_invalidation: Option<ChildInvalidation>,
}

pub trait TargetHandler<Prof: EngineProfile>: Send + Sync + Sized + 'static {
    /// Reconcile the desired target state against the previously-tracked
    /// records, returning the action to take.
    ///
    /// `desired_target_state` is borrowed (not owned) because the engine
    /// holds it under a short-lived `tokio::sync::MutexGuard` for the
    /// duration of this call — see the lock-scoped call site in
    /// `submit()`'s `pre_commit`. Borrowing here lets the host-specific
    /// implementation decide whether (and how) to clone:
    ///
    /// * Native Rust profile (`Value: Clone`): typically `value.clone()`
    ///   when constructing the `Action`.
    /// * Python profile (`Py<PyAny>: !Clone`): `value.clone_ref(py)`
    ///   under the GIL.
    ///
    /// Avoids forcing every call site to round-trip through an
    /// engine-level `clone_target_state_value` even when the impl
    /// might not need an owned copy.
    fn reconcile(
        &self,
        key: StableKey,
        desired_target_state: Option<&Prof::TargetStateValue>,
        prev_possible_records: &[Prof::TargetStateTrackingRecord],
        prev_may_be_missing: bool,
    ) -> Result<Option<TargetReconcileOutput<Prof>>>;

    /// Return all attachment types this handler supports, keyed by type name.
    /// The engine eagerly registers these as providers so that orphaned
    /// attachments can be cleaned up even when not declared in the current run.
    fn attachments(&self) -> Result<Vec<(Arc<str>, Prof::TargetHdl)>> {
        Ok(vec![])
    }
}

pub(crate) struct TargetStateProviderInner<Prof: EngineProfile> {
    parent_provider: Option<TargetStateProvider<Prof>>,
    stable_key: StableKey,
    target_state_path: TargetStatePath,
    /// Whether this provider was created for a declared target state (child
    /// providers from `register_lazy`), as opposed to provider-only segments
    /// (root providers, attachments). Target-state-backed segments resolve
    /// via the declaring component's owner-index/tracking records, so they
    /// need no persisted segment-name entry.
    backed_by_target_state: bool,
    handler: OnceLock<Prof::TargetHdl>,
    orphaned: OnceLock<()>,
    provider_generation: OnceLock<TargetStateProviderGeneration>,
    attachments: Mutex<HashMap<Arc<str>, TargetStateProvider<Prof>>>,
}

#[derive(Clone)]
pub struct TargetStateProvider<Prof: EngineProfile> {
    pub(crate) inner: Arc<TargetStateProviderInner<Prof>>,
}

impl<Prof: EngineProfile> TargetStateProvider<Prof> {
    pub fn target_state_path(&self) -> &TargetStatePath {
        &self.inner.target_state_path
    }

    pub fn handler(&self) -> Option<&Prof::TargetHdl> {
        self.inner.handler.get()
    }

    /// Fulfill the handler and eagerly register all its attachment providers
    /// into the given registry so that `pre_commit` Phase 2 can clean up
    /// orphaned attachments.
    pub fn fulfill_handler(
        &self,
        handler: Prof::TargetHdl,
        registry: &mut TargetStateProviderRegistry<Prof>,
    ) -> Result<()> {
        self.inner
            .handler
            .set(handler)
            .map_err(|_| internal_error!("Handler is already fulfilled"))?;
        self.register_all_attachment_providers(registry)
    }

    pub fn stable_key(&self) -> &StableKey {
        &self.inner.stable_key
    }

    pub fn stable_key_chain(&self) -> Vec<StableKey> {
        let mut chain = vec![self.inner.stable_key.clone()];
        let mut current = self;
        while let Some(parent) = &current.inner.parent_provider {
            chain.push(parent.inner.stable_key.clone());
            current = parent;
        }
        chain.reverse();
        chain
    }

    /// Collect segment-name entries (lone segment fingerprint → stable key)
    /// for this provider and its ancestors, stopping at the first ancestor
    /// backed by a declared target state: that segment resolves via its
    /// declaring component's owner-index/tracking records, and that
    /// component's own pre-commit covers the segments above it. `out` dedups
    /// across calls; an already-present fingerprint is skipped but the walk
    /// continues, since providers at different depths can share a segment
    /// (e.g. the same attachment type on two tables).
    pub(crate) fn collect_provider_only_segment_names(
        &self,
        out: &mut HashMap<utils::fingerprint::Fingerprint, StableKey>,
    ) {
        let mut current = self;
        loop {
            if current.inner.backed_by_target_state {
                return;
            }
            let fp = *current
                .inner
                .target_state_path
                .as_slice()
                .last()
                .expect("target state path is never empty");
            out.entry(fp)
                .or_insert_with(|| current.inner.stable_key.clone());
            match &current.inner.parent_provider {
                Some(parent) => current = parent,
                None => return,
            }
        }
    }

    pub fn is_orphaned(&self) -> bool {
        self.inner.orphaned.get().is_some()
    }

    pub fn provider_generation(&self) -> Option<&TargetStateProviderGeneration> {
        self.inner.provider_generation.get()
    }

    pub fn set_provider_generation(&self, generation: TargetStateProviderGeneration) -> Result<()> {
        self.inner
            .provider_generation
            .set(generation)
            .map_err(|_| internal_error!("Provider generation already set"))
    }

    fn register_all_attachment_providers(
        &self,
        registry: &mut TargetStateProviderRegistry<Prof>,
    ) -> Result<()> {
        let handler = match self.handler() {
            Some(h) => h,
            None => return Ok(()),
        };
        let att_entries = handler.attachments()?;
        if att_entries.is_empty() {
            return Ok(());
        }

        let mut attachments = self.inner.attachments.lock().unwrap();
        let provider_generation = self.provider_generation().cloned().unwrap_or_default();

        for (att_type, att_handler) in att_entries {
            if attachments.contains_key(&*att_type) {
                continue;
            }
            let symbol_key = StableKey::Symbol(att_type.clone());
            let target_state_path = self.target_state_path().concat(&symbol_key);

            let provider = TargetStateProvider {
                inner: Arc::new(TargetStateProviderInner {
                    parent_provider: Some(self.clone()),
                    stable_key: symbol_key,
                    target_state_path: target_state_path.clone(),
                    backed_by_target_state: false,
                    handler: OnceLock::from(att_handler),
                    orphaned: OnceLock::new(),
                    provider_generation: OnceLock::from(provider_generation.clone()),
                    attachments: Mutex::new(HashMap::new()),
                }),
            };

            registry.add(target_state_path, provider.clone())?;
            attachments.insert(att_type, provider);
        }
        Ok(())
    }

    /// Get or create an attachment provider for the given type.
    /// Called from Python when an attachment is declared (e.g. `declare_vector_index`).
    /// Returns the cached provider if already registered (by eager or prior lazy call).
    pub fn register_attachment_provider(
        &self,
        comp_ctx: &ComponentProcessorContext<Prof>,
        att_type: &str,
    ) -> Result<TargetStateProvider<Prof>> {
        // Fast path: already registered (eagerly or by a previous call).
        let attachments = self.inner.attachments.lock().unwrap();
        if let Some(existing) = attachments.get(att_type) {
            return Ok(existing.clone());
        }
        drop(attachments);

        // Slow path: not yet registered. This can happen if the handler doesn't
        // include this type in attachments(), or during the first run before
        // eager registration has occurred. Build it from the handler.
        let handler = self
            .handler()
            .ok_or_else(|| client_error!("Cannot register attachment on unfulfilled provider"))?;
        let att_entries = handler.attachments()?;
        let att_handler = att_entries
            .into_iter()
            .find(|(k, _)| &**k == att_type)
            .map(|(_, h)| h)
            .ok_or_else(|| {
                client_error!("Handler does not support attachment type: {att_type:?}")
            })?;

        let symbol_key = StableKey::Symbol(att_type.into());
        let target_state_path = self.target_state_path().concat(&symbol_key);

        let provider_generation = self.provider_generation().cloned().unwrap_or_default();

        let provider = TargetStateProvider {
            inner: Arc::new(TargetStateProviderInner {
                parent_provider: Some(self.clone()),
                stable_key: symbol_key,
                target_state_path: target_state_path.clone(),
                backed_by_target_state: false,
                handler: OnceLock::from(att_handler),
                orphaned: OnceLock::new(),
                provider_generation: OnceLock::from(provider_generation),
                attachments: Mutex::new(HashMap::new()),
            }),
        };

        comp_ctx.update_building_state(|building_state| {
            building_state
                .target_states
                .provider_registry
                .add(target_state_path, provider.clone())
        })?;

        let mut attachments = self.inner.attachments.lock().unwrap();
        attachments.insert(att_type.into(), provider.clone());
        Ok(provider)
    }
}

#[derive(Default)]
pub struct TargetStateProviderRegistry<Prof: EngineProfile> {
    pub(crate) providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
    pub(crate) curr_target_state_paths: Vec<TargetStatePath>,
}

impl<Prof: EngineProfile> TargetStateProviderRegistry<Prof> {
    pub fn new(
        providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
    ) -> Self {
        Self {
            providers,
            curr_target_state_paths: Vec::new(),
        }
    }

    pub fn add(
        &mut self,
        target_state_path: TargetStatePath,
        provider: TargetStateProvider<Prof>,
    ) -> Result<()> {
        if self.providers.contains_key(&target_state_path) {
            client_bail!(
                "Target state provider already registered for path: {:?}",
                target_state_path
            );
        }
        self.curr_target_state_paths.push(target_state_path.clone());
        self.providers.insert_mut(target_state_path, provider);
        Ok(())
    }

    pub fn register_root(
        &mut self,
        name: String,
        handler: Prof::TargetHdl,
    ) -> Result<TargetStateProvider<Prof>> {
        let target_state_path =
            TargetStatePath::new(utils::fingerprint::Fingerprint::from(&name)?, None);
        let provider = TargetStateProvider {
            inner: Arc::new(TargetStateProviderInner {
                parent_provider: None,
                stable_key: StableKey::Symbol(name.into()),
                target_state_path: target_state_path.clone(),
                backed_by_target_state: false,
                handler: OnceLock::from(handler),
                orphaned: OnceLock::new(),
                provider_generation: OnceLock::new(),
                attachments: Mutex::new(HashMap::new()),
            }),
        };
        self.add(target_state_path, provider.clone())?;
        provider.register_all_attachment_providers(self)?;
        Ok(provider)
    }

    pub fn register_lazy(
        &mut self,
        parent_provider: &TargetStateProvider<Prof>,
        stable_key: StableKey,
    ) -> Result<TargetStateProvider<Prof>> {
        let target_state_path = parent_provider.target_state_path().concat(&stable_key);
        let provider = TargetStateProvider {
            inner: Arc::new(TargetStateProviderInner {
                parent_provider: Some(parent_provider.clone()),
                stable_key,
                target_state_path: target_state_path.clone(),
                backed_by_target_state: true,
                handler: OnceLock::new(),
                orphaned: OnceLock::new(),
                provider_generation: OnceLock::new(),
                attachments: Mutex::new(HashMap::new()),
            }),
        };
        self.add(target_state_path, provider.clone())?;
        Ok(provider)
    }
}
