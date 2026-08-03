use crate::engine::runtime::get_runtime;
use crate::prelude::*;
use futures::FutureExt;
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Weak;

use crate::engine::context::FnCallContext;
use crate::engine::context::{
    AppContext, ComponentDeleteContext, ComponentProcessingAction, ComponentProcessingMode,
    ComponentProcessorContext, MemoStatesPayload, PreviewActionCollector,
};
use crate::engine::deadline::DeadlineContext;
use crate::engine::execution::{
    cleanup_tombstone, eager_existence_upsert, post_submit_for_build, submit,
    update_component_memo_states, use_or_invalidate_component_memoization,
};
use crate::engine::profile::EngineProfile;
use crate::engine::stats::ProcessingStats;
use crate::engine::target_state::{TargetStateProvider, TargetStateProviderRegistry};
use crate::state::stable_path::{StablePath, StablePathRef};
use crate::state::stable_path_set::StablePathSet;
use crate::state::target_state_path::TargetStatePath;
use synor_utils::error::{SharedError, SharedResult, SharedResultExt};
use synor_utils::fingerprint::Fingerprint;

/// Async on-error callback for background-style component execution.
///
/// Invoked by `run_in_background` / `delete` when the spawned task fails
/// (other than via cancellation, which is filtered). The callback is an
/// observer: returning normally means the failure was reported successfully;
/// returning an error means reporting itself failed. Neither result changes
/// the component's terminal failure or makes `handle.ready()` succeed.
///
/// Cancellation is never delivered to the handler — it's filtered
/// before this is invoked. The "no chain registered" case logs at ERROR.
pub type OnError = Arc<
    dyn Fn(Error) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>
        + Send
        + Sync
        + 'static,
>;

#[derive(Debug, Clone)]
pub struct ComponentProcessorInfo {
    pub name: String,
}

impl ComponentProcessorInfo {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

pub trait ComponentProcessor<Prof: EngineProfile>: Send + Sync + 'static {
    // TODO: Add method to expose function info and arguments, for tracing purpose & no-change detection.

    /// Run the logic to build the component.
    ///
    /// We expect the implementation of this method to spawn the logic to a separate thread or task when needed.
    fn process(
        &self,
        host_runtime_ctx: &Prof::HostRuntimeCtx,
        comp_ctx: &ComponentProcessorContext<Prof>,
    ) -> Result<impl Future<Output = Result<Prof::FunctionData>> + Send + 'static>;

    /// Fingerprint of the memoization key. When matching, re-processing can be skipped.
    /// When None, memoization is not enabled for the component.
    fn memo_key_fingerprint(&self) -> Option<Fingerprint>;

    fn processor_info(&self) -> &ComponentProcessorInfo;

    /// Whether this processor has a memo state handler for post-fingerprint validation.
    fn has_memo_state_handler(&self) -> bool {
        false
    }

    /// Validate or collect memo states after a fingerprint match.
    /// `stored_states`: `Some(payload)` on cache hit, `None` on cache miss (collect initial states).
    /// Returns `(new_states, can_reuse, states_changed)`:
    /// - `can_reuse`: when true, the cached value is valid and can be returned without re-execution.
    /// - `states_changed`: when true, the new states differ from stored states and must be persisted.
    ///   This can be true even when `can_reuse` is true (e.g. mtime changed but content hash unchanged).
    ///
    /// The payload carries both positional (argument-borne) and context-borne memo states.
    /// The core crate treats everything inside as opaque blobs — state functions themselves
    /// live Python-side in the Python profile.
    fn handle_memo_states(
        &self,
        host_runtime_ctx: &Prof::HostRuntimeCtx,
        comp_ctx: &ComponentProcessorContext<Prof>,
        stored_states: Option<MemoStatesPayload<Prof>>,
    ) -> Result<impl Future<Output = Result<(MemoStatesPayload<Prof>, bool, bool)>> + Send + 'static>
    {
        let _ = (host_runtime_ctx, comp_ctx, stored_states);
        Ok(async { Ok((MemoStatesPayload::default(), true, false)) })
    }
}

struct ComponentInner<Prof: EngineProfile> {
    app_ctx: AppContext<Prof>,
    stable_path: StablePath,

    /// Strong reference to the parent component. Keeps the parent (and its
    /// ancestors) alive as long as this child is alive. On Drop, removes
    /// this child's Weak entry from the parent's active_children.
    parent: Option<Component<Prof>>,

    /// Semaphore to ensure `process()` and `commit_effects()` calls cannot happen in parallel.
    build_semaphore: tokio::sync::Semaphore,
    last_memo_fp: Mutex<Option<Fingerprint>>,

    /// Active child components, keyed by their full StablePath.
    /// Uses Weak references — children are kept alive by their spawned tasks
    /// and LiveComponentController references, not by this map. When a child's
    /// last strong reference is dropped, its Drop impl removes the entry here.
    ///
    /// `parking_lot::Mutex` (non-poisoning): the Drop impl below acquires this
    /// lock, and a poisoned `std::sync::Mutex` would cascade panics through
    /// every subsequent Drop on the same parent's children map.
    active_children: parking_lot::Mutex<HashMap<StablePath, Weak<ComponentInner<Prof>>>>,

    /// Shared state for a live component running at this path.
    /// `parking_lot::Mutex` (non-poisoning): symmetric with `active_children`,
    /// since cancel/drain paths can lock this from `Drop` as well.
    live_state:
        parking_lot::Mutex<Option<Arc<crate::engine::live_component::LiveComponentState<Prof>>>>,
}

impl<Prof: EngineProfile> Drop for ComponentInner<Prof> {
    fn drop(&mut self) {
        if let Some(parent) = &self.parent {
            // Identity check: only remove our own entry. A previous
            // `get_child(stable_path)` may have observed our `Weak` failing to
            // upgrade (strong_count hit zero) and inserted a *new* `Weak` at
            // the same key BEFORE this Drop ran. Removing by key alone would
            // erroneously delete the new entry. Compare the stored Weak's
            // pointer against `self` to remove only if the slot still
            // identifies us.
            let mut children = parent.inner.active_children.lock();
            if let Some(weak) = children.get(&self.stable_path)
                && std::ptr::eq(weak.as_ptr(), self as *const ComponentInner<Prof>)
            {
                children.remove(&self.stable_path);
            }
        }
    }
}

#[derive(Clone)]
pub struct Component<Prof: EngineProfile> {
    inner: Arc<ComponentInner<Prof>>,
}

struct ComponentBgChildReadinessState {
    remaining_count: usize,
    build_done: bool,
    is_readiness_set: bool,
    outcome: ComponentRunOutcome,
}

impl ComponentBgChildReadinessState {
    fn maybe_set_readiness(
        &mut self,
        result: Option<Result<ComponentRunOutcome, SharedError>>,
        readiness: &tokio::sync::SetOnce<SharedResult<ComponentRunOutcome>>,
    ) {
        if self.is_readiness_set {
            return;
        }
        if let Some(result) = result {
            if let Ok(outcome) = result {
                self.outcome.merge(outcome);
            } else {
                self.is_readiness_set = true;
                readiness.set(result).expect("readiness set more than once");
                return;
            }
        }
        if self.remaining_count == 0 && self.build_done {
            self.is_readiness_set = true;
            readiness
                .set(Ok(std::mem::take(&mut self.outcome)))
                .expect("readiness set more than once");
        }
    }
}

/// Terminal readiness state for every background component operation.
///
/// Kept explicit rather than encoding state as `Result<()>`: cancellation and
/// coalescing are control-flow outcomes, not component failures, and callers at
/// the live-operation boundary need to preserve that distinction internally.
#[derive(Debug, Default, Clone)]
pub(crate) enum ComponentTerminalOutcome {
    #[default]
    Succeeded,
    Failed(SharedError),
    Cancelled,
    Superseded,
}

impl ComponentTerminalOutcome {
    fn into_ready_result(self) -> Result<()> {
        match self {
            Self::Succeeded | Self::Superseded => Ok(()),
            Self::Failed(error) => Err(Err::<(), _>(error)
                .into_result()
                .expect_err("failed terminal outcome must remain an error")),
            Self::Cancelled => Err(Error::cancelled()),
        }
    }

    fn into_readiness_outcome(self) -> ReadinessOutcome {
        match self {
            Self::Succeeded => ReadinessOutcome::Succeeded,
            Self::Failed(error) => ReadinessOutcome::Failed(
                Err::<(), _>(error)
                    .into_result()
                    .expect_err("failed terminal outcome must remain an error"),
            ),
            Self::Cancelled => ReadinessOutcome::Cancelled,
            Self::Superseded => ReadinessOutcome::Superseded,
        }
    }
}

/// Typed terminal result returned by a component readiness handle.
///
/// [`ComponentExecutionHandle::ready`] retains the compatibility behavior:
/// succeeded and superseded work return `Ok(())`, while failed and cancelled
/// work return an error. Use [`ComponentExecutionHandle::outcome`] when the
/// caller needs to distinguish all four terminal states explicitly.
#[derive(Debug)]
pub enum ReadinessOutcome {
    /// The component and its durable downstream reconciliation completed.
    Succeeded,
    /// The component failed. The original structured error is preserved when
    /// possible; later observers receive a faithful replica.
    Failed(Error),
    /// The operation was cancelled before reaching durable success.
    Cancelled,
    /// A newer operation for the same live-component path displaced this one.
    Superseded,
}

impl ReadinessOutcome {
    /// Convert the typed outcome to the historical success-or-error contract.
    pub fn into_result(self) -> Result<()> {
        match self {
            Self::Succeeded | Self::Superseded => Ok(()),
            Self::Failed(error) => Err(error),
            Self::Cancelled => Err(Error::cancelled()),
        }
    }

    /// Return the failure when this is [`ReadinessOutcome::Failed`].
    pub fn error(&self) -> Option<&Error> {
        match self {
            Self::Failed(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct ComponentRunOutcome {
    terminal: ComponentTerminalOutcome,
    logic_deps: HashSet<Fingerprint>,
    /// A foreground child failure is returned directly to its awaiting caller,
    /// which may deliberately catch it.  It must therefore not become a second
    /// terminal failure in the parent's background-readiness aggregate.  It
    /// still blocks memo finalization so a caught failure is retried on the next
    /// run instead of being cached as a successful parent execution.
    invalidates_memo: bool,
}

impl ComponentRunOutcome {
    pub(crate) fn cancelled() -> Self {
        Self {
            terminal: ComponentTerminalOutcome::Cancelled,
            invalidates_memo: true,
            ..Default::default()
        }
    }

    pub(crate) fn failed(error: SharedError) -> Self {
        Self {
            terminal: ComponentTerminalOutcome::Failed(error),
            invalidates_memo: true,
            ..Default::default()
        }
    }

    pub(crate) fn superseded() -> Self {
        Self {
            terminal: ComponentTerminalOutcome::Superseded,
            ..Default::default()
        }
    }

    fn terminal(&self) -> ComponentTerminalOutcome {
        self.terminal.clone()
    }

    fn has_exception(&self) -> bool {
        self.invalidates_memo
            || matches!(
                self.terminal,
                ComponentTerminalOutcome::Failed(_) | ComponentTerminalOutcome::Cancelled
            )
    }

    /// Convert a foreground child's outcome into dependency evidence for its
    /// parent.  The original terminal result still goes to `use_mount`/`call`;
    /// only the independent parent-readiness channel is made non-terminal.
    fn into_foreground_dependency(mut self) -> Self {
        if matches!(
            self.terminal,
            ComponentTerminalOutcome::Failed(_) | ComponentTerminalOutcome::Cancelled
        ) {
            self.terminal = ComponentTerminalOutcome::Succeeded;
            self.invalidates_memo = true;
        }
        self
    }

    /// Outcome for a component that hit its memo cache: no exception, but it
    /// still reports the stored logic dependency set so a mounting parent's
    /// memo depends on this subtree.
    fn reused(logic_deps: Vec<Fingerprint>) -> Self {
        Self {
            terminal: ComponentTerminalOutcome::Succeeded,
            logic_deps: logic_deps.into_iter().collect(),
            invalidates_memo: false,
        }
    }

    fn merge(&mut self, other: Self) {
        self.invalidates_memo |= other.invalidates_memo;
        // Aggregate precedence: a real failure wins; cancellation wins over
        // successful/no-op members; superseded is a neutral no-op at the
        // enclosing subtree level.
        match (&self.terminal, other.terminal) {
            (ComponentTerminalOutcome::Failed(_), _) => {}
            (_, ComponentTerminalOutcome::Failed(error)) => {
                self.terminal = ComponentTerminalOutcome::Failed(error);
            }
            (ComponentTerminalOutcome::Cancelled, _) => {}
            (_, ComponentTerminalOutcome::Cancelled) => {
                self.terminal = ComponentTerminalOutcome::Cancelled;
            }
            _ => {}
        }
        self.logic_deps.extend(other.logic_deps);
    }
}

struct ComponentBgChildReadinessInner {
    state: Mutex<ComponentBgChildReadinessState>,
    readiness: tokio::sync::SetOnce<SharedResult<ComponentRunOutcome>>,
}

#[derive(Clone)]
pub struct ComponentBgChildReadiness {
    inner: Arc<ComponentBgChildReadinessInner>,
}

pub struct ComponentBgChildReadinessChildGuard {
    readiness: ComponentBgChildReadiness,
    resolved: bool,
    foreground: bool,
}

impl Drop for ComponentBgChildReadinessChildGuard {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        let mut state = self.readiness.state().lock().unwrap();
        state.remaining_count -= 1;
        let outcome = ComponentRunOutcome::cancelled();
        let outcome = if self.foreground {
            outcome.into_foreground_dependency()
        } else {
            outcome
        };
        state.maybe_set_readiness(Some(Ok(outcome)), self.readiness.readiness());
    }
}

impl ComponentBgChildReadinessChildGuard {
    pub(crate) fn resolve(self, outcome: ComponentRunOutcome) {
        self.resolve_result(Ok(outcome));
    }

    pub(crate) fn resolve_terminal(self, terminal: ComponentTerminalOutcome) {
        let outcome = match terminal {
            ComponentTerminalOutcome::Succeeded => ComponentRunOutcome::default(),
            ComponentTerminalOutcome::Failed(error) => ComponentRunOutcome::failed(error),
            ComponentTerminalOutcome::Cancelled => ComponentRunOutcome::cancelled(),
            ComponentTerminalOutcome::Superseded => ComponentRunOutcome::superseded(),
        };
        self.resolve(outcome);
    }

    /// Resolve a foreground child at the parent's dependency boundary.
    /// Foreground errors are owned by the awaiting caller and may be caught;
    /// they invalidate memoization without independently failing the parent.
    fn resolve_foreground(self, outcome: ComponentRunOutcome) {
        self.resolve(outcome.into_foreground_dependency());
    }

    /// Like `resolve`, but propagates a full `SharedResult`: `Ok` merges the
    /// outcome (logic deps), `Err` fails the enclosing readiness. Used by a
    /// `StatsGroup` to forward its members' aggregate readiness to its parent.
    pub(crate) fn resolve_result(mut self, result: SharedResult<ComponentRunOutcome>) {
        {
            let mut state = self.readiness.state().lock().unwrap();
            state.remaining_count -= 1;
            state.maybe_set_readiness(Some(result), self.readiness.readiness());
        }
        self.resolved = true;
    }
}

impl Default for ComponentBgChildReadiness {
    fn default() -> Self {
        Self {
            inner: Arc::new(ComponentBgChildReadinessInner {
                state: Mutex::new(ComponentBgChildReadinessState {
                    remaining_count: 0,
                    is_readiness_set: false,
                    build_done: false,
                    outcome: Default::default(),
                }),
                readiness: tokio::sync::SetOnce::new(),
            }),
        }
    }
}

impl ComponentBgChildReadiness {
    fn state(&self) -> &Mutex<ComponentBgChildReadinessState> {
        &self.inner.state
    }

    fn readiness(&self) -> &tokio::sync::SetOnce<SharedResult<ComponentRunOutcome>> {
        &self.inner.readiness
    }

    pub fn add_child(self) -> ComponentBgChildReadinessChildGuard {
        self.add_child_with_kind(false)
    }

    fn add_foreground_child(self) -> ComponentBgChildReadinessChildGuard {
        self.add_child_with_kind(true)
    }

    fn add_child_with_kind(self, foreground: bool) -> ComponentBgChildReadinessChildGuard {
        self.state().lock().unwrap().remaining_count += 1;
        ComponentBgChildReadinessChildGuard {
            readiness: self,
            resolved: false,
            foreground,
        }
    }

    fn set_build_done(&self) {
        let mut state = self.state().lock().unwrap();
        state.build_done = true;
        state.maybe_set_readiness(None, self.readiness());
    }
}

/// "Is any member still active?" — pops dead `Weak`s off the back until the
/// first live one (or empty). Each entry is removed at most once over the
/// collection's lifetime ⇒ amortized O(1) per inserted member, no full scan.
/// Used by [`StatsGroup`] for live-mode termination (the group's unkeyed,
/// drop-hookless analogue of `active_children`).
fn any_active<T>(members: &mut Vec<Weak<T>>) -> bool {
    while let Some(last) = members.last() {
        if last.strong_count() > 0 {
            return true;
        }
        members.pop();
    }
    false
}

/// A named, separate stats aggregation scope created by `syn.stats_group(...)`.
/// Components mounted within the scope report into `stats` (split out of the
/// enclosing aggregate), register their initial readiness into `readiness`, and
/// are tracked for liveness in `active_members`. Shared (`Arc`) between the
/// substituted context view (which pushes members) and the spawned
/// group-lifecycle task (which awaits readiness and polls liveness).
pub(crate) struct StatsGroup<Prof: EngineProfile> {
    stats: ProcessingStats,
    readiness: ComponentBgChildReadiness,
    active_members: parking_lot::Mutex<Vec<Weak<ComponentInner<Prof>>>>,
}

impl<Prof: EngineProfile> StatsGroup<Prof> {
    pub(crate) fn new(operation_id: u64) -> Self {
        Self {
            stats: ProcessingStats::new_with_operation_id(operation_id),
            readiness: ComponentBgChildReadiness::default(),
            active_members: parking_lot::Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn stats(&self) -> &ProcessingStats {
        &self.stats
    }

    pub(crate) fn readiness(&self) -> &ComponentBgChildReadiness {
        &self.readiness
    }

    /// Register a direct member's `ComponentInner` for liveness tracking.
    pub(crate) fn push_member(&self, child: &Component<Prof>) {
        self.active_members.lock().push(child.downgrade_inner());
    }

    /// True while any member (or anything in its subtree, via the strong
    /// parent-chain) is still alive. Prunes dead entries as it scans.
    pub(crate) fn any_active(&self) -> bool {
        any_active(&mut self.active_members.lock())
    }
}

impl<Prof: EngineProfile> ComponentProcessorContext<Prof> {
    /// Open an unlabelled readiness group for a bulk mount operation.
    ///
    /// Every member still reports into the enclosing stats scope, but member
    /// readiness is reduced into one counter/outcome. This lets host bindings
    /// discard each per-component handle immediately instead of retaining an
    /// O(items) handle vector until `spawn_each().ready()` is called.
    pub fn begin_mount_group(&self) -> (ComponentProcessorContext<Prof>, ComponentExecutionHandle) {
        let readiness = ComponentBgChildReadiness::default();
        let derived = self.with_components_readiness(readiness.clone());
        let parent_guard = self.components_readiness().clone().add_child();
        let join_handle = get_runtime().spawn(async move {
            let outcome = readiness.readiness().wait().await.clone();
            let terminal = match &outcome {
                Ok(outcome) => outcome.terminal(),
                Err(error) => ComponentTerminalOutcome::Failed(error.clone()),
            };
            parent_guard.resolve_result(outcome);
            terminal
        });
        let handle = ComponentExecutionHandle::from_terminal(async move {
            match join_handle.await {
                Ok(terminal) => terminal,
                Err(error) => ComponentTerminalOutcome::Failed(SharedError::new(internal_error!(
                    "bulk mount readiness task panicked: {error}"
                ))),
            }
        });
        (derived, handle)
    }

    /// Close a bulk mount group after its members have been registered.
    pub fn end_mount_group(&self) {
        self.components_readiness().set_build_done();
    }

    /// Open a stats group rooted at this context. Returns a derived context view
    /// (whose mounts report into the group and register liveness with it) and
    /// the group's `ProcessingStats` (for the Python handle / watch).
    ///
    /// Spawns the group-lifecycle task, which mirrors the root's
    /// `notify_ready → wait_until_inactive → notify_terminated` flow but resolves
    /// the parent-readiness guard at READY (not at termination) so a live member
    /// can never deadlock the enclosing component's initial readiness.
    pub fn begin_stats_group(
        &self,
        title: String,
        report_to_stdout: bool,
        refresh_interval_secs: Option<f64>,
    ) -> (ComponentProcessorContext<Prof>, ProcessingStats) {
        let group = Arc::new(StatsGroup::new(self.processing_stats().operation_id()));
        let group_stats = group.stats().clone();

        // The group counts as one pending child of the enclosing readiness, so
        // the parent component (or outer group) waits for it as a unit.
        let parent_guard = self.components_readiness().clone().add_child();

        let cancel_token = self.app_ctx().cancellation_token();
        let live = self.live();
        let lifecycle_group = group.clone();
        get_runtime().spawn(async move {
            // Fires once registration is closed (`set_build_done` via
            // `end_stats_group`) AND every member reached initial readiness.
            let outcome = lifecycle_group.readiness().readiness().wait().await.clone();
            lifecycle_group.stats().notify_ready();
            // Propagate readiness/logic-deps upward AT READY — decoupled from
            // termination, matching the root (app.rs).
            parent_guard.resolve_result(outcome);

            if live && !cancel_token.is_cancelled() {
                // Cancel-aware inactivity poll, scoped to this group's members
                // (the group's analogue of `wait_until_inactive`).
                let mut delay = std::time::Duration::from_millis(1);
                let max_delay = std::time::Duration::from_secs(10);
                while lifecycle_group.any_active() {
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        () = cancel_token.cancelled() => break,
                    }
                    delay = (delay * 2).min(max_delay);
                }
            }
            lifecycle_group.stats().notify_terminated();
        });

        if report_to_stdout {
            crate::engine::progress_display::spawn_group_plain_report(
                group_stats.clone(),
                title,
                live,
                refresh_interval_secs,
            );
        }

        (self.with_stats_group(&group), group_stats)
    }

    /// Close the group opened by `begin_stats_group` for member registration.
    /// Non-blocking — readiness then resolves once the members finish.
    pub fn end_stats_group(&self) {
        self.components_readiness().set_build_done();
    }
}

pub struct ComponentMountRunHandle<Prof: EngineProfile> {
    join_handle: tokio::task::JoinHandle<Result<ComponentBuildOutput<Prof>>>,
    /// The waiting caller's deadline, checked post-wait in `result()`
    /// (caller-attributed: the child's committed success is preserved).
    /// Root runs store NONE here — the root's post-update observation
    /// point is `AppOpHandle::result()`.
    caller_deadline: DeadlineContext,
}

impl<Prof: EngineProfile> ComponentMountRunHandle<Prof> {
    pub async fn result(
        self,
        parent_context: Option<&ComponentProcessorContext<Prof>>,
    ) -> Result<Prof::FunctionData> {
        let output = self.join_handle.await??;
        if let Some(parent_context) = parent_context {
            parent_context.update_building_state(|building_state| {
                for target_state_path in
                    output.built_target_states_providers.curr_target_state_paths
                {
                    let Some(provider) = output
                        .built_target_states_providers
                        .providers
                        .get(&target_state_path)
                    else {
                        error!(
                            "target states provider not found for path {}",
                            target_state_path
                        );
                        continue;
                    };
                    if !provider.is_orphaned() {
                        building_state
                            .target_states
                            .provider_registry
                            .add(target_state_path, provider.clone())?;
                    }
                }
                Ok(())
            })?;
        }
        self.caller_deadline.check()?;
        Ok(output.ret)
    }
}

#[derive(Clone)]
pub struct ComponentExecutionHandle {
    fut: futures::future::Shared<
        Pin<Box<dyn Future<Output = ComponentTerminalOutcome> + Send + Sync>>,
    >,
}

impl ComponentExecutionHandle {
    pub fn new(fut: impl Future<Output = SharedResult<()>> + Send + Sync + 'static) -> Self {
        Self::from_terminal(async move {
            match fut.await {
                Ok(()) => ComponentTerminalOutcome::Succeeded,
                Err(error) => ComponentTerminalOutcome::Failed(error),
            }
        })
    }

    pub(crate) fn from_terminal(
        fut: impl Future<Output = ComponentTerminalOutcome> + Send + Sync + 'static,
    ) -> Self {
        let fut: Pin<Box<dyn Future<Output = ComponentTerminalOutcome> + Send + Sync>> =
            Box::pin(fut);
        Self { fut: fut.shared() }
    }

    pub(crate) async fn terminal(&self) -> ComponentTerminalOutcome {
        self.fut.clone().await
    }

    /// Wait for the component and return its explicit terminal state.
    ///
    /// Handles are repeatable: `outcome()` and `ready()` may be called in any
    /// order, including by concurrent observers.
    pub async fn outcome(&self) -> ReadinessOutcome {
        self.terminal().await.into_readiness_outcome()
    }

    /// Wait for readiness using the compatibility success-or-error contract.
    pub async fn ready(&self) -> Result<()> {
        self.terminal().await.into_ready_result()
    }
}

struct ComponentBuildOutput<Prof: EngineProfile> {
    ret: Prof::FunctionData,
    built_target_states_providers: TargetStateProviderRegistry<Prof>,
}

impl<Prof: EngineProfile> Component<Prof> {
    pub(crate) fn new(
        app_ctx: AppContext<Prof>,
        stable_path: StablePath,
        parent: Option<Component<Prof>>,
    ) -> Self {
        Self {
            inner: Arc::new(ComponentInner {
                app_ctx,
                stable_path,
                parent,
                build_semaphore: tokio::sync::Semaphore::const_new(1),
                last_memo_fp: Mutex::new(None),
                active_children: parking_lot::Mutex::new(HashMap::new()),
                live_state: parking_lot::Mutex::new(None),
            }),
        }
    }

    pub fn mount_child(&self, fn_ctx: &FnCallContext, stable_path: StablePath) -> Result<Self> {
        fn_ctx.update(|inner| inner.has_child_components = true);
        Ok(self.get_child(stable_path))
    }

    /// Mount and run a child in the foreground (use_mount path).
    /// Inherits live from the parent context.
    ///
    /// `deadline` is the deadline for the CHILD, taken from the caller's
    /// current scope, and cannot be read from `parent_ctx.deadline`: that is
    /// the caller's own base, frozen at the caller's mount, while narrowing
    /// (`with syn.timeout(...)`) lives in the SDK's per-task carrier —
    /// concurrent tasks within one component can hold different narrowed
    /// scopes at the same moment, so the current value must travel with each
    /// call. It is threaded through the child's execution checkpoints and
    /// captured by the returned handle for the post-wait check; it is never
    /// stored on the ctx (the deadline only ever travels, never rests).
    pub async fn use_mount(
        self,
        parent_ctx: &ComponentProcessorContext<Prof>,
        processor: Prof::ComponentProc,
        deadline: DeadlineContext,
    ) -> Result<ComponentMountRunHandle<Prof>> {
        let child_ctx = self.new_processor_context_for_build(
            Some(parent_ctx),
            parent_ctx.processing_stats().clone(),
            parent_ctx.full_reprocess(),
            parent_ctx.live(), // use_mount inherits live from parent
            parent_ctx.preview_collector().cloned(),
            parent_ctx.host_ctx().clone(),
            parent_ctx.effect_mode(),
        )?;
        self.run(processor, child_ctx, deadline, deadline).await
    }

    /// Mount and run a child in the background (mount path).
    /// Inherits live from the parent context.
    pub async fn mount(
        self,
        parent_ctx: &ComponentProcessorContext<Prof>,
        processor: Prof::ComponentProc,
        on_error: Option<OnError>,
        pre_execute_check: Option<Box<dyn FnOnce() -> bool + Send>>,
    ) -> Result<ComponentExecutionHandle> {
        let child_ctx = self.new_processor_context_for_build(
            Some(parent_ctx),
            parent_ctx.processing_stats().clone(),
            parent_ctx.full_reprocess(),
            parent_ctx.live(), // mount inherits live from parent
            parent_ctx.preview_collector().cloned(),
            parent_ctx.host_ctx().clone(),
            parent_ctx.effect_mode(),
        )?;
        self.run_in_background(processor, child_ctx, on_error, pre_execute_check)
            .await
    }

    pub fn get_child(&self, stable_path: StablePath) -> Self {
        let mut children = self.inner.active_children.lock();
        if let Some(weak) = children.get(&stable_path) {
            if let Some(inner) = weak.upgrade() {
                return Self { inner };
            }
        }
        let child = Self::new(
            self.app_ctx().clone(),
            stable_path.clone(),
            Some(self.clone()),
        );
        children.insert(stable_path, Arc::downgrade(&child.inner));
        child
    }

    pub fn app_ctx(&self) -> &AppContext<Prof> {
        &self.inner.app_ctx
    }

    pub fn stable_path(&self) -> &StablePath {
        &self.inner.stable_path
    }

    /// A `Weak` to this component's inner, for liveness tracking by a
    /// [`StatsGroup`] (alive iff this component or any descendant is alive).
    fn downgrade_inner(&self) -> Weak<ComponentInner<Prof>> {
        Arc::downgrade(&self.inner)
    }

    pub fn set_live_state(
        &self,
        state: Arc<crate::engine::live_component::LiveComponentState<Prof>>,
    ) {
        *self.inner.live_state.lock() = Some(state);
    }

    pub fn live_state(
        &self,
    ) -> Option<Arc<crate::engine::live_component::LiveComponentState<Prof>>> {
        self.inner.live_state.lock().clone()
    }

    pub fn clear_live_state_if(
        &self,
        expected: &Arc<crate::engine::live_component::LiveComponentState<Prof>>,
    ) {
        let mut slot = self.inner.live_state.lock();
        if slot
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, expected))
        {
            slot.take();
        }
    }

    /// Returns true if this component has no active children (all Weak refs are dead).
    pub fn has_active_children(&self) -> bool {
        let children = self.inner.active_children.lock();
        children.values().any(|w| w.strong_count() > 0)
    }

    /// Wait until all descendants are inactive (active_children is empty).
    /// Uses exponential backoff polling: 1ms → 2ms → 4ms → ... → 10s cap.
    pub async fn wait_until_inactive(&self) {
        let mut delay = std::time::Duration::from_millis(1);
        let max_delay = std::time::Duration::from_secs(10);
        while self.has_active_children() {
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(max_delay);
        }
    }

    pub fn parent(&self) -> Option<&Component<Prof>> {
        self.inner.parent.as_ref()
    }

    pub(crate) fn relative_path(&self) -> Result<StablePathRef<'_>> {
        if let Some(parent) = self.parent() {
            self.stable_path()
                .as_ref()
                .strip_parent(parent.stable_path().as_ref())
        } else {
            Ok(self.stable_path().as_ref())
        }
    }

    /// `deadline` governs this component's own execution checkpoints;
    /// `caller_deadline` is stored in the returned handle for the post-wait
    /// check. They coincide for use_mount; the root passes NONE as
    /// `caller_deadline` since AppOpHandle owns the root's post-result check.
    pub(crate) async fn run(
        self,
        processor: Prof::ComponentProc,
        context: ComponentProcessorContext<Prof>,
        deadline: DeadlineContext,
        caller_deadline: DeadlineContext,
    ) -> Result<ComponentMountRunHandle<Prof>> {
        // Release parent's inflight permit (deadlock prevention).
        // On a component's first child mount, the parent gives up its slot
        // so children can make progress.
        if let Some(parent_ctx) = context.parent_context() {
            parent_ctx.release_inflight_permit();
        }

        // Acquire inflight permit (waits if quota exhausted).
        if let Some(sem) = self.app_ctx().inflight_semaphore() {
            let permit = sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| internal_error!("Inflight semaphore closed"))?;
            context.set_inflight_permit(permit);
        }

        let relative_path = self.relative_path()?;
        let child_readiness_guard = context
            .parent_context()
            .map(|c| c.components_readiness().clone().add_foreground_child());
        let span = info_span!("component.run", component_path = %relative_path);
        let cancel_token = self.app_ctx().cancellation_token();
        let join_handle = get_runtime().spawn(
            async move {
                // Race the work against app-level cancellation. On cancel, the
                // work future is dropped, which cascades drop into from_py_future
                // → CancelOnDropPy and cancels the underlying Python task.
                let result = tokio::select! {
                    r = self.execute_once(&context, Some(&processor), deadline) => r,
                    _ = cancel_token.cancelled() => Err(Error::cancelled()),
                };
                let (outcome, output) = match result {
                    Ok((outcome, output)) => {
                        let output = outcome.terminal().into_ready_result().map(|()| output);
                        (outcome, output)
                    }
                    Err(err) => {
                        if err.is_cancelled() {
                            (ComponentRunOutcome::cancelled(), Err(err))
                        } else {
                            let failure = SharedError::new(err);
                            let outcome = ComponentRunOutcome::failed(failure.clone());
                            let output = Err(Err::<(), _>(failure)
                                .into_result()
                                .expect_err("component failure must remain an error"));
                            (outcome, output)
                        }
                    }
                };
                context.release_inflight_permit();
                drop(processor);
                drop(context);
                drop(self);
                child_readiness_guard.map(|guard| guard.resolve_foreground(outcome));
                output?
                    .ok_or_else(|| internal_error!("component deletion can only run in background"))
            }
            .instrument(span),
        );
        Ok(ComponentMountRunHandle {
            join_handle,
            caller_deadline,
        })
    }

    pub(crate) async fn run_in_background(
        self,
        processor: Prof::ComponentProc,
        context: ComponentProcessorContext<Prof>,
        on_error: Option<OnError>,
        pre_execute_check: Option<Box<dyn FnOnce() -> bool + Send>>,
    ) -> Result<ComponentExecutionHandle> {
        // TODO: Skip building and reuse cached result if the component is already built and up to date.

        // Release parent's inflight permit (deadlock prevention).
        if let Some(parent_ctx) = context.parent_context() {
            parent_ctx.release_inflight_permit();
        }

        // Acquire inflight permit (waits if quota exhausted).
        if let Some(sem) = self.app_ctx().inflight_semaphore() {
            let permit = sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| internal_error!("Inflight semaphore closed"))?;
            context.set_inflight_permit(permit);
        }

        let child_readiness_guard = context
            .parent_context()
            .map(|c| c.components_readiness().clone().add_child());
        let cancel_token = self.app_ctx().cancellation_token();
        let join_handle = get_runtime().spawn(async move {
            // Check if this task has been superseded before executing.
            if let Some(check) = pre_execute_check {
                if !check() {
                    // Superseded — skip execution and preserve that distinct
                    // internal terminal state (public ready remains a no-op).
                    context.release_inflight_permit();
                    drop(processor);
                    drop(context);
                    drop(self);
                    if let Some(guard) = child_readiness_guard {
                        guard.resolve(ComponentRunOutcome::superseded());
                    }
                    return ComponentTerminalOutcome::Superseded;
                }
            }
            // Race the work against app-level cancellation. On cancel, the
            // work future is dropped, which cascades drop into from_py_future
            // → CancelOnDropPy and cancels the underlying Python task.
            let result = tokio::select! {
                // Background components are deadline-isolated by design.
                r = self.execute_once(&context, Some(&processor), DeadlineContext::NONE) => r,
                _ = cancel_token.cancelled() => Err(Error::cancelled()),
            };
            // Background-style error handling:
            // - Cancellation is filtered (no handler call) — Ctrl+C /
            //   shutdown / re-mount has its own operation-level signal.
            // - A direct failure is reported once through `on_error`, but the
            //   observer cannot rewrite the terminal result. The same shared
            //   failure reaches both `handle.ready()` and the parent's
            //   aggregate readiness.
            // - A descendant failure is already reported at its origin. It is
            //   propagated without invoking this component's handler again.
            let (outcome, task_result) = match result {
                Ok((outcome, _)) => {
                    let task_result = outcome.terminal();
                    (outcome, task_result)
                }
                Err(err) => {
                    if cancel_token.is_cancelled() || err.is_cancelled() {
                        trace!("component build cancelled");
                        (
                            ComponentRunOutcome::cancelled(),
                            ComponentTerminalOutcome::Cancelled,
                        )
                    } else {
                        let already_reported = err.is_reported();
                        let failure = SharedError::new(err.reported());
                        if !already_reported {
                            if let Some(handler) = &on_error {
                                let report_error = Err::<(), _>(failure.clone())
                                    .into_result()
                                    .expect_err("component failure must remain an error");
                                if let Err(handler_error) = handler(report_error).await {
                                    error!(
                                        "component exception handler failed while reporting a terminal failure:\n{handler_error:?}"
                                    );
                                }
                            } else {
                                error!("component build failed:\n{failure:?}");
                            }
                        }
                        (
                            ComponentRunOutcome::failed(failure.clone()),
                            ComponentTerminalOutcome::Failed(failure),
                        )
                    }
                }
            };
            context.release_inflight_permit();
            drop(processor);
            drop(context);
            drop(self);
            if let Some(guard) = child_readiness_guard {
                guard.resolve(outcome);
            }
            task_result
        });
        Ok(ComponentExecutionHandle::from_terminal(async move {
            match join_handle.await {
                Ok(terminal) => terminal,
                Err(error) => ComponentTerminalOutcome::Failed(SharedError::new(internal_error!(
                    "task panicked: {error}"
                ))),
            }
        }))
    }

    pub fn delete(
        self,
        context: ComponentProcessorContext<Prof>,
        pre_execute_check: Option<Box<dyn FnOnce() -> bool + Send>>,
    ) -> Result<ComponentExecutionHandle> {
        let child_readiness_guard = context
            .parent_context()
            .map(|c| c.components_readiness().clone().add_child());
        // Pull reporting policy out of the delete context before moving it
        // into the spawned task.
        let on_error = context.delete_action_on_error();
        let defer_failure_reporting = context.defer_delete_failure_reporting();
        let join_handle: tokio::task::JoinHandle<ComponentTerminalOutcome> =
            get_runtime().spawn(async move {
                if let Some(check) = pre_execute_check {
                    if !check() {
                        drop(context);
                        drop(self);
                        if let Some(guard) = child_readiness_guard {
                            guard.resolve(ComponentRunOutcome::superseded());
                        }
                        return ComponentTerminalOutcome::Superseded;
                    }
                }
                trace!("deleting component at {}", self.stable_path());
                // Delete/GC runs must never be deadline-bounded.
                let result = self
                    .execute_once(&context, None, DeadlineContext::NONE)
                    .await;
                // Same error model as `run_in_background`: cancellation is
                // filtered; handlers observe but cannot swallow terminal
                // failures; descendant failures propagate without duplicate
                // handler calls.
                let (outcome, task_result) = match result {
                    Ok((outcome, _)) => {
                        let task_result = outcome.terminal();
                        (outcome, task_result)
                    }
                    Err(err) => {
                        if err.is_cancelled() {
                            trace!("component delete cancelled");
                            (
                                ComponentRunOutcome::cancelled(),
                                ComponentTerminalOutcome::Cancelled,
                            )
                        } else {
                            let already_reported = err.is_reported();
                            let failure = if defer_failure_reporting {
                                SharedError::new(err)
                            } else {
                                SharedError::new(err.reported())
                            };
                            if !defer_failure_reporting && !already_reported {
                                if let Some(handler) = &on_error {
                                    let report_error = Err::<(), _>(failure.clone())
                                        .into_result()
                                        .expect_err("component failure must remain an error");
                                    if let Err(handler_error) = handler(report_error).await {
                                        error!(
                                            "component exception handler failed while reporting a terminal failure:\n{handler_error:?}"
                                        );
                                    }
                                } else {
                                    error!("component delete failed:\n{failure:?}");
                                }
                            }
                            (
                                ComponentRunOutcome::failed(failure.clone()),
                                ComponentTerminalOutcome::Failed(failure),
                            )
                        }
                    }
                };
                // Drop profile-specific objects BEFORE resolving child readiness.
                // See run_in_background for the rationale (PyGILState finalization fix).
                drop(context);
                drop(self);
                if let Some(guard) = child_readiness_guard {
                    guard.resolve(outcome);
                }
                task_result
            });
        Ok(ComponentExecutionHandle::from_terminal(async move {
            match join_handle.await {
                Ok(terminal) => terminal,
                Err(error) => ComponentTerminalOutcome::Failed(SharedError::new(internal_error!(
                    "task panicked: {error}"
                ))),
            }
        }))
    }

    async fn execute_once(
        &self,
        processor_context: &ComponentProcessorContext<Prof>,
        processor: Option<&Prof::ComponentProc>,
        deadline: DeadlineContext,
    ) -> Result<(ComponentRunOutcome, Option<ComponentBuildOutput<Prof>>)> {
        let mut reported_processor_name: Option<Cow<'_, str>> = None;
        let mut memo_fp_to_store: Option<Fingerprint> = None;
        // Memo states collected from state validation (on cache hit with invalid states)
        // or to be collected after execution (on cache miss).
        let mut memo_states_for_store: Option<MemoStatesPayload<Prof>> = None;
        let processing_stats = processor_context.processing_stats();

        if let Some(processor) = processor {
            let processor_name = processor.processor_info().name.as_str();
            memo_fp_to_store = processor.memo_key_fingerprint();
            deadline.check()?;

            // Fast-path: component memoization check does not require acquiring the build permit.
            // If it hits, we can immediately return without processing/submitting/waiting.

            match use_or_invalidate_component_memoization(processor_context, memo_fp_to_store).await
            {
                Ok(Some((ret, memo_states, stored_logic_deps))) => {
                    // If processor has state handler and there are stored states, validate them.
                    if processor.has_memo_state_handler() && !memo_states.is_empty() {
                        let fut = processor.handle_memo_states(
                            processor_context.app_ctx().host_callback_ctx(),
                            processor_context,
                            Some(memo_states),
                        )?;
                        let (new_states, can_reuse, states_changed) = fut.await?;
                        if can_reuse {
                            // Memo is reusable — update stored states if they changed
                            if states_changed {
                                update_component_memo_states(processor_context, &new_states)
                                    .await?;
                            }
                            processing_stats.update(processor_name.as_ref(), |stats| {
                                stats.num_execution_starts += 1;
                                stats.num_unchanged += 1;
                            });
                            // Report the stored dependency set upward even on a
                            // memo hit, so a mounting parent's memo depends on
                            // this whole subtree (see `merge_logic_deps` below).
                            return Ok((
                                ComponentRunOutcome::reused(stored_logic_deps),
                                Some(ComponentBuildOutput {
                                    ret,
                                    built_target_states_providers: Default::default(),
                                }),
                            ));
                        }
                        // Not reusable — fall through to re-execution
                        memo_states_for_store = Some(new_states);
                    } else {
                        // No state handler or no states — use cached result directly
                        processing_stats.update(processor_name.as_ref(), |stats| {
                            stats.num_execution_starts += 1;
                            stats.num_unchanged += 1;
                        });
                        return Ok((
                            ComponentRunOutcome::reused(stored_logic_deps),
                            Some(ComponentBuildOutput {
                                ret,
                                built_target_states_providers: Default::default(),
                            }),
                        ));
                    }
                }
                Err(err) => {
                    error!("component memoization restore failed: {err:?}");
                }
                Ok(None) => {}
            }

            processor_context
                .processing_stats()
                .update(processor_name.as_ref(), |stats| {
                    stats.num_execution_starts += 1;
                });
            reported_processor_name = Some(Cow::Borrowed(processor.processor_info().name.as_str()));
        }

        let result = {
            let reported_processor_name = &mut reported_processor_name;
            async move {
                // Acquire the semaphore to ensure `process()` and `submit()` cannot overlap
                // with another execution of the same component.
                let (ret, submit_output, mut children_outcome) = {
                    let _permit = self.inner.build_semaphore.acquire().await?;
                    processor_context.assert_live_generation_current()?;

                    // Build mode only: write the component's own existence bit
                    // (and ancestor chain) into the parent in its own txn,
                    // before the user processor runs. Maintains the invariant
                    // that existence ⊇ tracked state and eliminates the
                    // dual-writer conflict with the parent's commit-time
                    // existence reconciliation. See `internal_states.md` §3.1.
                    if processor_context.mode() == ComponentProcessingMode::Build
                        && !processor_context.preview()
                    {
                        eager_existence_upsert(processor_context).await?;
                    }

                    // Eagerly load all function-memo and user-state entries for
                    // this component into the per-build cache (one read txn), so
                    // every subsequent fn-call probe and `use_state` serves from
                    // memory. Skipped under `full_reprocess` and in delete mode
                    // (no `ComponentBuildingState`); see the cache flush logic
                    // for how those cases are handled at commit time.
                    processor_context.prefetch_states().await?;

                    if memo_fp_to_store.is_some() {
                        *self.inner.last_memo_fp.lock().unwrap() = memo_fp_to_store;
                        // TODO: when matching, it means there're ongoing processing for the same memoization key pending on children.
                        // We can piggyback on the same processing to avoid duplicating the work.
                    }

                    // The earlier deadline check guards memo lookup. A component can still
                    // spend time waiting for the build semaphore, existence upsert, or state
                    // prefetch before the user body starts, so check again at the actual
                    // processor-entry boundary.
                    deadline.check()?;

                    let ret: Result<Option<Prof::FunctionData>> = match &processor {
                        Some(processor) => processor
                            .process(
                                processor_context.app_ctx().host_callback_ctx(),
                                &processor_context,
                            )?
                            .await
                            .map(Some),
                        None => Ok(None),
                    };

                    // Wait until children components ready before submitting this
                    // component's target states and child-existence reconciliation.
                    let components_readiness = processor_context.components_readiness();
                    components_readiness.set_build_done();
                    let mut children_outcome = components_readiness
                        .readiness()
                        .wait()
                        .await
                        .clone()
                        .into_result()?;

                    // Merge children's logic deps into this component's context. The
                    // full set (own fp ∪ all descendants) is taken once after
                    // memo-state collection below and used for both this component's
                    // own memo and the outcome reported to its parent.
                    processor_context
                        .merge_logic_deps(std::mem::take(&mut children_outcome.logic_deps));

                    let ret = ret?;
                    deadline.check()?;
                    let submit_output = submit(processor_context, processor, |name| {
                        if reported_processor_name.is_none() {
                            processing_stats.update(&name, |stats| {
                                stats.num_execution_starts += 1;
                            });
                            *reported_processor_name = Some(Cow::Owned(name.to_string()));
                        }
                    })
                    .await?;
                    Ok::<_, Error>((ret, submit_output, children_outcome))
                }?;
                let build_output = match ret {
                    Some(ret) => {
                        if !children_outcome.has_exception() {
                            // Collect initial memo states on cache miss if processor has a state handler.
                            let memo_states: MemoStatesPayload<Prof> = if let Some(processor) =
                                processor
                                && processor.has_memo_state_handler()
                            {
                                if let Some(states) = memo_states_for_store.take() {
                                    // From invalid cache hit path
                                    states
                                } else {
                                    // Cache miss — collect initial states
                                    let fut = processor.handle_memo_states(
                                        processor_context.app_ctx().host_callback_ctx(),
                                        processor_context,
                                        None,
                                    )?;
                                    let (initial_states, _, _) = fut.await?;
                                    initial_states
                                }
                            } else {
                                MemoStatesPayload::default()
                            };

                            let comp_memo = if let Some(fp) = memo_fp_to_store
                                && let last_memo_fp = processor_context
                                    .component()
                                    .inner
                                    .last_memo_fp
                                    .lock()
                                    .unwrap()
                                && *last_memo_fp == memo_fp_to_store
                            {
                                Some((fp, &ret, &memo_states))
                            } else {
                                None
                            };
                            // Take the full dependency set once (O(1) move). It
                            // must run after the memo-state collection above, which
                            // reads the set via `collect_context_initial_states`.
                            // Serves both this component's own memo (sorted inside
                            // `post_submit_for_build`, only when memoizing) and the
                            // outcome reported to the parent across the mount
                            // boundary — so the parent's memo depends on this whole
                            // subtree's logic.
                            let logic_deps = processor_context.take_logic_deps();
                            post_submit_for_build(processor_context, comp_memo, &logic_deps)
                                .await?;
                            children_outcome.logic_deps = logic_deps;
                        }
                        Some(ComponentBuildOutput {
                            ret,
                            built_target_states_providers: submit_output
                                .built_target_states_providers
                                .ok_or_else(|| {
                                    internal_error!("expect built target states providers")
                                })?,
                        })
                    }
                    None => {
                        // Delete path. When any descendant delete failed,
                        // skip `cleanup_tombstone` (symmetric with the
                        // build branch skipping `post_submit_for_build`)
                        // — that preserves the tombstone for the next
                        // reconcile to retry.
                        //
                        // The typed child outcome propagates to the awaiting
                        // caller; this branch only decides whether cleanup
                        // metadata is safe to remove.
                        if !children_outcome.has_exception() {
                            cleanup_tombstone(&processor_context).await?;
                        }
                        None
                    }
                };
                Ok::<_, Error>((
                    children_outcome,
                    build_output,
                    submit_output.touched_previous_states,
                ))
            }
            .await
        };

        let final_processor_name = reported_processor_name
            .as_ref()
            .map(|s| s.as_ref())
            .unwrap_or(db_schema::UNKNOWN_PROCESSOR_NAME);
        match result {
            Ok((children_outcome, build_output, touched_previous_states)) => {
                processing_stats.update(final_processor_name, |stats| {
                    if reported_processor_name.is_none() {
                        stats.num_execution_starts += 1;
                    }
                    match processor_context.mode() {
                        ComponentProcessingMode::Build => {
                            if touched_previous_states {
                                stats.num_reprocesses += 1;
                            } else {
                                stats.num_adds += 1;
                            }
                        }
                        ComponentProcessingMode::Delete => {
                            stats.num_deletes += 1;
                        }
                    }
                });
                Ok((children_outcome, build_output))
            }
            Err(err) => {
                processing_stats.update(final_processor_name, |stats| {
                    if reported_processor_name.is_none() {
                        stats.num_execution_starts += 1;
                    }
                    stats.num_errors += 1;
                });
                Err(err)
            }
        }
    }

    pub fn new_processor_context_for_build(
        &self,
        parent_ctx: Option<&ComponentProcessorContext<Prof>>,
        processing_stats: ProcessingStats,
        full_reprocess: bool,
        live: bool,
        preview_collector: Option<PreviewActionCollector<Prof>>,
        host_ctx: Arc<Prof::HostCtx>,
        effect_mode: crate::engine::target_state::EffectMode,
    ) -> Result<ComponentProcessorContext<Prof>> {
        let providers = if let Some(parent_ctx) = parent_ctx {
            let sub_path = self
                .stable_path()
                .as_ref()
                .strip_parent(parent_ctx.stable_path().as_ref())?;
            parent_ctx.update_building_state(|building_state| {
                building_state
                    .child_path_set
                    .add_child(sub_path, StablePathSet::Component)?;
                Ok(building_state
                    .target_states
                    .provider_registry
                    .providers
                    .clone())
            })?
        } else {
            self.app_ctx()
                .env()
                .target_states_providers()
                .lock()
                .unwrap()
                .providers
                .clone()
        };
        Ok(ComponentProcessorContext::new(
            self.clone(),
            parent_ctx.cloned(),
            processing_stats,
            host_ctx,
            ComponentProcessingAction::new_build(
                providers,
                full_reprocess,
                live,
                preview_collector,
                effect_mode,
            ),
        ))
    }

    pub fn new_processor_context_for_delete(
        &self,
        providers: rpds::HashTrieMapSync<TargetStatePath, TargetStateProvider<Prof>>,
        parent_ctx: Option<&ComponentProcessorContext<Prof>>,
        processing_stats: ProcessingStats,
        host_ctx: Arc<Prof::HostCtx>,
        on_error: Option<OnError>,
        defer_failure_reporting: bool,
        effect_mode: crate::engine::target_state::EffectMode,
        provider_missing_is_error: bool,
        tombstone_generation: Option<u64>,
    ) -> ComponentProcessorContext<Prof> {
        ComponentProcessorContext::new(
            self.clone(),
            parent_ctx.cloned(),
            processing_stats,
            host_ctx,
            ComponentProcessingAction::Delete(ComponentDeleteContext {
                providers,
                on_error,
                defer_failure_reporting,
                effect_mode,
                provider_missing_is_error,
                tombstone_generation,
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ComponentBgChildReadiness, ComponentExecutionHandle, ComponentProcessor,
        ComponentProcessorInfo, ComponentRunOutcome, ComponentTerminalOutcome, ReadinessOutcome,
        any_active,
    };
    use crate::engine::app::{App, AppUpdateOptions};
    use crate::engine::context::{ComponentProcessorContext, MemoStatesPayload};
    use crate::engine::deadline::{
        DeadlineContext, testing_advance_deadline_clock, testing_deadline_clock_lock,
        testing_disable_deadline_clock, testing_reset_deadline_clock,
    };
    use crate::engine::environment::Environment;
    use crate::engine::profile::{EngineProfile, Persist};
    use crate::engine::target_state::{
        ChildTargetDef, TargetActionSink, TargetHandler, TargetReconcileOutput,
        TargetStateProviderRegistry,
    };
    use crate::state::stable_path::StableKey;
    use crate::state_store::StorageSettings;
    use async_trait::async_trait;
    use std::hash::{Hash, Hasher};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, Weak};
    use std::time::Duration;
    use synor_utils::error::{Error, SharedError};
    use synor_utils::fingerprint::Fingerprint;

    #[tokio::test]
    async fn readiness_terminal_succeeded_maps_to_ok() {
        let handle =
            ComponentExecutionHandle::from_terminal(async { ComponentTerminalOutcome::Succeeded });
        handle.ready().await.unwrap();
    }

    #[tokio::test]
    async fn readiness_terminal_failed_preserves_error() {
        let handle = ComponentExecutionHandle::from_terminal(async {
            ComponentTerminalOutcome::Failed(SharedError::new(Error::internal_msg(
                "terminal failure",
            )))
        });
        let error = handle.ready().await.unwrap_err();
        assert!(error.to_string().contains("terminal failure"));
    }

    #[tokio::test]
    async fn readiness_terminal_cancelled_preserves_cancellation() {
        let handle =
            ComponentExecutionHandle::from_terminal(async { ComponentTerminalOutcome::Cancelled });
        assert!(handle.ready().await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn readiness_terminal_superseded_maps_to_compatible_success() {
        let handle =
            ComponentExecutionHandle::from_terminal(async { ComponentTerminalOutcome::Superseded });
        handle.ready().await.unwrap();
    }

    #[tokio::test]
    async fn readiness_outcome_preserves_all_terminal_variants() {
        let succeeded =
            ComponentExecutionHandle::from_terminal(async { ComponentTerminalOutcome::Succeeded });
        assert!(matches!(
            succeeded.outcome().await,
            ReadinessOutcome::Succeeded
        ));

        let failed = ComponentExecutionHandle::from_terminal(async {
            ComponentTerminalOutcome::Failed(SharedError::new(Error::internal_msg(
                "typed terminal failure",
            )))
        });
        let ReadinessOutcome::Failed(error) = failed.outcome().await else {
            panic!("expected failed readiness outcome");
        };
        assert!(error.to_string().contains("typed terminal failure"));

        let cancelled =
            ComponentExecutionHandle::from_terminal(async { ComponentTerminalOutcome::Cancelled });
        assert!(matches!(
            cancelled.outcome().await,
            ReadinessOutcome::Cancelled
        ));

        let superseded =
            ComponentExecutionHandle::from_terminal(async { ComponentTerminalOutcome::Superseded });
        assert!(matches!(
            superseded.outcome().await,
            ReadinessOutcome::Superseded
        ));
    }

    #[tokio::test]
    async fn readiness_handle_can_be_observed_repeatedly() {
        let handle = ComponentExecutionHandle::from_terminal(async {
            ComponentTerminalOutcome::Failed(SharedError::new(Error::internal_msg(
                "repeatable terminal failure",
            )))
        });

        let first = handle.ready().await.unwrap_err();
        let second = handle.ready().await.unwrap_err();
        let ReadinessOutcome::Failed(third) = handle.outcome().await else {
            panic!("expected failed readiness outcome");
        };

        assert!(first.to_string().contains("repeatable terminal failure"));
        assert!(second.to_string().contains("repeatable terminal failure"));
        assert!(third.to_string().contains("repeatable terminal failure"));
    }

    #[test]
    fn aggregate_terminal_precedence_is_failure_then_cancellation_then_success() {
        let mut success_then_cancelled = ComponentRunOutcome::default();
        success_then_cancelled.merge(ComponentRunOutcome::cancelled());
        assert!(matches!(
            success_then_cancelled.terminal,
            ComponentTerminalOutcome::Cancelled
        ));

        let mut superseded_then_cancelled = ComponentRunOutcome::superseded();
        superseded_then_cancelled.merge(ComponentRunOutcome::cancelled());
        assert!(matches!(
            superseded_then_cancelled.terminal,
            ComponentTerminalOutcome::Cancelled
        ));

        let mut cancelled_then_success = ComponentRunOutcome::cancelled();
        cancelled_then_success.merge(ComponentRunOutcome::default());
        assert!(matches!(
            cancelled_then_success.terminal,
            ComponentTerminalOutcome::Cancelled
        ));

        let failure = SharedError::new(Error::internal_msg("aggregate failure"));
        let mut cancelled_then_failed = ComponentRunOutcome::cancelled();
        cancelled_then_failed.merge(ComponentRunOutcome::failed(failure));
        assert!(matches!(
            cancelled_then_failed.terminal,
            ComponentTerminalOutcome::Failed(_)
        ));

        let failure = SharedError::new(Error::internal_msg("aggregate failure"));
        let mut failed_then_cancelled = ComponentRunOutcome::failed(failure);
        failed_then_cancelled.merge(ComponentRunOutcome::cancelled());
        assert!(matches!(
            failed_then_cancelled.terminal,
            ComponentTerminalOutcome::Failed(_)
        ));
    }

    #[tokio::test]
    async fn dropped_child_guard_aggregates_as_typed_cancellation() {
        let readiness = ComponentBgChildReadiness::default();
        let guard = readiness.clone().add_child();
        readiness.set_build_done();
        drop(guard);

        let outcome = readiness.readiness().wait().await.clone().unwrap();
        assert!(matches!(
            outcome.terminal,
            ComponentTerminalOutcome::Cancelled
        ));
    }

    #[test]
    fn foreground_failure_is_nonterminal_parent_evidence_that_invalidates_memo() {
        let failure = SharedError::new(Error::internal_msg("caught foreground failure"));
        let outcome = ComponentRunOutcome::failed(failure).into_foreground_dependency();

        assert!(matches!(
            outcome.terminal,
            ComponentTerminalOutcome::Succeeded
        ));
        assert!(outcome.has_exception());
    }

    #[tokio::test]
    async fn dropped_foreground_guard_does_not_independently_cancel_parent() {
        let readiness = ComponentBgChildReadiness::default();
        let guard = readiness.clone().add_foreground_child();
        readiness.set_build_done();
        drop(guard);

        let outcome = readiness.readiness().wait().await.clone().unwrap();
        assert!(matches!(
            outcome.terminal,
            ComponentTerminalOutcome::Succeeded
        ));
        assert!(outcome.has_exception());
    }

    #[test]
    fn test_any_active_pop_prune() {
        // Empty → inactive.
        let mut members: Vec<Weak<()>> = Vec::new();
        assert!(!any_active(&mut members));

        // One live entry → active, not pruned.
        let live = Arc::new(());
        members.push(Arc::downgrade(&live));
        assert!(any_active(&mut members));
        assert_eq!(members.len(), 1);

        // Trailing dead entries are popped off the back until the first live
        // one; the live entry at the front is preserved.
        let dead = Arc::new(());
        members.push(Arc::downgrade(&dead));
        drop(dead);
        assert!(any_active(&mut members)); // pops the dead tail, finds `live`
        assert_eq!(members.len(), 1);

        // All dead → inactive and emptied.
        drop(live);
        assert!(!any_active(&mut members));
        assert!(members.is_empty());
    }

    #[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
    struct TestProfile;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TestData(Vec<u8>);

    impl Persist for TestData {
        fn to_bytes(&self) -> crate::prelude::Result<bytes::Bytes> {
            Ok(bytes::Bytes::from(self.0.clone()))
        }

        fn from_bytes(data: &[u8]) -> crate::prelude::Result<Self> {
            Ok(Self(data.to_vec()))
        }
    }

    struct NoopSink;

    #[async_trait]
    impl TargetActionSink<TestProfile> for NoopSink {
        async fn apply(
            &self,
            _host_runtime_ctx: &(),
            _host_ctx: Arc<()>,
            _actions: Vec<()>,
        ) -> crate::prelude::Result<Option<Vec<Option<ChildTargetDef<TestProfile>>>>> {
            Ok(None)
        }
    }

    struct NoopHandler;

    impl TargetHandler<TestProfile> for NoopHandler {
        fn reconcile(
            &self,
            _key: StableKey,
            _desired_target_state: Option<&()>,
            _prev_possible_records: &[TestData],
            _prev_may_be_missing: bool,
        ) -> crate::prelude::Result<Option<TargetReconcileOutput<TestProfile>>> {
            Ok(None)
        }
    }

    impl Hash for TestProcessor {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.memo_fp.hash(state);
        }
    }

    impl PartialEq for TestProcessor {
        fn eq(&self, other: &Self) -> bool {
            self.memo_fp == other.memo_fp
        }
    }

    impl Eq for TestProcessor {}

    impl EngineProfile for TestProfile {
        type HostRuntimeCtx = ();
        type HostCtx = ();
        type ComponentProc = TestProcessor;
        type FunctionData = TestData;
        type TargetHdl = NoopHandler;
        type TargetStateTrackingRecord = TestData;
        type TargetAction = ();
        type TargetActionSink = NoopSink;
        type TargetStateValue = ();
    }

    struct TestProcessor {
        info: ComponentProcessorInfo,
        memo_fp: Fingerprint,
        body_started: Arc<AtomicBool>,
        advance_clock_in_state_handler: bool,
    }

    impl TestProcessor {
        fn new(
            name: &str,
            memo_fp: Fingerprint,
            body_started: Arc<AtomicBool>,
            advance_clock_in_state_handler: bool,
        ) -> Self {
            Self {
                info: ComponentProcessorInfo::new(name.to_string()),
                memo_fp,
                body_started,
                advance_clock_in_state_handler,
            }
        }
    }

    impl ComponentProcessor<TestProfile> for TestProcessor {
        fn process(
            &self,
            _host_runtime_ctx: &(),
            _comp_ctx: &ComponentProcessorContext<TestProfile>,
        ) -> crate::prelude::Result<
            impl Future<Output = crate::prelude::Result<TestData>> + Send + 'static,
        > {
            let body_started = self.body_started.clone();
            Ok(async move {
                body_started.store(true, Ordering::SeqCst);
                Ok(TestData(b"ret".to_vec()))
            })
        }

        fn memo_key_fingerprint(&self) -> Option<Fingerprint> {
            Some(self.memo_fp)
        }

        fn processor_info(&self) -> &ComponentProcessorInfo {
            &self.info
        }

        fn has_memo_state_handler(&self) -> bool {
            true
        }

        fn handle_memo_states(
            &self,
            _host_runtime_ctx: &(),
            _comp_ctx: &ComponentProcessorContext<TestProfile>,
            _stored_states: Option<MemoStatesPayload<TestProfile>>,
        ) -> crate::prelude::Result<
            impl Future<Output = crate::prelude::Result<(MemoStatesPayload<TestProfile>, bool, bool)>>
            + Send
            + 'static,
        > {
            let advance_clock = self.advance_clock_in_state_handler;
            Ok(async move {
                if advance_clock {
                    testing_advance_deadline_clock(Duration::from_secs(2));
                }
                Ok((
                    MemoStatesPayload {
                        positional: vec![TestData(b"state".to_vec())],
                        by_context_fp: Vec::new(),
                    },
                    false,
                    false,
                ))
            })
        }
    }

    struct TestClockGuard {
        _guard: std::sync::MutexGuard<'static, ()>,
    }

    impl TestClockGuard {
        fn new() -> Self {
            let guard = testing_deadline_clock_lock();
            testing_reset_deadline_clock();
            Self { _guard: guard }
        }
    }

    impl Drop for TestClockGuard {
        fn drop(&mut self) {
            testing_disable_deadline_clock();
        }
    }

    async fn test_app(name: &str) -> (App<TestProfile>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let settings = StorageSettings {
            db_path: dir.path().join("lmdb"),
            lmdb_max_dbs: 64,
            lmdb_map_size: 1 << 24,
        };
        let providers = Arc::new(Mutex::new(TargetStateProviderRegistry::new(
            Default::default(),
        )));
        let env = Environment::<TestProfile>::new(settings, providers, ())
            .await
            .unwrap();
        let app = App::new(name, env, None).await.unwrap();
        (app, dir)
    }

    #[tokio::test]
    async fn deadline_rechecked_after_memo_state_handler_before_processor_body() {
        let _clock = TestClockGuard::new();
        let memo_fp = Fingerprint::from(&"processor-entry-deadline").unwrap();
        let (app, _dir) = test_app("deadline_pre_body").await;

        let first_body_started = Arc::new(AtomicBool::new(false));
        let first_processor = TestProcessor::new(
            "deadline_pre_body",
            memo_fp,
            first_body_started.clone(),
            false,
        );
        let (handle, _) = app
            .update(
                first_processor,
                AppUpdateOptions::default(),
                Arc::new(()),
                None,
            )
            .unwrap();
        handle.result().await.unwrap();
        assert!(first_body_started.load(Ordering::SeqCst));

        let second_body_started = Arc::new(AtomicBool::new(false));
        let second_processor = TestProcessor::new(
            "deadline_pre_body",
            memo_fp,
            second_body_started.clone(),
            true,
        );
        let deadline = DeadlineContext::NONE.with_timeout(Duration::from_secs(1));
        let (handle, _) = app
            .update(
                second_processor,
                AppUpdateOptions {
                    deadline,
                    ..AppUpdateOptions::default()
                },
                Arc::new(()),
                None,
            )
            .unwrap();
        let err = handle.result().await.unwrap_err();
        assert!(err.is_deadline_exceeded());
        assert!(
            !second_body_started.load(Ordering::SeqCst),
            "processor body must not start after memo-state validation expires the deadline"
        );
    }
}
