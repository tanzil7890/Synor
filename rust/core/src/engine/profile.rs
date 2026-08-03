use std::{fmt::Debug, hash::Hash, sync::Arc};

use crate::engine::{
    component::ComponentProcessor,
    target_state::{TargetActionSink, TargetHandler},
};
use crate::prelude::*;

pub trait Persist: Sized {
    fn to_bytes(&self) -> Result<bytes::Bytes>;

    fn from_bytes(data: &[u8]) -> Result<Self>;
}

impl<T: Persist> Persist for Arc<T> {
    fn to_bytes(&self) -> Result<bytes::Bytes> {
        (**self).to_bytes()
    }

    fn from_bytes(data: &[u8]) -> Result<Self> {
        Ok(Arc::new(T::from_bytes(data)?))
    }
}

pub trait StableFingerprint {
    fn stable_fingerprint(&self) -> utils::fingerprint::Fingerprint;
}

impl<T: StableFingerprint> StableFingerprint for Arc<T> {
    fn stable_fingerprint(&self) -> utils::fingerprint::Fingerprint {
        (**self).stable_fingerprint()
    }
}

pub trait EngineProfile: Debug + Clone + PartialEq + Eq + Hash + Default + 'static {
    type HostRuntimeCtx: Clone + Send + Sync + Eq + Hash + 'static;
    type HostCtx: Send + Sync + 'static;

    type ComponentProc: ComponentProcessor<Self>;
    type FunctionData: Clone + Send + Sync + Persist + 'static;

    type TargetHdl: TargetHandler<Self>;
    type TargetStateTrackingRecord: Send + Persist + 'static;
    type TargetAction: Send + 'static;
    type TargetActionSink: TargetActionSink<Self>;
    type TargetStateValue: Send + 'static;

    /// Derive the callback context owned by one app.
    ///
    /// Native profiles have no foreign callback runtime, so cloning the
    /// environment context is sufficient. Host bindings can override this to
    /// create an app-local drain scope while retaining environment-wide
    /// shutdown admission and tracking.
    fn derive_host_callback_context(
        host_runtime_ctx: &Self::HostRuntimeCtx,
    ) -> Self::HostRuntimeCtx {
        host_runtime_ctx.clone()
    }

    /// Best-effort retained size of one declared target-state value.
    /// Profiles with host-owned or serialized values should override this so
    /// a component cannot retain an unbounded declaration working set.
    fn target_state_value_size_bytes(value: &Self::TargetStateValue) -> usize {
        std::mem::size_of_val(value).max(1)
    }

    /// Best-effort retained size of one target action for queue backpressure.
    /// Profiles with serialized or host-owned values should override this.
    fn target_action_size_bytes(action: &Self::TargetAction) -> usize {
        std::mem::size_of_val(action).max(1)
    }

    /// Acquire admission for a host-bound app operation.
    ///
    /// The returned guard is retained through operation termination. Native
    /// profiles have no independently closing host environment, so their
    /// default guard is inert.
    fn acquire_host_operation(
        _host_runtime_ctx: &Self::HostRuntimeCtx,
    ) -> Result<Box<dyn Send + Sync>> {
        Ok(Box::new(()))
    }

    /// Await host-environment shutdown for a live app operation.
    ///
    /// Native profiles have no independently managed host environment, so the
    /// default never resolves. Host bindings override this with a
    /// generation-scoped signal that is cancelled before shutdown drains
    /// admitted operation leases. Non-live operations do not observe this
    /// hook and retain their existing finish-before-shutdown behavior.
    fn host_live_operation_cancelled(
        _host_runtime_ctx: &Self::HostRuntimeCtx,
    ) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(std::future::pending())
    }

    /// Await host callbacks that started before this barrier.
    ///
    /// Native profiles have no foreign-runtime callbacks, so the default is a
    /// no-op. Host bindings override this to prevent operation completion from
    /// racing callbacks that cannot be forcefully aborted once running.
    fn drain_host_callbacks(
        _host_runtime_ctx: &Self::HostRuntimeCtx,
    ) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}
