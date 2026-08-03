//! Typed terminal outcomes for background component readiness.

use crate::error::{Error, Result};

/// The terminal state of a background component operation.
///
/// This is the explicit counterpart to the established `Result<()>` readiness
/// contract. A superseded live update remains compatible success because a
/// newer operation owns the same stable path, while cancellation remains an
/// error when converted through [`ReadinessOutcome::into_result`].
#[derive(Debug)]
#[must_use]
pub enum ReadinessOutcome {
    /// The operation and its durable downstream reconciliation completed.
    Succeeded,
    /// The operation failed. The structured SDK error remains available for
    /// logging, classification, or propagation.
    Failed(Error),
    /// The operation was cancelled before durable success.
    Cancelled,
    /// A newer live operation for the same stable path displaced this one.
    Superseded,
}

impl ReadinessOutcome {
    /// Whether the historical readiness contract treats this as success.
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded | Self::Superseded)
    }

    /// Return the error carried by a failed outcome.
    pub fn error(&self) -> Option<&Error> {
        match self {
            Self::Failed(error) => Some(error),
            _ => None,
        }
    }

    /// Convert to the established success-or-error readiness contract.
    ///
    /// `Succeeded` and `Superseded` return `Ok(())`; `Failed` returns its
    /// original error; `Cancelled` returns a cancellation-classified error.
    pub fn into_result(self) -> Result<()> {
        match self {
            Self::Succeeded | Self::Superseded => Ok(()),
            Self::Failed(error) => Err(error),
            Self::Cancelled => Err(Error::from(synor_utils::error::Error::cancelled())),
        }
    }

    pub(crate) fn from_core(outcome: synor_core::engine::component::ReadinessOutcome) -> Self {
        match outcome {
            synor_core::engine::component::ReadinessOutcome::Succeeded => Self::Succeeded,
            synor_core::engine::component::ReadinessOutcome::Failed(error) => {
                Self::Failed(Error::from(error))
            }
            synor_core::engine::component::ReadinessOutcome::Cancelled => Self::Cancelled,
            synor_core::engine::component::ReadinessOutcome::Superseded => Self::Superseded,
        }
    }

    pub(crate) fn from_result(result: Result<()>) -> Self {
        match result {
            Ok(()) => Self::Succeeded,
            Err(Error::Core(error)) if error.is_cancelled() => Self::Cancelled,
            Err(error) => Self::Failed(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ReadinessOutcome;
    use crate::Error;

    #[test]
    fn compatibility_conversion_preserves_each_variant() {
        ReadinessOutcome::Succeeded.into_result().unwrap();
        ReadinessOutcome::Superseded.into_result().unwrap();

        let failure = ReadinessOutcome::Failed(Error::engine("readiness failed"))
            .into_result()
            .unwrap_err();
        assert_eq!(failure.to_string(), "readiness failed");

        let cancellation = ReadinessOutcome::Cancelled.into_result().unwrap_err();
        assert!(matches!(cancellation, Error::Core(ref error) if error.is_cancelled()));
    }

    #[test]
    fn core_conversion_preserves_typed_variants() {
        use synor_core::engine::component::ReadinessOutcome as CoreOutcome;

        assert!(matches!(
            ReadinessOutcome::from_core(CoreOutcome::Succeeded),
            ReadinessOutcome::Succeeded
        ));
        assert!(matches!(
            ReadinessOutcome::from_core(CoreOutcome::Cancelled),
            ReadinessOutcome::Cancelled
        ));
        assert!(matches!(
            ReadinessOutcome::from_core(CoreOutcome::Superseded),
            ReadinessOutcome::Superseded
        ));

        let outcome = ReadinessOutcome::from_core(CoreOutcome::Failed(
            synor_utils::error::Error::internal_msg("core readiness failure"),
        ));
        assert!(matches!(
            outcome,
            ReadinessOutcome::Failed(ref error)
                if error.to_string().contains("core readiness failure")
        ));
    }
}
