use std::io;
use thiserror::Error;

/// All errors produced by the synor API.
#[derive(Debug, Error)]
pub enum Error {
    /// Filesystem I/O failed.
    #[error("io: {0}")]
    Io(#[from] io::Error),

    /// Serialization failed.
    #[error("serde encode: {0}")]
    SerdeEncode(#[from] rmp_serde::encode::Error),

    /// Deserialization failed.
    #[error("serde decode: {0}")]
    SerdeDecode(#[from] rmp_serde::decode::Error),

    /// Engine invariant violated (component path conflict, cycle, etc).
    #[error("{0}")]
    Engine(String),

    /// A structured core-engine error crossing an internal SDK boundary.
    ///
    /// This keeps cancellation, reporting, and host-error metadata intact
    /// while a nested live component propagates through user-facing SDK
    /// futures. It is not constructed directly by SDK users.
    #[doc(hidden)]
    #[error("{0}")]
    Core(synor_utils::error::Error),

    /// The active Synor deadline has expired.
    #[error("Synor timeout deadline exceeded")]
    DeadlineExceeded,

    /// Requested type or key not found in context.
    #[error(
        "context: `{0}` not provided — call Environment::builder().provide() or provide_key() first"
    )]
    MissingContext(String),

    /// User-provided closure returned an error.
    #[error(transparent)]
    User(Box<dyn std::error::Error + Send + Sync>),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn user(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Error::User(Box::new(err))
    }

    pub fn engine(msg: impl Into<String>) -> Self {
        Error::Engine(msg.into())
    }

    pub fn is_deadline_exceeded(&self) -> bool {
        match self {
            Error::DeadlineExceeded => true,
            Error::Core(error) => error.is_deadline_exceeded(),
            _ => false,
        }
    }

    pub(crate) fn into_core(self) -> synor_utils::error::Error {
        match self {
            Error::DeadlineExceeded => synor_utils::error::Error::deadline_exceeded(),
            Error::Core(error) => error,
            other => synor_utils::error::Error::internal_msg(other.to_string()),
        }
    }
}

/// Convert from synor_utils::error::Error (used by core).
impl From<synor_utils::error::Error> for Error {
    fn from(e: synor_utils::error::Error) -> Self {
        if e.is_deadline_exceeded() && !e.is_reported() {
            return Error::DeadlineExceeded;
        }
        Error::Core(e)
    }
}
