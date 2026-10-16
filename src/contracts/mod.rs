use thiserror::Error;

pub type CoreResult<T> = Result<T, CoreError>;

#[derive(Debug, Clone, Error)]
pub enum CoreError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
    #[error("deadline exceeded: {0}")]
    DeadlineExceeded(String),
    #[error("cancelled: {0}")]
    Cancelled(String),
    #[error("lease lost: {0}")]
    LeaseLost(String),
    #[error("index not ready: {0}")]
    IndexNotReady(String),
}

impl From<std::io::Error> for CoreError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<serde_json::Error> for CoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error.to_string())
    }
}
