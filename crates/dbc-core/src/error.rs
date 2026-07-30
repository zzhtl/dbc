use thiserror::Error;

/// Errors that can cross the driver boundary without leaking implementation details.
#[derive(Debug, Error)]
pub enum DriverError {
    #[error("authentication failed: {0}")]
    Authentication(String),
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("permission denied: {0}")]
    Permission(String),
    #[error("operation timed out")]
    Timeout,
    #[error("operation was cancelled")]
    Cancelled,
    #[error("query is invalid: {0}")]
    Query(String),
    #[error("data changed since it was loaded: {0}")]
    Conflict(String),
    #[error("capability is not supported: {0}")]
    Unsupported(String),
    #[error("plugin protocol error: {0}")]
    Protocol(String),
    #[error("driver error: {0}")]
    Internal(String),
}
