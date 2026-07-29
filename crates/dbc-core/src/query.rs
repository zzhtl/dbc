use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::capability::QueryLanguage;

/// A bounded query submitted to a database session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryRequest {
    pub id: Uuid,
    pub language: QueryLanguage,
    pub text: String,
    pub timeout: Duration,
    pub row_limit: usize,
}

impl QueryRequest {
    #[must_use]
    pub fn new(
        id: Uuid,
        language: QueryLanguage,
        text: impl Into<String>,
        timeout: Duration,
        row_limit: usize,
    ) -> Self {
        Self {
            id,
            language,
            text: text.into(),
            timeout,
            row_limit,
        }
    }

    /// Validate limits before a request crosses a driver boundary.
    ///
    /// # Errors
    ///
    /// Returns a specific error for empty text, zero timeout, or zero row limit.
    pub fn validate(&self) -> Result<(), QueryValidationError> {
        if self.text.trim().is_empty() {
            return Err(QueryValidationError::EmptyQuery);
        }
        if self.timeout.is_zero() {
            return Err(QueryValidationError::ZeroTimeout);
        }
        if self.row_limit == 0 {
            return Err(QueryValidationError::ZeroRowLimit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum QueryValidationError {
    #[error("query text is empty")]
    EmptyQuery,
    #[error("query timeout must be greater than zero")]
    ZeroTimeout,
    #[error("query row limit must be greater than zero")]
    ZeroRowLimit,
}
