use std::{collections::BTreeMap, time::Duration};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const MAX_SLOW_QUERY_PAGE_SIZE: usize = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExplainMode {
    Estimated,
    Analyze,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanFormat {
    Json,
    Text,
}

/// A request for a database-native execution plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplainRequest {
    pub id: Uuid,
    pub text: String,
    pub mode: ExplainMode,
    pub timeout: Duration,
}

impl ExplainRequest {
    /// Validate input and timeout before a driver constructs its EXPLAIN command.
    ///
    /// # Errors
    ///
    /// Returns an error for empty SQL or a zero timeout.
    pub fn validate(&self) -> Result<(), ExplainValidationError> {
        if self.text.trim().is_empty() {
            return Err(ExplainValidationError::EmptyQuery);
        }
        if self.timeout.is_zero() {
            return Err(ExplainValidationError::ZeroTimeout);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum ExplainValidationError {
    #[error("query text is empty")]
    EmptyQuery,
    #[error("execution-plan timeout must be greater than zero")]
    ZeroTimeout,
}

/// A plan payload plus portable metadata for generic and driver-specific viewers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub engine: String,
    pub format: PlanFormat,
    pub analyzed: bool,
    pub document: serde_json::Value,
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlowQueryOrder {
    MeanTime,
    TotalTime,
    Calls,
}

/// A bounded request for a database's aggregated slow-query statistics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlowQueryRequest {
    pub id: Uuid,
    pub limit: usize,
    pub minimum_mean_time_millis: Option<f64>,
    pub order: SlowQueryOrder,
    pub timeout: Duration,
}

impl SlowQueryRequest {
    /// Validate finite thresholds and resource bounds.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid page limit, timeout, or duration.
    pub fn validate(&self) -> Result<(), SlowQueryValidationError> {
        if !(1..=MAX_SLOW_QUERY_PAGE_SIZE).contains(&self.limit) {
            return Err(SlowQueryValidationError::InvalidLimit {
                maximum: MAX_SLOW_QUERY_PAGE_SIZE,
            });
        }
        if self.timeout.is_zero() {
            return Err(SlowQueryValidationError::ZeroTimeout);
        }
        if self
            .minimum_mean_time_millis
            .is_some_and(|millis| !millis.is_finite() || millis.is_sign_negative())
        {
            return Err(SlowQueryValidationError::InvalidMinimumDuration);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum SlowQueryValidationError {
    #[error("slow-query page limit must be between 1 and {maximum}")]
    InvalidLimit { maximum: usize },
    #[error("slow-query timeout must be greater than zero")]
    ZeroTimeout,
    #[error("minimum mean time must be a finite non-negative value")]
    InvalidMinimumDuration,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlowQueryEntry {
    pub fingerprint: Option<String>,
    pub database: Option<String>,
    pub user: Option<String>,
    pub query: String,
    pub calls: u64,
    pub total_time_millis: f64,
    pub mean_time_millis: f64,
    pub max_time_millis: Option<f64>,
    pub rows: u64,
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlowQueryPage {
    pub source: String,
    pub entries: Vec<SlowQueryEntry>,
}
