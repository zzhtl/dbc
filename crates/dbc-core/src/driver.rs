use std::{collections::BTreeMap, fmt, pin::Pin, sync::Arc};

use async_trait::async_trait;
use dbc_data::{DataBatch, DataSchema};
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroize;

use crate::{
    capability::CapabilitySet,
    diagnostics::{ExecutionPlan, ExplainRequest, SlowQueryPage, SlowQueryRequest},
    error::DriverError,
    metadata::{ObjectListRequest, ObjectPage},
    query::QueryRequest,
};

pub type QueryStream = Pin<Box<dyn Stream<Item = Result<QueryEvent, DriverError>> + Send>>;

/// Stable metadata for a driver implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DriverDescriptor {
    pub id: String,
    pub display_name: String,
    pub version: String,
    pub capabilities: CapabilitySet,
}

/// Result stream events. Row payload variants are added by the data crate.
#[derive(Debug, Clone)]
pub enum QueryEvent {
    Schema(DataSchema),
    Rows(DataBatch),
    Message(String),
    AffectedRows(u64),
    Finished(QueryStats),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryStats {
    pub elapsed_millis: u64,
    pub returned_rows: u64,
    pub affected_rows: u64,
    pub row_limit_reached: bool,
}

/// Creates database sessions without coupling the application to a concrete driver.
#[async_trait]
pub trait DriverFactory: Send + Sync {
    fn descriptor(&self) -> &DriverDescriptor;

    async fn connect(
        &self,
        profile: &ConnectionProfile,
        secret: Option<&SecretValue>,
    ) -> Result<Arc<dyn DatabaseSession>, DriverError>;
}

/// A live database connection or connection pool.
#[async_trait]
pub trait DatabaseSession: Send + Sync {
    async fn execute(
        &self,
        request: QueryRequest,
        cancellation: CancellationToken,
    ) -> Result<QueryStream, DriverError>;

    async fn list_objects(
        &self,
        _request: ObjectListRequest,
        _cancellation: CancellationToken,
    ) -> Result<ObjectPage, DriverError> {
        Err(DriverError::Unsupported(
            "database object discovery".to_owned(),
        ))
    }

    async fn explain(
        &self,
        _request: ExplainRequest,
        _cancellation: CancellationToken,
    ) -> Result<ExecutionPlan, DriverError> {
        Err(DriverError::Unsupported("execution plans".to_owned()))
    }

    async fn slow_queries(
        &self,
        _request: SlowQueryRequest,
        _cancellation: CancellationToken,
    ) -> Result<SlowQueryPage, DriverError> {
        Err(DriverError::Unsupported("slow-query statistics".to_owned()))
    }

    async fn close(&self) -> Result<(), DriverError>;
}

impl fmt::Debug for dyn DatabaseSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DatabaseSession(..)")
    }
}

/// Non-secret connection settings. `secret_id` points to the platform keychain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionProfile {
    pub id: String,
    pub driver_id: String,
    pub display_name: String,
    pub endpoint: String,
    pub database: Option<String>,
    pub user: Option<String>,
    pub secret_id: Option<String>,
}

/// In-memory secret that is redacted from diagnostics and cleared on drop.
pub struct SecretValue(String);

impl SecretValue {
    #[must_use]
    pub fn new(secret: impl Into<String>) -> Self {
        Self(secret.into())
    }

    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretValue([REDACTED])")
    }
}

impl Drop for SecretValue {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// Runtime registry for built-in and process-backed drivers.
#[derive(Default)]
pub struct DriverRegistry {
    factories: BTreeMap<String, Arc<dyn DriverFactory>>,
}

impl DriverRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a driver factory.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError::DuplicateDriverId`] when the id is already present.
    pub fn register(&mut self, factory: Arc<dyn DriverFactory>) -> Result<(), RegistryError> {
        let id = factory.descriptor().id.clone();
        if self.factories.contains_key(&id) {
            return Err(RegistryError::DuplicateDriverId(id));
        }
        self.factories.insert(id, factory);
        Ok(())
    }

    #[must_use]
    pub fn get(&self, id: &str) -> Option<Arc<dyn DriverFactory>> {
        self.factories.get(id).cloned()
    }

    #[must_use]
    pub fn descriptors(&self) -> Vec<&DriverDescriptor> {
        self.factories
            .values()
            .map(|factory| factory.descriptor())
            .collect()
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("driver id is already registered: {0}")]
    DuplicateDriverId(String),
}
