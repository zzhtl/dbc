use std::sync::Arc;

use async_trait::async_trait;
use dbc_core::{
    capability::{CapabilitySet, QueryLanguage},
    driver::{
        ConnectionProfile, DatabaseSession, DriverDescriptor, DriverFactory, DriverRegistry,
        RegistryError,
    },
    error::DriverError,
};

struct StubFactory {
    descriptor: DriverDescriptor,
}

impl StubFactory {
    fn new(id: &str, display_name: &str) -> Self {
        Self {
            descriptor: DriverDescriptor {
                id: id.to_owned(),
                display_name: display_name.to_owned(),
                version: "1.0.0".to_owned(),
                capabilities: CapabilitySet::builder()
                    .query_language(QueryLanguage::Sql)
                    .build(),
            },
        }
    }
}

#[async_trait]
impl DriverFactory for StubFactory {
    fn descriptor(&self) -> &DriverDescriptor {
        &self.descriptor
    }

    async fn connect(
        &self,
        _profile: &ConnectionProfile,
        _secret: Option<&dbc_core::driver::SecretValue>,
    ) -> Result<Arc<dyn DatabaseSession>, DriverError> {
        Err(DriverError::Unsupported(
            "stub driver cannot connect".to_owned(),
        ))
    }
}

#[test]
fn registry_rejects_duplicate_driver_ids_and_lists_deterministically() {
    let mut registry = DriverRegistry::new();
    registry
        .register(Arc::new(StubFactory::new("postgres", "PostgreSQL")))
        .expect("first registration should succeed");
    registry
        .register(Arc::new(StubFactory::new("mysql", "MySQL")))
        .expect("second registration should succeed");

    assert_eq!(
        registry
            .descriptors()
            .iter()
            .map(|descriptor| descriptor.id.as_str())
            .collect::<Vec<_>>(),
        vec!["mysql", "postgres"]
    );

    assert_eq!(
        registry
            .register(Arc::new(StubFactory::new("postgres", "Other")))
            .expect_err("duplicate id should be rejected"),
        RegistryError::DuplicateDriverId("postgres".to_owned())
    );
    assert!(registry.get("postgres").is_some());
    assert!(registry.get("missing").is_none());
}
