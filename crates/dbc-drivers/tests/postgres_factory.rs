use dbc_core::{
    driver::{ConnectionProfile, DriverFactory, SecretValue},
    error::DriverError,
};
use dbc_drivers::PostgresFactory;

#[test]
fn postgres_factory_exposes_the_postgres_capability_profile() {
    let factory = PostgresFactory::new();
    let descriptor = factory.descriptor();

    assert_eq!(descriptor.id, "postgresql");
    assert_eq!(descriptor.display_name, "PostgreSQL");
    assert!(descriptor.capabilities.supports_crud());
    assert!(descriptor.capabilities.supports_explain());
    assert!(descriptor.capabilities.supports_slow_queries());
}

#[tokio::test]
async fn postgres_factory_rejects_invalid_endpoints_before_network_io() {
    let factory = PostgresFactory::new();
    let profile = ConnectionProfile {
        id: "invalid".to_owned(),
        driver_id: "postgresql".to_owned(),
        display_name: "Invalid".to_owned(),
        endpoint: "this is not a postgres endpoint".to_owned(),
        database: None,
        user: None,
        secret_id: Some("connection:invalid".to_owned()),
    };
    let secret = SecretValue::new("not-logged");

    let error = factory
        .connect(&profile, Some(&secret))
        .await
        .expect_err("invalid endpoint should be rejected");

    assert!(matches!(error, DriverError::Connection(_)));
    assert!(!error.to_string().contains("not-logged"));
}
