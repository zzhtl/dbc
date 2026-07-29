use dbc_core::{
    driver::{ConnectionProfile, DriverFactory, SecretValue},
    error::DriverError,
};
use dbc_drivers::MongoFactory;

#[test]
fn mongo_factory_exposes_the_mongodb_capability_profile() {
    let factory = MongoFactory::new();
    let descriptor = factory.descriptor();

    assert_eq!(descriptor.id, "mongodb");
    assert_eq!(descriptor.display_name, "MongoDB");
    assert!(descriptor.capabilities.supports_crud());
    assert!(descriptor.capabilities.supports_explain());
    assert!(descriptor.capabilities.supports_slow_queries());
}

#[tokio::test]
async fn mongo_factory_rejects_invalid_endpoints_without_leaking_secrets() {
    let factory = MongoFactory::new();
    let profile = ConnectionProfile {
        id: "invalid".to_owned(),
        driver_id: "mongodb".to_owned(),
        display_name: "Invalid".to_owned(),
        endpoint: "not mongodb".to_owned(),
        database: None,
        user: None,
        secret_id: Some("connection:invalid".to_owned()),
    };
    let secret = SecretValue::new("must-not-leak");

    let error = factory
        .connect(&profile, Some(&secret))
        .await
        .expect_err("invalid endpoint should be rejected");
    assert!(matches!(error, DriverError::Connection(_)));
    assert!(!error.to_string().contains("must-not-leak"));
}
