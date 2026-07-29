use dbc_core::{
    driver::{ConnectionProfile, DriverFactory, SecretValue},
    error::DriverError,
};
use dbc_drivers::RedisFactory;

#[test]
fn redis_factory_exposes_the_redis_and_valkey_capability_profile() {
    let factory = RedisFactory::new();
    let descriptor = factory.descriptor();

    assert_eq!(descriptor.id, "redis");
    assert_eq!(descriptor.display_name, "Redis / Valkey");
    assert!(descriptor.capabilities.supports_crud());
    assert!(!descriptor.capabilities.supports_explain());
    assert!(descriptor.capabilities.supports_slow_queries());
}

#[tokio::test]
async fn redis_factory_rejects_invalid_endpoints_without_leaking_secrets() {
    let factory = RedisFactory::new();
    let profile = ConnectionProfile {
        id: "invalid".to_owned(),
        driver_id: "redis".to_owned(),
        display_name: "Invalid".to_owned(),
        endpoint: "not redis".to_owned(),
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
