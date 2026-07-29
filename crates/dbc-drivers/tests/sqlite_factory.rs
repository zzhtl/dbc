use dbc_core::{
    driver::{ConnectionProfile, DriverFactory},
    error::DriverError,
};
use dbc_drivers::SqliteFactory;

#[test]
fn sqlite_factory_exposes_the_sqlite_capability_profile() {
    let factory = SqliteFactory::new();
    let descriptor = factory.descriptor();

    assert_eq!(descriptor.id, "sqlite");
    assert_eq!(descriptor.display_name, "SQLite");
    assert!(descriptor.capabilities.supports_crud());
    assert!(descriptor.capabilities.supports_explain());
    assert!(!descriptor.capabilities.supports_slow_queries());
}

#[tokio::test]
async fn sqlite_factory_rejects_non_sqlite_endpoints() {
    let factory = SqliteFactory::new();
    let profile = ConnectionProfile {
        id: "invalid".to_owned(),
        driver_id: "sqlite".to_owned(),
        display_name: "Invalid".to_owned(),
        endpoint: "postgres://localhost/app".to_owned(),
        database: None,
        user: None,
        secret_id: None,
    };

    let error = factory
        .connect(&profile, None)
        .await
        .expect_err("non-SQLite endpoint should be rejected");
    assert!(matches!(error, DriverError::Connection(_)));
}
