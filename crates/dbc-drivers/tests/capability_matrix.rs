use dbc_core::{
    capability::{Capability, QueryLanguage},
    driver::DriverDescriptor,
};
use dbc_drivers::{builtin_descriptors, builtin_factories};

fn descriptor<'a>(descriptors: &'a [DriverDescriptor], id: &str) -> &'a DriverDescriptor {
    descriptors
        .iter()
        .find(|descriptor| descriptor.id == id)
        .expect("built-in descriptor should exist")
}

#[test]
fn every_descriptor_has_a_runnable_builtin_factory() {
    let descriptor_ids = builtin_descriptors()
        .into_iter()
        .map(|descriptor| descriptor.id)
        .collect::<Vec<_>>();
    let factory_ids = builtin_factories()
        .into_iter()
        .map(|factory| factory.descriptor().id.clone())
        .collect::<Vec<_>>();

    assert_eq!(factory_ids, descriptor_ids);
}

#[test]
fn registers_the_five_mainstream_database_families() {
    let descriptors = builtin_descriptors();

    assert_eq!(
        descriptors
            .iter()
            .map(|descriptor| descriptor.id.as_str())
            .collect::<Vec<_>>(),
        vec!["mongodb", "mysql", "postgresql", "redis", "sqlite"]
    );
}

#[test]
fn relational_and_specialized_capabilities_are_not_flattened() {
    let descriptors = builtin_descriptors();
    let postgres = descriptor(&descriptors, "postgresql");
    let mongo = descriptor(&descriptors, "mongodb");
    let redis = descriptor(&descriptors, "redis");
    let sqlite = descriptor(&descriptors, "sqlite");

    assert!(postgres.capabilities.supports_crud());
    assert!(postgres.capabilities.supports_explain());
    assert!(postgres.capabilities.supports_slow_queries());
    assert!(
        postgres
            .capabilities
            .query_languages()
            .contains(&QueryLanguage::Sql)
    );

    assert!(mongo.capabilities.supports_crud());
    assert!(mongo.capabilities.supports_explain());
    assert!(mongo.capabilities.supports_slow_queries());
    assert!(
        mongo
            .capabilities
            .query_languages()
            .contains(&QueryLanguage::MongoQuery)
    );
    assert!(redis.capabilities.supports_crud());
    assert!(!redis.capabilities.supports_explain());
    assert!(redis.capabilities.supports_slow_queries());
    assert!(
        redis
            .capabilities
            .query_languages()
            .contains(&QueryLanguage::RedisCommand)
    );

    assert!(sqlite.capabilities.supports_explain());
    assert!(!sqlite.capabilities.supports_slow_queries());
    assert!(postgres.capabilities.capabilities().iter().any(
        |capability| matches!(capability, Capability::SlowQueries(settings) if settings.configurable)
    ));
}
