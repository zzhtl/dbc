//! Built-in database driver factories.

mod mongo;
mod mysql;
mod postgres;
mod redis;
mod sqlite;

use std::sync::Arc;

use dbc_core::{
    capability::{
        Capability, CapabilitySet, CrudCapabilities, ExplainCapabilities, QueryLanguage,
        SlowQueryCapabilities,
    },
    driver::DriverDescriptor,
    driver::DriverFactory,
};

pub use mongo::{MongoFactory, MongoOperation};
pub use mysql::MySqlFactory;
pub use postgres::PostgresFactory;
pub use redis::RedisFactory;
pub use sqlite::SqliteFactory;

/// Return all native driver factories in stable id order.
#[must_use]
pub fn builtin_factories() -> Vec<Arc<dyn DriverFactory>> {
    vec![
        Arc::new(MongoFactory::new()),
        Arc::new(MySqlFactory::new()),
        Arc::new(PostgresFactory::new()),
        Arc::new(RedisFactory::new()),
        Arc::new(SqliteFactory::new()),
    ]
}

/// Return the built-in driver descriptors in stable id order.
#[must_use]
pub fn builtin_descriptors() -> Vec<DriverDescriptor> {
    vec![
        mongo_descriptor(),
        mysql_descriptor(),
        postgres_descriptor(),
        redis_descriptor(),
        sqlite_descriptor(),
    ]
}

fn postgres_descriptor() -> DriverDescriptor {
    descriptor(
        "postgresql",
        "PostgreSQL",
        QueryLanguage::Sql,
        transactional_crud(),
        Some(full_explain()),
        Some(configurable_slow_queries()),
    )
}

fn mysql_descriptor() -> DriverDescriptor {
    descriptor(
        "mysql",
        "MySQL / MariaDB",
        QueryLanguage::Sql,
        transactional_crud(),
        Some(full_explain()),
        Some(configurable_slow_queries()),
    )
}

fn mongo_descriptor() -> DriverDescriptor {
    descriptor(
        "mongodb",
        "MongoDB",
        QueryLanguage::MongoQuery,
        transactional_crud(),
        Some(full_explain()),
        Some(SlowQueryCapabilities {
            available: true,
            configurable: true,
        }),
    )
}

fn sqlite_descriptor() -> DriverDescriptor {
    descriptor(
        "sqlite",
        "SQLite",
        QueryLanguage::Sql,
        transactional_crud(),
        Some(ExplainCapabilities {
            estimated: true,
            analyzed: false,
        }),
        None,
    )
}

fn redis_descriptor() -> DriverDescriptor {
    descriptor(
        "redis",
        "Redis / Valkey",
        QueryLanguage::RedisCommand,
        CrudCapabilities {
            create: true,
            update: true,
            delete: true,
            transactional: false,
        },
        None,
        Some(SlowQueryCapabilities {
            available: true,
            configurable: false,
        }),
    )
}

fn descriptor(
    id: &str,
    display_name: &str,
    language: QueryLanguage,
    crud: CrudCapabilities,
    explain: Option<ExplainCapabilities>,
    slow_queries: Option<SlowQueryCapabilities>,
) -> DriverDescriptor {
    let mut capabilities = CapabilitySet::builder()
        .query_language(language)
        .enable(Capability::Crud(crud))
        .enable(Capability::SchemaManagement)
        .enable(Capability::Backup)
        .enable(Capability::ImportExport)
        .enable(Capability::Cancellation);
    if let Some(explain) = explain {
        capabilities = capabilities.enable(Capability::Explain(explain));
    }
    if let Some(slow_queries) = slow_queries {
        capabilities = capabilities.enable(Capability::SlowQueries(slow_queries));
    }

    DriverDescriptor {
        id: id.to_owned(),
        display_name: display_name.to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        capabilities: capabilities.build(),
    }
}

const fn transactional_crud() -> CrudCapabilities {
    CrudCapabilities {
        create: true,
        update: true,
        delete: true,
        transactional: true,
    }
}

const fn full_explain() -> ExplainCapabilities {
    ExplainCapabilities {
        estimated: true,
        analyzed: true,
    }
}

const fn configurable_slow_queries() -> SlowQueryCapabilities {
    SlowQueryCapabilities {
        available: true,
        configurable: true,
    }
}
