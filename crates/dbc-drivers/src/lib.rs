//! Built-in database driver factories.

mod mongo;
mod mysql;
mod postgres;
mod redis;
mod relational;
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

/// What one driver actually implements.
///
/// Capabilities are never declared optimistically: `server_side_cancel` is set
/// only for drivers that tell the database to stop the statement, and the
/// schema-management, backup and import/export flags were dropped outright
/// because nothing implemented them.
struct DriverProfile {
    id: &'static str,
    display_name: &'static str,
    language: QueryLanguage,
    crud: CrudCapabilities,
    explain: Option<ExplainCapabilities>,
    slow_queries: Option<SlowQueryCapabilities>,
    table_data: bool,
    server_side_cancel: bool,
}

fn postgres_descriptor() -> DriverDescriptor {
    descriptor(DriverProfile {
        id: "postgresql",
        display_name: "PostgreSQL",
        language: QueryLanguage::Sql,
        crud: transactional_crud(),
        explain: Some(full_explain()),
        slow_queries: Some(configurable_slow_queries()),
        table_data: true,
        // `pg_cancel_backend` stops the statement on the server.
        server_side_cancel: true,
    })
}

fn mysql_descriptor() -> DriverDescriptor {
    descriptor(DriverProfile {
        id: "mysql",
        display_name: "MySQL / MariaDB",
        language: QueryLanguage::Sql,
        crud: transactional_crud(),
        explain: Some(full_explain()),
        slow_queries: Some(configurable_slow_queries()),
        table_data: true,
        // `KILL QUERY` stops the statement on the server.
        server_side_cancel: true,
    })
}

fn mongo_descriptor() -> DriverDescriptor {
    descriptor(DriverProfile {
        id: "mongodb",
        display_name: "MongoDB",
        language: QueryLanguage::MongoQuery,
        crud: transactional_crud(),
        explain: Some(full_explain()),
        slow_queries: Some(SlowQueryCapabilities {
            available: true,
            configurable: true,
        }),
        table_data: false,
        // Cancellation is client-side only: no cursor kill is issued.
        server_side_cancel: false,
    })
}

fn sqlite_descriptor() -> DriverDescriptor {
    descriptor(DriverProfile {
        id: "sqlite",
        display_name: "SQLite",
        language: QueryLanguage::Sql,
        crud: transactional_crud(),
        explain: Some(ExplainCapabilities {
            estimated: true,
            analyzed: false,
        }),
        slow_queries: None,
        table_data: true,
        // sqlx exposes no interrupt hook for the bundled SQLite engine.
        server_side_cancel: false,
    })
}

fn redis_descriptor() -> DriverDescriptor {
    descriptor(DriverProfile {
        id: "redis",
        display_name: "Redis / Valkey",
        language: QueryLanguage::RedisCommand,
        crud: CrudCapabilities {
            create: true,
            update: true,
            delete: true,
            transactional: false,
        },
        explain: None,
        slow_queries: Some(SlowQueryCapabilities {
            available: true,
            configurable: false,
        }),
        table_data: false,
        // RESP is strictly request/response; an in-flight command cannot be
        // cancelled.
        server_side_cancel: false,
    })
}

fn descriptor(profile: DriverProfile) -> DriverDescriptor {
    let mut capabilities = CapabilitySet::builder()
        .query_language(profile.language)
        .enable(Capability::Crud(profile.crud));
    if let Some(explain) = profile.explain {
        capabilities = capabilities.enable(Capability::Explain(explain));
    }
    if let Some(slow_queries) = profile.slow_queries {
        capabilities = capabilities.enable(Capability::SlowQueries(slow_queries));
    }
    if profile.table_data {
        capabilities = capabilities.enable(Capability::TableData);
    }
    if profile.server_side_cancel {
        capabilities = capabilities.enable(Capability::Cancellation);
    }

    DriverDescriptor {
        id: profile.id.to_owned(),
        display_name: profile.display_name.to_owned(),
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
