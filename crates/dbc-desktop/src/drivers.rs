use dbc_core::capability::QueryLanguage;
use egui_code_editor::Syntax;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EditorSyntax {
    Sql,
    Json,
    Shell,
}

impl EditorSyntax {
    pub fn syntax(self) -> Syntax {
        match self {
            Self::Sql => Syntax::sql(),
            Self::Json => Syntax::new("json").with_keywords(["true", "false", "null"]),
            Self::Shell => Syntax::shell(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DriverChoice {
    pub id: &'static str,
    pub name: &'static str,
    pub endpoint: &'static str,
    pub database: &'static str,
    pub user: &'static str,
    pub query: &'static str,
    pub language: QueryLanguage,
    pub editor_syntax: EditorSyntax,
}

pub const DRIVER_CHOICES: &[DriverChoice] = &[
    DriverChoice {
        id: "postgresql",
        name: "PostgreSQL",
        endpoint: "postgres://127.0.0.1:5432/postgres",
        database: "",
        user: "postgres",
        query: "SELECT current_database(), current_user, version();",
        language: QueryLanguage::Sql,
        editor_syntax: EditorSyntax::Sql,
    },
    DriverChoice {
        id: "mysql",
        name: "MySQL / MariaDB",
        endpoint: "mysql://127.0.0.1:3306/mysql",
        database: "",
        user: "root",
        query: "SELECT DATABASE(), CURRENT_USER(), VERSION();",
        language: QueryLanguage::Sql,
        editor_syntax: EditorSyntax::Sql,
    },
    DriverChoice {
        id: "sqlite",
        name: "SQLite",
        endpoint: "sqlite::memory:",
        database: "",
        user: "",
        query: "SELECT sqlite_version() AS version;",
        language: QueryLanguage::Sql,
        editor_syntax: EditorSyntax::Sql,
    },
    DriverChoice {
        id: "mongodb",
        name: "MongoDB",
        endpoint: "mongodb://127.0.0.1:27017",
        database: "test",
        user: "",
        query: "{\n  \"operation\": \"find\",\n  \"collection\": \"items\",\n  \"filter\": {},\n  \"limit\": 100\n}",
        language: QueryLanguage::MongoQuery,
        editor_syntax: EditorSyntax::Json,
    },
    DriverChoice {
        id: "redis",
        name: "Redis / Valkey",
        endpoint: "redis://127.0.0.1:6379",
        database: "",
        user: "",
        query: "PING",
        language: QueryLanguage::RedisCommand,
        editor_syntax: EditorSyntax::Shell,
    },
];
