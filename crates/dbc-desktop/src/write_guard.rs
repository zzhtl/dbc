//! Second-click confirmation for write operations.
//!
//! This is misuse protection, not a sandbox: the classification is deliberately
//! fail-closed, so anything that cannot be proven read-only asks for a second
//! click. Use a least-privilege database account as well.

use dbc_core::{
    capability::QueryLanguage,
    sql::{StatementRisk, classify_sql},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteAction {
    Execute,
    Analyze,
    FullExport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingWriteConfirmation {
    pub(crate) action: WriteAction,
    pub(crate) text: String,
}

pub(crate) fn requires_confirmation(language: QueryLanguage, text: &str) -> bool {
    match language {
        QueryLanguage::Sql => classify_sql(text) != StatementRisk::ReadOnly,
        QueryLanguage::MongoQuery => serde_json::from_str::<serde_json::Value>(text)
            .map_or(true, |value| mongo_requires_confirmation(&value)),
        QueryLanguage::RedisCommand => {
            let command = text
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_ascii_uppercase();
            !matches!(
                command.as_str(),
                "PING"
                    | "GET"
                    | "MGET"
                    | "SCAN"
                    | "TYPE"
                    | "TTL"
                    | "PTTL"
                    | "HGET"
                    | "HGETALL"
                    | "LRANGE"
                    | "SMEMBERS"
                    | "ZRANGE"
                    | "XRANGE"
                    | "INFO"
                    | "DBSIZE"
                    | "EXISTS"
            )
        }
    }
}

pub(crate) fn mongo_requires_confirmation(value: &serde_json::Value) -> bool {
    let operation = value
        .get("operation")
        .and_then(serde_json::Value::as_str)
        .map(str::to_ascii_lowercase);
    match operation.as_deref() {
        Some("find") => false,
        Some("aggregate") => value
            .get("pipeline")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|pipeline| {
                pipeline.iter().any(|stage| {
                    stage.as_object().is_some_and(|stage| {
                        stage.contains_key("$out") || stage.contains_key("$merge")
                    })
                })
            }),
        _ => true,
    }
}

pub(crate) fn write_confirmation_matches(
    pending: Option<&PendingWriteConfirmation>,
    action: WriteAction,
    text: &str,
) -> bool {
    pending.is_some_and(|pending| pending.action == action && pending.text == text)
}

#[cfg(test)]
mod tests {
    use dbc_core::capability::QueryLanguage;

    use super::{
        PendingWriteConfirmation, WriteAction, requires_confirmation,
        write_confirmation_matches,
    };

    #[test]
    fn write_confirmation_covers_all_query_languages() {
        assert!(!requires_confirmation(QueryLanguage::Sql, "SELECT 1"));
        assert!(requires_confirmation(
            QueryLanguage::Sql,
            "DELETE FROM items"
        ));
        assert!(!requires_confirmation(
            QueryLanguage::MongoQuery,
            r#"{"operation":"find","collection":"items"}"#
        ));
        assert!(!requires_confirmation(
            QueryLanguage::MongoQuery,
            r#"{"operation":"aggregate","collection":"items","pipeline":[{"$match":{}}]}"#
        ));
        assert!(requires_confirmation(
            QueryLanguage::MongoQuery,
            r#"{"operation":"aggregate","collection":"items","pipeline":[{"$out":"archive"}]}"#
        ));
        assert!(requires_confirmation(
            QueryLanguage::MongoQuery,
            r#"{"operation":"aggregate","collection":"items","pipeline":[{"$merge":{"into":"archive"}}]}"#
        ));
        assert!(requires_confirmation(
            QueryLanguage::MongoQuery,
            r#"{"operation":"delete","collection":"items"}"#
        ));
        assert!(!requires_confirmation(
            QueryLanguage::RedisCommand,
            "GET key"
        ));
        assert!(requires_confirmation(
            QueryLanguage::RedisCommand,
            "DEL key"
        ));
    }

    #[test]
    fn write_confirmation_is_scoped_to_action_and_exact_text() {
        let pending = PendingWriteConfirmation {
            action: WriteAction::Execute,
            text: "DELETE FROM items".to_owned(),
        };

        assert!(write_confirmation_matches(
            Some(&pending),
            WriteAction::Execute,
            "DELETE FROM items"
        ));
        assert!(!write_confirmation_matches(
            Some(&pending),
            WriteAction::Analyze,
            "DELETE FROM items"
        ));
        assert!(!write_confirmation_matches(
            Some(&pending),
            WriteAction::FullExport,
            "DELETE FROM items"
        ));
        assert!(!write_confirmation_matches(
            Some(&pending),
            WriteAction::Execute,
            "DELETE FROM other_items"
        ));
    }

}
