use std::{
    collections::BTreeMap,
    future::Future,
    sync::Arc,
    time::Duration,
};

use async_stream::try_stream;
use async_trait::async_trait;
use dbc_core::{
    capability::QueryLanguage,
    diagnostics::{SlowQueryEntry, SlowQueryOrder, SlowQueryPage, SlowQueryRequest},
    driver::{
        ConnectionProfile, DatabaseSession, DriverDescriptor, DriverFactory, QueryEvent,
        QueryStats, QueryStream, SecretValue,
    },
    error::DriverError,
    metadata::{
        DatabaseObject, DatabaseObjectKind, ObjectListRequest, ObjectPage, ObjectPath,
    },
    query::QueryRequest,
};
use dbc_core::result::{DataBatch, DataSchema};
use redis::{
    Client, ErrorKind, IntoConnectionInfo, RedisError, Value,
    aio::MultiplexedConnection,
};
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

use crate::redis_descriptor;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_VALUE_DEPTH: usize = 64;

/// Creates asynchronous Redis and Valkey sessions using RESP.
#[derive(Debug, Clone)]
pub struct RedisFactory {
    descriptor: DriverDescriptor,
}

impl RedisFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {
            descriptor: redis_descriptor(),
        }
    }
}

impl Default for RedisFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DriverFactory for RedisFactory {
    fn descriptor(&self) -> &DriverDescriptor {
        &self.descriptor
    }

    async fn connect(
        &self,
        profile: &ConnectionProfile,
        secret: Option<&SecretValue>,
    ) -> Result<Arc<dyn DatabaseSession>, DriverError> {
        if profile.driver_id != self.descriptor.id {
            return Err(DriverError::Connection(
                "connection profile does not target Redis".to_owned(),
            ));
        }
        let endpoint = profile.endpoint.trim();
        if !["redis://", "rediss://", "valkey://", "valkeys://"]
            .iter()
            .any(|scheme| endpoint.starts_with(scheme))
        {
            return Err(DriverError::Connection(
                "Redis endpoint must use redis://, rediss://, valkey://, or valkeys://".to_owned(),
            ));
        }

        let info = endpoint
            .into_connection_info()
            .map_err(|_| DriverError::Connection("Redis endpoint is not a valid URL".to_owned()))?;
        let mut settings = info.redis_settings().clone();
        if let Some(user) = profile.user.as_deref() {
            settings = settings.set_username(user);
        }
        if let Some(secret) = secret {
            settings = settings.set_password(secret.expose());
        }
        if let Some(database) = profile.database.as_deref() {
            let database = database.parse::<i64>().map_err(|_| {
                DriverError::Connection("Redis database must be a numeric index".to_owned())
            })?;
            if database.is_negative() {
                return Err(DriverError::Connection(
                    "Redis database index cannot be negative".to_owned(),
                ));
            }
            settings = settings.set_db(database);
        }
        let client = Client::open(info.set_redis_settings(settings))
            .map_err(|_| DriverError::Connection("Redis connection settings are invalid".to_owned()))?;
        let connection = timeout(
            CONNECT_TIMEOUT,
            client.get_multiplexed_async_connection(),
        )
            .await
            .map_err(|_| DriverError::Timeout)?
            .map_err(map_connect_error)?;

        Ok(Arc::new(RedisSession { connection }))
    }
}

#[derive(Debug)]
struct RedisSession {
    connection: MultiplexedConnection,
}

#[async_trait]
impl DatabaseSession for RedisSession {
    async fn execute(
        &self,
        request: QueryRequest,
        cancellation: CancellationToken,
    ) -> Result<QueryStream, DriverError> {
        request
            .validate()
            .map_err(|error| DriverError::Query(error.to_string()))?;
        if request.language != QueryLanguage::RedisCommand {
            return Err(DriverError::Unsupported(
                "Redis accepts Redis command requests only".to_owned(),
            ));
        }
        let arguments = parse_command(&request.text)?;
        if arguments[0].eq_ignore_ascii_case("KEYS") {
            return Err(DriverError::Query(
                "KEYS is intentionally disabled; use SCAN to avoid blocking the server".to_owned(),
            ));
        }
        let deadline = operation_deadline(request.timeout)?;
        let row_limit = request.row_limit;
        let mut connection = self.connection.clone();

        Ok(Box::pin(try_stream! {
            let started = Instant::now();
            let mut command = redis::cmd(&arguments[0]);
            for argument in &arguments[1..] {
                command.arg(argument.as_bytes());
            }
            let value = controlled(
                deadline,
                &cancellation,
                command.query_async::<Value>(&mut connection),
            )
            .await?
            .map_err(map_query_error)?;
            let value_rows = value_row_count(&value, row_limit);
            let document = value_to_json(value, row_limit, 0);
            yield QueryEvent::Schema(DataSchema::Documents);
            yield QueryEvent::Rows(DataBatch::Documents(vec![document]));
            yield QueryEvent::Finished(QueryStats {
                elapsed_millis: duration_millis(started.elapsed()),
                returned_rows: 1,
                affected_rows: 0,
                row_limit_reached: value_rows
                    >= u64::try_from(row_limit).unwrap_or(u64::MAX),
            });
        }))
    }

    async fn list_objects(
        &self,
        request: ObjectListRequest,
        cancellation: CancellationToken,
    ) -> Result<ObjectPage, DriverError> {
        request
            .validate()
            .map_err(|error| DriverError::Query(error.to_string()))?;
        let parent = request.parent.unwrap_or_default();
        if !parent.segments.is_empty() {
            return Ok(ObjectPage {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        let cursor = request
            .cursor
            .as_deref()
            .unwrap_or("0")
            .parse::<u64>()
            .map_err(|_| DriverError::Query("Redis SCAN cursor is invalid".to_owned()))?;
        let deadline = operation_deadline(request.timeout)?;
        let mut connection = self.connection.clone();
        let mut scan = redis::cmd("SCAN");
        scan.arg(cursor).arg("COUNT").arg(request.limit);
        let (next_cursor, mut keys) = controlled(
            deadline,
            &cancellation,
            scan.query_async::<(u64, Vec<Vec<u8>>)>(&mut connection),
        )
        .await?
        .map_err(map_query_error)?;
        keys.truncate(request.limit);

        let mut pipeline = redis::pipe();
        for key in &keys {
            pipeline.cmd("TYPE").arg(key);
            pipeline.cmd("PTTL").arg(key);
        }
        let metadata = if keys.is_empty() {
            Vec::new()
        } else {
            let value = controlled(
                deadline,
                &cancellation,
                pipeline.query_async::<Value>(&mut connection),
            )
            .await?
            .map_err(map_query_error)?;
            match value {
                Value::Array(values) => values,
                _ => Vec::new(),
            }
        };

        let items = keys
            .into_iter()
            .enumerate()
            .map(|(index, key)| redis_key_object(&parent, key, &metadata, index))
            .collect();
        Ok(ObjectPage {
            items,
            next_cursor: (next_cursor != 0).then(|| next_cursor.to_string()),
        })
    }

    async fn slow_queries(
        &self,
        request: SlowQueryRequest,
        cancellation: CancellationToken,
    ) -> Result<SlowQueryPage, DriverError> {
        request
            .validate()
            .map_err(|error| DriverError::Query(error.to_string()))?;
        let deadline = operation_deadline(request.timeout)?;
        let mut connection = self.connection.clone();
        let mut command = redis::cmd("SLOWLOG");
        command.arg("GET").arg(request.limit);
        let value = controlled(
            deadline,
            &cancellation,
            command.query_async::<Value>(&mut connection),
        )
        .await?
        .map_err(map_query_error)?;
        let mut entries = match value {
            Value::Array(entries) => entries
                .iter()
                .filter_map(parse_slowlog_entry)
                .filter(|entry| {
                    request
                        .minimum_mean_time_millis
                        .is_none_or(|minimum| entry.mean_time_millis >= minimum)
                })
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        };
        match request.order {
            SlowQueryOrder::MeanTime | SlowQueryOrder::TotalTime => entries.sort_by(|left, right| {
                right
                    .mean_time_millis
                    .total_cmp(&left.mean_time_millis)
            }),
            SlowQueryOrder::Calls => {
                entries.sort_by_key(|entry| std::cmp::Reverse(entry.calls));
            }
        }
        entries.truncate(request.limit);
        Ok(SlowQueryPage {
            source: "SLOWLOG".to_owned(),
            entries,
        })
    }

    async fn close(&self) -> Result<(), DriverError> {
        Ok(())
    }
}

async fn controlled<T, F>(
    deadline: Instant,
    cancellation: &CancellationToken,
    future: F,
) -> Result<T, DriverError>
where
    F: Future<Output = T>,
{
    tokio::select! {
        _ = cancellation.cancelled() => Err(DriverError::Cancelled),
        result = tokio::time::timeout_at(deadline, future) => {
            result.map_err(|_| DriverError::Timeout)
        }
    }
}

fn operation_deadline(duration: Duration) -> Result<Instant, DriverError> {
    Instant::now()
        .checked_add(duration)
        .ok_or_else(|| DriverError::Query("operation timeout is too large".to_owned()))
}

fn parse_command(text: &str) -> Result<Vec<String>, DriverError> {
    let arguments = shell_words::split(text)
        .map_err(|_| DriverError::Query("Redis command contains an unmatched quote".to_owned()))?;
    if arguments.is_empty() {
        return Err(DriverError::Query("Redis command is empty".to_owned()));
    }
    Ok(arguments)
}

fn redis_key_object(
    parent: &ObjectPath,
    key: Vec<u8>,
    metadata: &[Value],
    index: usize,
) -> DatabaseObject {
    let name = String::from_utf8_lossy(&key).into_owned();
    let value_type = metadata
        .get(index.saturating_mul(2))
        .and_then(value_as_text)
        .unwrap_or_else(|| "unknown".to_owned());
    let ttl_millis = metadata
        .get(index.saturating_mul(2).saturating_add(1))
        .and_then(value_as_i64);
    let mut properties = BTreeMap::new();
    properties.insert(
        "value_type".to_owned(),
        serde_json::Value::String(value_type),
    );
    if let Some(ttl_millis) = ttl_millis.filter(|ttl| *ttl >= 0) {
        properties.insert(
            "ttl_millis".to_owned(),
            serde_json::Value::from(ttl_millis),
        );
    }

    DatabaseObject {
        id: format!("redis:key:{}", encode_hex(&key)),
        path: parent.child(name.clone()),
        name,
        kind: DatabaseObjectKind::Key,
        has_children: false,
        properties,
    }
}

fn parse_slowlog_entry(value: &Value) -> Option<SlowQueryEntry> {
    let Value::Array(fields) = value else {
        return None;
    };
    let id = fields.first().and_then(value_as_i64)?;
    let timestamp = fields.get(1).and_then(value_as_i64)?;
    let duration_micros = fields.get(2).and_then(value_as_i64)?;
    let query = fields
        .get(3)
        .and_then(value_as_array)
        .map(|arguments| {
            arguments
                .iter()
                .map(value_display)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default();
    let mut metadata = BTreeMap::new();
    metadata.insert(
        "timestamp_unix".to_owned(),
        serde_json::Value::from(timestamp),
    );
    if let Some(client_address) = fields.get(4).and_then(value_as_text) {
        metadata.insert(
            "client_address".to_owned(),
            serde_json::Value::String(client_address),
        );
    }
    if let Some(client_name) = fields.get(5).and_then(value_as_text) {
        metadata.insert(
            "client_name".to_owned(),
            serde_json::Value::String(client_name),
        );
    }
    let duration_millis = duration_micros as f64 / 1_000.0;

    Some(SlowQueryEntry {
        fingerprint: Some(id.to_string()),
        database: None,
        user: None,
        query,
        calls: 1,
        total_time_millis: duration_millis,
        mean_time_millis: duration_millis,
        max_time_millis: Some(duration_millis),
        rows: 0,
        metadata,
    })
}

fn value_to_json(value: Value, limit: usize, depth: usize) -> serde_json::Value {
    if depth >= MAX_VALUE_DEPTH {
        return serde_json::Value::String("<maximum nesting depth reached>".to_owned());
    }
    match value {
        Value::Nil => serde_json::Value::Null,
        Value::Int(value) => serde_json::Value::from(value),
        Value::BulkString(value) => bytes_to_json(value),
        Value::Array(values) | Value::Set(values) => serde_json::Value::Array(
            values
                .into_iter()
                .take(limit)
                .map(|value| value_to_json(value, limit, depth + 1))
                .collect(),
        ),
        Value::SimpleString(value) => serde_json::Value::String(value),
        Value::Okay => serde_json::Value::String("OK".to_owned()),
        Value::Map(values) => serde_json::Value::Array(
            values
                .into_iter()
                .take(limit)
                .map(|(key, value)| {
                    serde_json::json!({
                        "key": value_to_json(key, limit, depth + 1),
                        "value": value_to_json(value, limit, depth + 1),
                    })
                })
                .collect(),
        ),
        Value::Attribute { data, attributes } => serde_json::json!({
            "data": value_to_json(*data, limit, depth + 1),
            "attributes": attributes
                .into_iter()
                .take(limit)
                .map(|(key, value)| serde_json::json!({
                    "key": value_to_json(key, limit, depth + 1),
                    "value": value_to_json(value, limit, depth + 1),
                }))
                .collect::<Vec<_>>(),
        }),
        Value::Double(value) => serde_json::Number::from_f64(value)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::Boolean(value) => serde_json::Value::Bool(value),
        Value::VerbatimString { format, text } => serde_json::json!({
            "format": format!("{format:?}"),
            "text": text,
        }),
        Value::BigNumber(value) => bytes_to_json(value),
        Value::Push { kind, data } => serde_json::json!({
            "kind": format!("{kind:?}"),
            "data": data
                .into_iter()
                .take(limit)
                .map(|value| value_to_json(value, limit, depth + 1))
                .collect::<Vec<_>>(),
        }),
        Value::ServerError(error) => serde_json::json!({
            "server_error": error.to_string(),
        }),
        _ => serde_json::Value::String("<unsupported RESP value>".to_owned()),
    }
}

fn bytes_to_json(bytes: Vec<u8>) -> serde_json::Value {
    match String::from_utf8(bytes) {
        Ok(text) => serde_json::Value::String(text),
        Err(error) => serde_json::json!({
            "binary_hex": encode_hex(error.as_bytes()),
        }),
    }
}

fn value_row_count(value: &Value, limit: usize) -> u64 {
    let count = match value {
        Value::Array(values) | Value::Set(values) => values.len().min(limit),
        Value::Map(values) => values.len().min(limit),
        _ => 1,
    };
    u64::try_from(count).unwrap_or(u64::MAX)
}

fn value_as_array(value: &Value) -> Option<&[Value]> {
    match value {
        Value::Array(values) => Some(values),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Int(value) => Some(*value),
        Value::BulkString(value) => std::str::from_utf8(value).ok()?.parse().ok(),
        Value::SimpleString(value) => value.parse().ok(),
        _ => None,
    }
}

fn value_as_text(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(value) => Some(String::from_utf8_lossy(value).into_owned()),
        Value::SimpleString(value) => Some(value.clone()),
        Value::Int(value) => Some(value.to_string()),
        _ => None,
    }
}

fn value_display(value: &Value) -> String {
    value_as_text(value).unwrap_or_else(|| format!("{value:?}"))
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn map_connect_error(error: RedisError) -> DriverError {
    match error.kind() {
        ErrorKind::AuthenticationFailed => {
            DriverError::Authentication("Redis rejected the credentials".to_owned())
        }
        _ => DriverError::Connection("could not establish the Redis connection".to_owned()),
    }
}

fn map_query_error(error: RedisError) -> DriverError {
    match error.kind() {
        ErrorKind::AuthenticationFailed => {
            DriverError::Authentication("Redis authentication failed".to_owned())
        }
        ErrorKind::Server(redis::ServerErrorKind::NoPerm) => {
            DriverError::Permission("Redis ACL denied the command".to_owned())
        }
        ErrorKind::Io => DriverError::Connection("Redis connection was interrupted".to_owned()),
        _ => DriverError::Query(error.to_string()),
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{parse_command, value_to_json};
    use redis::Value;

    #[test]
    fn command_parser_preserves_quoted_arguments() {
        assert_eq!(
            parse_command("SET greeting 'hello world'").expect("command should parse"),
            vec!["SET", "greeting", "hello world"]
        );
        assert!(parse_command("SET key 'unfinished").is_err());
    }

    #[test]
    fn resp_values_are_bounded_and_binary_safe() {
        let value = Value::Array(vec![
            Value::BulkString(vec![0xff]),
            Value::Int(2),
            Value::Int(3),
        ]);
        let json = value_to_json(value, 2, 0);
        let values = json.as_array().expect("array should stay an array");
        assert_eq!(values.len(), 2);
        assert_eq!(values[0]["binary_hex"], "ff");
    }
}
