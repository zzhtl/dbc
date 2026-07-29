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
    diagnostics::{
        ExecutionPlan, ExplainMode, ExplainRequest, PlanFormat, SlowQueryEntry, SlowQueryOrder,
        SlowQueryPage, SlowQueryRequest,
    },
    driver::{
        ConnectionProfile, DatabaseSession, DriverDescriptor, DriverFactory, QueryEvent,
        QueryStats, QueryStream, SecretValue,
    },
    error::DriverError,
    metadata::{
        DatabaseObject, DatabaseObjectKind, ObjectListRequest, ObjectPage,
    },
    query::QueryRequest,
};
use dbc_data::{DataBatch, DataSchema};
use futures_util::TryStreamExt;
use mongodb::{
    Client,
    bson::{Bson, Document, doc},
    error::{Error as MongoError, ErrorKind},
    options::{ClientOptions, Credential},
};
use serde::{Deserialize, Serialize};
use tokio::time::{Instant, timeout};
use tokio_util::sync::CancellationToken;

use crate::mongo_descriptor;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const DOCUMENT_BATCH_SIZE: usize = 512;

/// JSON operation envelope accepted by [`MongoSession`](DatabaseSession).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "camelCase")]
pub enum MongoOperation {
    Find {
        database: Option<String>,
        collection: String,
        #[serde(default)]
        filter: Document,
        projection: Option<Document>,
        sort: Option<Document>,
        skip: Option<u64>,
        limit: Option<u64>,
    },
    Aggregate {
        database: Option<String>,
        collection: String,
        pipeline: Vec<Document>,
    },
    InsertOne {
        database: Option<String>,
        collection: String,
        document: Document,
    },
    UpdateOne {
        database: Option<String>,
        collection: String,
        #[serde(default)]
        filter: Document,
        update: Document,
        #[serde(default)]
        upsert: bool,
    },
    UpdateMany {
        database: Option<String>,
        collection: String,
        #[serde(default)]
        filter: Document,
        update: Document,
        #[serde(default)]
        upsert: bool,
    },
    DeleteOne {
        database: Option<String>,
        collection: String,
        #[serde(default)]
        filter: Document,
    },
    DeleteMany {
        database: Option<String>,
        collection: String,
        #[serde(default)]
        filter: Document,
    },
    RunCommand {
        database: Option<String>,
        command: Document,
    },
}

impl MongoOperation {
    fn returns_documents(&self) -> bool {
        !matches!(self, Self::DeleteOne { .. } | Self::DeleteMany { .. })
    }
}

/// Creates native MongoDB sessions using the official async driver.
#[derive(Debug, Clone)]
pub struct MongoFactory {
    descriptor: DriverDescriptor,
}

impl MongoFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {
            descriptor: mongo_descriptor(),
        }
    }
}

impl Default for MongoFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DriverFactory for MongoFactory {
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
                "connection profile does not target MongoDB".to_owned(),
            ));
        }
        let endpoint = profile.endpoint.trim();
        if !endpoint.starts_with("mongodb://") && !endpoint.starts_with("mongodb+srv://") {
            return Err(DriverError::Connection(
                "MongoDB endpoint must use mongodb:// or mongodb+srv://".to_owned(),
            ));
        }

        let mut options = timeout(CONNECT_TIMEOUT, ClientOptions::parse(endpoint))
            .await
            .map_err(|_| DriverError::Timeout)?
            .map_err(|_| DriverError::Connection("MongoDB endpoint is not valid".to_owned()))?;
        options.app_name = Some("dbc".to_owned());
        options.max_pool_size = Some(4);
        options.server_selection_timeout = Some(CONNECT_TIMEOUT);
        if profile.user.is_some() || secret.is_some() {
            let credential = options
                .credential
                .get_or_insert_with(Credential::default);
            if let Some(user) = profile.user.as_deref() {
                credential.username = Some(user.to_owned());
            }
            if let Some(secret) = secret {
                credential.password = Some(secret.expose().to_owned());
            }
        }
        let default_database = profile
            .database
            .clone()
            .or_else(|| options.default_database.clone());
        let client = Client::with_options(options).map_err(map_connect_error)?;
        let ping_database = default_database.as_deref().unwrap_or("admin");
        timeout(
            CONNECT_TIMEOUT,
            client
                .database(ping_database)
                .run_command(doc! { "ping": 1 }),
        )
        .await
        .map_err(|_| DriverError::Timeout)?
        .map_err(map_connect_error)?;

        Ok(Arc::new(MongoSession {
            client,
            default_database,
        }))
    }
}

#[derive(Debug)]
struct MongoSession {
    client: Client,
    default_database: Option<String>,
}

#[async_trait]
impl DatabaseSession for MongoSession {
    async fn execute(
        &self,
        request: QueryRequest,
        cancellation: CancellationToken,
    ) -> Result<QueryStream, DriverError> {
        request
            .validate()
            .map_err(|error| DriverError::Query(error.to_string()))?;
        if request.language != QueryLanguage::MongoQuery {
            return Err(DriverError::Unsupported(
                "MongoDB accepts MongoDB JSON operations only".to_owned(),
            ));
        }
        let operation = parse_operation(&request.text)?;
        validate_operation(&operation, self.default_database.as_deref())?;
        let client = self.client.clone();
        let default_database = self.default_database.clone();
        let deadline = operation_deadline(request.timeout)?;
        let timeout_duration = request.timeout;
        let row_limit = request.row_limit;
        let returns_documents = operation.returns_documents();

        Ok(Box::pin(try_stream! {
            let started = Instant::now();
            let mut returned_rows = 0_u64;
            let mut affected_rows = 0_u64;
            if returns_documents {
                yield QueryEvent::Schema(DataSchema::Documents);
            }

            match operation {
                MongoOperation::Find {
                    database,
                    collection,
                    filter,
                    projection,
                    sort,
                    skip,
                    limit,
                } => {
                    let database = selected_database(database.as_deref(), default_database.as_deref())?;
                    let collection = client.database(database).collection::<Document>(&collection);
                    let limit = limit
                        .unwrap_or(usize_to_u64(row_limit))
                        .min(usize_to_u64(row_limit));
                    let limit = i64::try_from(limit)
                        .map_err(|_| DriverError::Query("MongoDB find limit is too large".to_owned()))?;
                    let batch_size = u32::try_from(row_limit.min(DOCUMENT_BATCH_SIZE))
                        .map_err(|_| DriverError::Query("MongoDB batch size is too large".to_owned()))?;
                    let mut find = collection
                        .find(filter)
                        .limit(limit)
                        .batch_size(batch_size)
                        .max_time(timeout_duration);
                    if let Some(projection) = projection {
                        find = find.projection(projection);
                    }
                    if let Some(sort) = sort {
                        find = find.sort(sort);
                    }
                    if let Some(skip) = skip {
                        find = find.skip(skip);
                    }
                    let mut cursor = controlled(deadline, &cancellation, async { find.await })
                        .await?
                        .map_err(map_query_error)?;
                    let mut documents = Vec::with_capacity(DOCUMENT_BATCH_SIZE.min(row_limit));
                    while returned_rows < usize_to_u64(row_limit) {
                        let next = controlled(deadline, &cancellation, cursor.try_next())
                            .await?
                            .map_err(map_query_error)?;
                        let Some(document) = next else {
                            break;
                        };
                        documents.push(document_to_json(document)?);
                        returned_rows = returned_rows.saturating_add(1);
                        if documents.len() >= DOCUMENT_BATCH_SIZE {
                            yield QueryEvent::Rows(DataBatch::Documents(std::mem::take(&mut documents)));
                        }
                    }
                    if !documents.is_empty() {
                        yield QueryEvent::Rows(DataBatch::Documents(documents));
                    }
                }
                MongoOperation::Aggregate {
                    database,
                    collection,
                    pipeline,
                } => {
                    let database = selected_database(database.as_deref(), default_database.as_deref())?;
                    let collection = client.database(database).collection::<Document>(&collection);
                    let batch_size = u32::try_from(row_limit.min(DOCUMENT_BATCH_SIZE))
                        .map_err(|_| DriverError::Query("MongoDB batch size is too large".to_owned()))?;
                    let aggregate = collection
                        .aggregate(pipeline)
                        .batch_size(batch_size)
                        .max_time(timeout_duration);
                    let mut cursor = controlled(deadline, &cancellation, async { aggregate.await })
                        .await?
                        .map_err(map_query_error)?;
                    let mut documents = Vec::with_capacity(DOCUMENT_BATCH_SIZE.min(row_limit));
                    while returned_rows < usize_to_u64(row_limit) {
                        let next = controlled(deadline, &cancellation, cursor.try_next())
                            .await?
                            .map_err(map_query_error)?;
                        let Some(document) = next else {
                            break;
                        };
                        documents.push(document_to_json(document)?);
                        returned_rows = returned_rows.saturating_add(1);
                        if documents.len() >= DOCUMENT_BATCH_SIZE {
                            yield QueryEvent::Rows(DataBatch::Documents(std::mem::take(&mut documents)));
                        }
                    }
                    if !documents.is_empty() {
                        yield QueryEvent::Rows(DataBatch::Documents(documents));
                    }
                }
                MongoOperation::InsertOne {
                    database,
                    collection,
                    document,
                } => {
                    let database = selected_database(database.as_deref(), default_database.as_deref())?;
                    let collection = client.database(database).collection::<Document>(&collection);
                    let result = controlled(
                        deadline,
                        &cancellation,
                        async { collection.insert_one(document).await },
                    )
                    .await?
                    .map_err(map_query_error)?;
                    returned_rows = 1;
                    affected_rows = 1;
                    let inserted_id = bson_to_json(result.inserted_id)?;
                    yield QueryEvent::Rows(DataBatch::Documents(vec![serde_json::json!({
                        "insertedId": inserted_id,
                    })]));
                    yield QueryEvent::AffectedRows(1);
                }
                MongoOperation::UpdateOne {
                    database,
                    collection,
                    filter,
                    update,
                    upsert,
                } => {
                    let database = selected_database(database.as_deref(), default_database.as_deref())?;
                    let collection = client.database(database).collection::<Document>(&collection);
                    let result = controlled(
                        deadline,
                        &cancellation,
                        async { collection.update_one(filter, update).upsert(upsert).await },
                    )
                    .await?
                    .map_err(map_query_error)?;
                    returned_rows = 1;
                    affected_rows = result.modified_count;
                    let upserted_id = result.upserted_id.map(bson_to_json).transpose()?;
                    yield QueryEvent::Rows(DataBatch::Documents(vec![serde_json::json!({
                        "matchedCount": result.matched_count,
                        "modifiedCount": result.modified_count,
                        "upsertedId": upserted_id,
                    })]));
                    yield QueryEvent::AffectedRows(result.modified_count);
                }
                MongoOperation::UpdateMany {
                    database,
                    collection,
                    filter,
                    update,
                    upsert,
                } => {
                    let database = selected_database(database.as_deref(), default_database.as_deref())?;
                    let collection = client.database(database).collection::<Document>(&collection);
                    let result = controlled(
                        deadline,
                        &cancellation,
                        async { collection.update_many(filter, update).upsert(upsert).await },
                    )
                    .await?
                    .map_err(map_query_error)?;
                    returned_rows = 1;
                    affected_rows = result.modified_count;
                    let upserted_id = result.upserted_id.map(bson_to_json).transpose()?;
                    yield QueryEvent::Rows(DataBatch::Documents(vec![serde_json::json!({
                        "matchedCount": result.matched_count,
                        "modifiedCount": result.modified_count,
                        "upsertedId": upserted_id,
                    })]));
                    yield QueryEvent::AffectedRows(result.modified_count);
                }
                MongoOperation::DeleteOne {
                    database,
                    collection,
                    filter,
                } => {
                    let database = selected_database(database.as_deref(), default_database.as_deref())?;
                    let collection = client.database(database).collection::<Document>(&collection);
                    let result = controlled(
                        deadline,
                        &cancellation,
                        async { collection.delete_one(filter).await },
                    )
                    .await?
                    .map_err(map_query_error)?;
                    affected_rows = result.deleted_count;
                    yield QueryEvent::AffectedRows(result.deleted_count);
                }
                MongoOperation::DeleteMany {
                    database,
                    collection,
                    filter,
                } => {
                    let database = selected_database(database.as_deref(), default_database.as_deref())?;
                    let collection = client.database(database).collection::<Document>(&collection);
                    let result = controlled(
                        deadline,
                        &cancellation,
                        async { collection.delete_many(filter).await },
                    )
                    .await?
                    .map_err(map_query_error)?;
                    affected_rows = result.deleted_count;
                    yield QueryEvent::AffectedRows(result.deleted_count);
                }
                MongoOperation::RunCommand { database, command } => {
                    let database = selected_database(database.as_deref(), default_database.as_deref())?;
                    let result = controlled(
                        deadline,
                        &cancellation,
                        async { client.database(database).run_command(command).await },
                    )
                    .await?
                    .map_err(map_query_error)?;
                    returned_rows = 1;
                    let document = document_to_json(result)?;
                    yield QueryEvent::Rows(DataBatch::Documents(vec![document]));
                }
            }

            yield QueryEvent::Finished(QueryStats {
                elapsed_millis: duration_millis(started.elapsed()),
                returned_rows,
                affected_rows,
                row_limit_reached: returned_rows >= usize_to_u64(row_limit),
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
        let deadline = operation_deadline(request.timeout)?;
        let offset = decode_cursor(request.cursor.as_deref())?;
        let parent = request.parent.unwrap_or_default();

        match parent.segments.as_slice() {
            [] => {
                let mut names = controlled(
                    deadline,
                    &cancellation,
                    async { self.client.list_database_names().await },
                )
                .await?
                .map_err(map_query_error)?;
                names.sort();
                let objects = names.into_iter().map(|name| DatabaseObject {
                    id: format!("mongodb:database:{name}"),
                    path: parent.child(name.clone()),
                    name,
                    kind: DatabaseObjectKind::Keyspace,
                    has_children: true,
                    properties: BTreeMap::new(),
                });
                paginate_objects(objects, request.limit, offset)
            }
            [database] => {
                let mut names = controlled(
                    deadline,
                    &cancellation,
                    async {
                        self.client
                            .database(database)
                            .list_collection_names()
                            .await
                    },
                )
                .await?
                .map_err(map_query_error)?;
                names.sort();
                let objects = names.into_iter().map(|name| DatabaseObject {
                    id: format!("mongodb:collection:{database}:{name}"),
                    path: parent.child(name.clone()),
                    name,
                    kind: DatabaseObjectKind::Collection,
                    has_children: true,
                    properties: BTreeMap::new(),
                });
                paginate_objects(objects, request.limit, offset)
            }
            [database, collection] => {
                let collection_name = collection.clone();
                let mongo_collection = self
                    .client
                    .database(database)
                    .collection::<Document>(collection);
                let mut cursor = controlled(
                    deadline,
                    &cancellation,
                    async { mongo_collection.list_indexes().await },
                )
                .await?
                .map_err(map_query_error)?;
                let mut objects = Vec::new();
                while let Some(index) = controlled(deadline, &cancellation, cursor.try_next())
                    .await?
                    .map_err(map_query_error)?
                {
                    let name = index
                        .options
                        .as_ref()
                        .and_then(|options| options.name.clone())
                        .unwrap_or_else(|| index_name_from_keys(&index.keys));
                    let mut properties = BTreeMap::new();
                    properties.insert("keys".to_owned(), document_to_json(index.keys)?);
                    if let Some(options) = index.options {
                        properties.insert(
                            "unique".to_owned(),
                            serde_json::Value::Bool(options.unique.unwrap_or(false)),
                        );
                        properties.insert(
                            "sparse".to_owned(),
                            serde_json::Value::Bool(options.sparse.unwrap_or(false)),
                        );
                    }
                    objects.push(DatabaseObject {
                        id: format!("mongodb:index:{database}:{collection_name}:{name}"),
                        path: parent.child(name.clone()),
                        name,
                        kind: DatabaseObjectKind::Index,
                        has_children: false,
                        properties,
                    });
                }
                paginate_objects(objects, request.limit, offset)
            }
            _ => Err(DriverError::Unsupported(
                "MongoDB object paths deeper than collection indexes".to_owned(),
            )),
        }
    }

    async fn explain(
        &self,
        request: ExplainRequest,
        cancellation: CancellationToken,
    ) -> Result<ExecutionPlan, DriverError> {
        request
            .validate()
            .map_err(|error| DriverError::Query(error.to_string()))?;
        let operation = parse_operation(&request.text)?;
        let command = explainable_command(operation, self.default_database.as_deref())?;
        let verbosity = match request.mode {
            ExplainMode::Estimated => "queryPlanner",
            ExplainMode::Analyze => "executionStats",
        };
        let database = command.0;
        let explain = doc! {
            "explain": command.1,
            "verbosity": verbosity,
        };
        let deadline = operation_deadline(request.timeout)?;
        let document = controlled(
            deadline,
            &cancellation,
            async { self.client.database(&database).run_command(explain).await },
        )
        .await?
        .map_err(map_query_error)?;

        Ok(ExecutionPlan {
            engine: "mongodb".to_owned(),
            format: PlanFormat::Json,
            analyzed: request.mode == ExplainMode::Analyze,
            document: document_to_json(document)?,
            metadata: BTreeMap::new(),
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
        let database = self.default_database.as_deref().ok_or_else(|| {
            DriverError::Unsupported(
                "MongoDB profiler queries require a default database".to_owned(),
            )
        })?;
        let deadline = operation_deadline(request.timeout)?;
        let minimum = request.minimum_mean_time_millis.unwrap_or(0.0);
        let limit = i64::try_from(request.limit)
            .map_err(|_| DriverError::Query("slow-query limit is too large".to_owned()))?;
        let collection = self
            .client
            .database(database)
            .collection::<Document>("system.profile");
        let mut cursor = controlled(
            deadline,
            &cancellation,
            async {
                collection
                    .find(doc! { "millis": { "$gte": minimum } })
                    .sort(doc! { "millis": -1 })
                    .limit(limit)
                    .max_time(request.timeout)
                    .await
            },
        )
        .await?
        .map_err(map_query_error)?;
        let mut entries = Vec::new();
        while let Some(document) = controlled(deadline, &cancellation, cursor.try_next())
            .await?
            .map_err(map_query_error)?
        {
            if let Some(entry) = profile_entry(document) {
                entries.push(entry);
            }
        }
        if request.order == SlowQueryOrder::Calls {
            entries.sort_by_key(|entry| std::cmp::Reverse(entry.calls));
        }
        Ok(SlowQueryPage {
            source: format!("{database}.system.profile"),
            entries,
        })
    }

    async fn close(&self) -> Result<(), DriverError> {
        self.client.clone().shutdown().await;
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

fn parse_operation(text: &str) -> Result<MongoOperation, DriverError> {
    serde_json::from_str(text)
        .map_err(|error| DriverError::Query(format!("invalid MongoDB operation JSON: {error}")))
}

fn validate_operation(
    operation: &MongoOperation,
    default_database: Option<&str>,
) -> Result<(), DriverError> {
    let (database, collection) = match operation {
        MongoOperation::Find {
            database,
            collection,
            ..
        }
        | MongoOperation::Aggregate {
            database,
            collection,
            ..
        }
        | MongoOperation::InsertOne {
            database,
            collection,
            ..
        }
        | MongoOperation::UpdateOne {
            database,
            collection,
            ..
        }
        | MongoOperation::UpdateMany {
            database,
            collection,
            ..
        }
        | MongoOperation::DeleteOne {
            database,
            collection,
            ..
        }
        | MongoOperation::DeleteMany {
            database,
            collection,
            ..
        } => (database.as_deref(), Some(collection.as_str())),
        MongoOperation::RunCommand { database, .. } => (database.as_deref(), None),
    };
    selected_database(database, default_database)?;
    if collection.is_some_and(str::is_empty) {
        return Err(DriverError::Query(
            "MongoDB collection name cannot be empty".to_owned(),
        ));
    }
    Ok(())
}

fn selected_database<'a>(
    explicit: Option<&'a str>,
    default: Option<&'a str>,
) -> Result<&'a str, DriverError> {
    explicit
        .filter(|name| !name.is_empty())
        .or(default.filter(|name| !name.is_empty()))
        .ok_or_else(|| {
            DriverError::Query(
                "MongoDB operation requires a database in the profile or operation".to_owned(),
            )
        })
}

fn explainable_command(
    operation: MongoOperation,
    default_database: Option<&str>,
) -> Result<(String, Document), DriverError> {
    match operation {
        MongoOperation::Find {
            database,
            collection,
            filter,
            projection,
            sort,
            skip,
            limit,
        } => {
            let database = selected_database(database.as_deref(), default_database)?.to_owned();
            let mut command = doc! { "find": collection, "filter": filter };
            if let Some(projection) = projection {
                command.insert("projection", projection);
            }
            if let Some(sort) = sort {
                command.insert("sort", sort);
            }
            if let Some(skip) = skip {
                command.insert("skip", i64::try_from(skip).map_err(|_| {
                    DriverError::Query("MongoDB skip is too large".to_owned())
                })?);
            }
            if let Some(limit) = limit {
                command.insert("limit", i64::try_from(limit).map_err(|_| {
                    DriverError::Query("MongoDB limit is too large".to_owned())
                })?);
            }
            Ok((database, command))
        }
        MongoOperation::Aggregate {
            database,
            collection,
            pipeline,
        } => {
            let database = selected_database(database.as_deref(), default_database)?.to_owned();
            Ok((
                database,
                doc! {
                    "aggregate": collection,
                    "pipeline": pipeline,
                    "cursor": {},
                },
            ))
        }
        _ => Err(DriverError::Unsupported(
            "MongoDB execution plans currently support find and aggregate operations".to_owned(),
        )),
    }
}

fn paginate_objects<I>(
    objects: I,
    limit: usize,
    offset: usize,
) -> Result<ObjectPage, DriverError>
where
    I: IntoIterator<Item = DatabaseObject>,
{
    let mut objects = objects
        .into_iter()
        .skip(offset)
        .take(limit.saturating_add(1))
        .collect::<Vec<_>>();
    let has_next_page = objects.len() > limit;
    objects.truncate(limit);
    let next_cursor = if has_next_page {
        Some(
            offset
                .checked_add(limit)
                .ok_or_else(|| DriverError::Query("object page cursor overflowed".to_owned()))?
                .to_string(),
        )
    } else {
        None
    };
    Ok(ObjectPage {
        items: objects,
        next_cursor,
    })
}

fn decode_cursor(cursor: Option<&str>) -> Result<usize, DriverError> {
    cursor
        .map(str::parse)
        .transpose()
        .map(|offset| offset.unwrap_or_default())
        .map_err(|_| DriverError::Query("object page cursor is invalid".to_owned()))
}

fn index_name_from_keys(keys: &Document) -> String {
    keys.iter()
        .map(|(field, direction)| format!("{field}_{direction}"))
        .collect::<Vec<_>>()
        .join("_")
}

fn profile_entry(document: Document) -> Option<SlowQueryEntry> {
    let duration = document_number(&document, "millis")?;
    let fingerprint = document
        .get_str("queryHash")
        .or_else(|_| document.get_str("planCacheShapeHash"))
        .ok()
        .map(ToOwned::to_owned);
    let namespace = document.get_str("ns").ok().map(ToOwned::to_owned);
    let operation = document.get_str("op").unwrap_or("command");
    let query = document
        .get_document("command")
        .ok()
        .and_then(|command| serde_json::to_string(&document_to_json(command.clone()).ok()?).ok())
        .unwrap_or_else(|| operation.to_owned());
    let mut metadata = BTreeMap::new();
    if let Some(namespace) = namespace {
        metadata.insert(
            "namespace".to_owned(),
            serde_json::Value::String(namespace),
        );
    }
    for field in ["docsExamined", "keysExamined", "numYield"] {
        if let Some(value) = document_number(&document, field) {
            metadata.insert(field.to_owned(), serde_json::json!(value));
        }
    }

    Some(SlowQueryEntry {
        fingerprint,
        database: document
            .get_str("ns")
            .ok()
            .and_then(|namespace| namespace.split('.').next())
            .map(ToOwned::to_owned),
        user: None,
        query,
        calls: 1,
        total_time_millis: duration,
        mean_time_millis: duration,
        max_time_millis: Some(duration),
        rows: document_number(&document, "nreturned")
            .and_then(|value| u64::try_from(value as i64).ok())
            .unwrap_or_default(),
        metadata,
    })
}

fn document_number(document: &Document, field: &str) -> Option<f64> {
    match document.get(field)? {
        Bson::Int32(value) => Some(f64::from(*value)),
        Bson::Int64(value) => Some(*value as f64),
        Bson::Double(value) => Some(*value),
        _ => None,
    }
}

fn document_to_json(document: Document) -> Result<serde_json::Value, DriverError> {
    serde_json::to_value(document)
        .map_err(|_| DriverError::Internal("could not serialize a MongoDB document".to_owned()))
}

fn bson_to_json(value: Bson) -> Result<serde_json::Value, DriverError> {
    serde_json::to_value(value)
        .map_err(|_| DriverError::Internal("could not serialize a MongoDB value".to_owned()))
}

fn map_connect_error(error: MongoError) -> DriverError {
    match error.kind.as_ref() {
        ErrorKind::Authentication { .. } => {
            DriverError::Authentication("MongoDB rejected the credentials".to_owned())
        }
        _ => DriverError::Connection("could not establish the MongoDB connection".to_owned()),
    }
}

fn map_query_error(error: MongoError) -> DriverError {
    match error.kind.as_ref() {
        ErrorKind::Authentication { .. } => {
            DriverError::Authentication("MongoDB authentication failed".to_owned())
        }
        ErrorKind::Command(command) if command.code == 13 => {
            DriverError::Permission("MongoDB denied the operation".to_owned())
        }
        ErrorKind::Command(command) => DriverError::Query(format!(
            "MongoDB command failed [{} {}]",
            command.code, command.code_name
        )),
        ErrorKind::ServerSelection { .. }
        | ErrorKind::Io(_)
        | ErrorKind::ConnectionPoolCleared { .. } => {
            DriverError::Connection("MongoDB connection was interrupted".to_owned())
        }
        _ => DriverError::Internal("MongoDB operation failed".to_owned()),
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{MongoOperation, index_name_from_keys, parse_operation};
    use mongodb::bson::doc;

    #[test]
    fn operation_envelope_parses_extended_json_documents() {
        let operation = parse_operation(
            r#"{
                "operation": "find",
                "database": "app",
                "collection": "users",
                "filter": {"active": true},
                "projection": null,
                "sort": {"created_at": -1},
                "skip": 0,
                "limit": 25
            }"#,
        )
        .expect("find operation should parse");
        assert!(matches!(operation, MongoOperation::Find { .. }));
    }

    #[test]
    fn index_names_can_be_derived_when_the_server_omits_one() {
        assert_eq!(
            index_name_from_keys(&doc! { "email": 1, "created_at": -1 }),
            "email_1_created_at_-1"
        );
    }
}
