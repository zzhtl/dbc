use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context as _, Result};
use dbc_core::{
    capability::{Capability, QueryLanguage},
    diagnostics::{
        ExecutionPlan, ExplainMode, ExplainRequest, SlowQueryOrder, SlowQueryPage, SlowQueryRequest,
    },
    driver::{
        ConnectionProfile, DatabaseSession, DriverRegistry, QueryEvent, QueryStats, SecretValue,
    },
    error::DriverError,
    metadata::{DatabaseObject, DatabaseObjectKind, ObjectListRequest, ObjectPage, ObjectPath},
    query::QueryRequest,
    query_editability::{
        QueryEditabilityReason, QuerySourceAnalysis, analyze_query_source,
        resolve_query_editability,
    },
    sql::{SqlDialect, StatementRisk, classify_sql},
    table_data::{
        FilterOperator, SortDirection, TableApplyResult, TableBrowseRequest, TableChangePlan,
        TableFilter, TableMetadata, TablePage, TableRef, TableSort,
    },
};
use dbc_data::{BufferLimits, CellValue, DataSchema, ResultBuffer};
use dbc_drivers::builtin_factories;
use dbc_runtime::{RuntimeConfig, TaskError, TaskRuntime};
use eframe::egui;
use egui_code_editor::{CodeEditor, ColorTheme};
use futures_util::TryStreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    drivers::{DRIVER_CHOICES, DriverChoice},
    export::{
        ExportError, ExportFormat, ExportLimits, ExportSummary, FULL_EXPORT_QUERY_ROWS,
        export_buffer_cancellable, export_query,
    },
    result_table::{ResultGridState, ResultModel, tabular_cells},
    table_editor::{EditorOrigin, TableEditorState, input_cell_value},
};

const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const OBJECT_TIMEOUT: Duration = Duration::from_secs(10);
const FULL_EXPORT_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
const PAGE_SIZE_PRESETS: &[usize] = &[100, 200, 500, 1_000];
const ROW_LIMIT_PRESETS: &[usize] = &[10_000, 50_000, 100_000];
const MEMORY_LIMIT_PRESETS_MIB: &[usize] = &[64, 128, 256];
const DEFAULT_PAGE_SIZE: usize = 200;
const DEFAULT_ROW_LIMIT: usize = 10_000;
const DEFAULT_MEMORY_LIMIT_MIB: usize = 64;
const QUERY_EVENT_CHANNEL_CAPACITY: usize = 8;

type DriverTaskResult<T> = std::result::Result<T, TaskError<DriverError>>;
type ExportTaskResult = std::result::Result<ExportSummary, TaskError<ExportError>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResultTab {
    Data,
    Plan,
    SlowQueries,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteAction {
    Execute,
    Analyze,
    FullExport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExportScope {
    Current,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationKind {
    Query,
    Explain,
    SlowQueries,
    Export,
    TableLoad,
    TablePlan,
    TableApply,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingWriteConfirmation {
    action: WriteAction,
    text: String,
}

#[derive(Debug, Clone)]
struct ObjectRow {
    object: DatabaseObject,
    depth: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QuerySettings {
    page_size: usize,
    row_limit: usize,
    memory_limit_mib: usize,
}

impl Default for QuerySettings {
    fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            row_limit: DEFAULT_ROW_LIMIT,
            memory_limit_mib: DEFAULT_MEMORY_LIMIT_MIB,
        }
    }
}

impl QuerySettings {
    fn buffer_limits(self) -> BufferLimits {
        BufferLimits {
            max_rows: self.row_limit,
            max_bytes: self
                .memory_limit_mib
                .saturating_mul(1024)
                .saturating_mul(1024),
        }
    }
}

#[derive(Debug)]
pub(crate) struct BufferedQueryResult {
    pub(crate) schema: Option<DataSchema>,
    pub(crate) buffer: ResultBuffer,
    messages: Vec<String>,
    affected_rows: u64,
    stats: Option<QueryStats>,
}

impl BufferedQueryResult {
    fn new(limits: BufferLimits) -> Self {
        Self {
            schema: None,
            buffer: ResultBuffer::new(limits),
            messages: Vec::new(),
            affected_rows: 0,
            stats: None,
        }
    }

    fn apply(&mut self, event: QueryEvent) {
        match event {
            QueryEvent::Schema(schema) => {
                if self.schema.is_none() {
                    self.schema = Some(schema);
                }
            }
            QueryEvent::Rows(batch) => {
                if self.schema.is_none() {
                    self.schema = Some(DataSchema::from_batch(&batch));
                }
                let _outcome = self.buffer.append(batch);
            }
            QueryEvent::Message(message) => self.messages.push(message),
            QueryEvent::AffectedRows(rows) => {
                self.affected_rows = self.affected_rows.saturating_add(rows);
            }
            QueryEvent::Finished(stats) => self.stats = Some(stats),
        }
    }
}

#[derive(Debug, Clone)]
struct QuerySnapshot {
    operation_id: u64,
    language: QueryLanguage,
    text: String,
    source_analysis: Option<QuerySourceAnalysis>,
}

struct LoadedTable {
    metadata: TableMetadata,
    page: TablePage,
}

struct PreparedTableChange {
    editor_generation: u64,
    revision: u64,
    plan: TableChangePlan,
}

#[derive(Debug, Clone)]
struct TableBrowseControls {
    table: TableRef,
    filters: Vec<TableFilter>,
    raw_where: String,
    raw_order_by: String,
    sort_column: String,
    sort_direction: SortDirection,
    filter_column: String,
    filter_operator: FilterOperator,
    filter_value: String,
    second_filter_value: String,
}

impl TableBrowseControls {
    fn new(table: TableRef) -> Self {
        Self {
            table,
            filters: Vec::new(),
            raw_where: String::new(),
            raw_order_by: String::new(),
            sort_column: String::new(),
            sort_direction: SortDirection::Ascending,
            filter_column: String::new(),
            filter_operator: FilterOperator::Equals,
            filter_value: String::new(),
            second_filter_value: String::new(),
        }
    }
}

struct ActiveQueryEvents {
    session_generation: u64,
    operation_id: u64,
    receiver: mpsc::Receiver<QueryEvent>,
}

enum AppEvent {
    Connected {
        session_generation: u64,
        result: DriverTaskResult<Arc<dyn DatabaseSession>>,
    },
    ObjectsLoaded {
        session_generation: u64,
        key: String,
        parent: Option<ObjectPath>,
        depth: usize,
        result: DriverTaskResult<ObjectPage>,
    },
    QueryFinished {
        session_generation: u64,
        operation_id: u64,
        result: DriverTaskResult<()>,
    },
    QueryMetadataLoaded {
        session_generation: u64,
        operation_id: u64,
        analysis: QuerySourceAnalysis,
        result: DriverTaskResult<TableMetadata>,
    },
    ExplainFinished {
        session_generation: u64,
        operation_id: u64,
        result: DriverTaskResult<ExecutionPlan>,
    },
    SlowQueriesFinished {
        session_generation: u64,
        operation_id: u64,
        result: DriverTaskResult<SlowQueryPage>,
    },
    ExportFinished {
        session_generation: u64,
        operation_id: u64,
        path: PathBuf,
        result: ExportTaskResult,
    },
    TableLoaded {
        session_generation: u64,
        operation_id: u64,
        result: DriverTaskResult<LoadedTable>,
    },
    TablePlanReady {
        session_generation: u64,
        operation_id: u64,
        editor_generation: u64,
        revision: u64,
        result: DriverTaskResult<TableChangePlan>,
    },
    TableApplied {
        session_generation: u64,
        operation_id: u64,
        editor_generation: u64,
        result: DriverTaskResult<TableApplyResult>,
    },
}

#[derive(Debug, Clone, Copy)]
struct CapabilitySupport {
    explain: bool,
    analyze: bool,
    slow_queries: bool,
    crud: bool,
    table_data: bool,
}

pub struct DbcApp {
    registry: Arc<DriverRegistry>,
    runtime: Arc<TaskRuntime>,
    event_sender: mpsc::UnboundedSender<AppEvent>,
    event_receiver: mpsc::UnboundedReceiver<AppEvent>,
    query_events: Option<ActiveQueryEvents>,
    selected_driver: usize,
    endpoint: String,
    database: String,
    user: String,
    password: String,
    password_visible: bool,
    query_text: String,
    data_grid: ResultGridState,
    slow_grid: ResultGridState,
    session: Option<Arc<dyn DatabaseSession>>,
    objects: Vec<ObjectRow>,
    expanded_object_keys: BTreeSet<String>,
    loaded_object_keys: BTreeSet<String>,
    loading_object_keys: BTreeSet<String>,
    result_tab: ResultTab,
    plan_text: String,
    status: String,
    connection_busy: bool,
    operation_busy: bool,
    active_cancellation: Option<CancellationToken>,
    active_operation_id: Option<u64>,
    active_operation_kind: Option<OperationKind>,
    pending_write_confirmation: Option<PendingWriteConfirmation>,
    session_generation: u64,
    operation_generation: u64,
    query_settings: QuerySettings,
    active_result: Option<BufferedQueryResult>,
    current_result: Option<Arc<BufferedQueryResult>>,
    current_page: usize,
    last_query: Option<QuerySnapshot>,
    left_panel_open: bool,
    right_panel_open: bool,
    table_editor: Option<TableEditorState>,
    table_editor_generation: u64,
    table_browse: Option<TableBrowseControls>,
    prepared_table_change: Option<PreparedTableChange>,
    show_table_change_preview: bool,
    query_read_only_reason: Option<String>,
}

impl DbcApp {
    pub fn new() -> Result<Self> {
        let choice = DRIVER_CHOICES
            .first()
            .context("at least one built-in driver is required")?;
        let mut registry = DriverRegistry::new();
        for factory in builtin_factories() {
            registry
                .register(factory)
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        }
        let runtime = TaskRuntime::new(RuntimeConfig::default())
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        Ok(Self {
            registry: Arc::new(registry),
            runtime: Arc::new(runtime),
            event_sender,
            event_receiver,
            query_events: None,
            selected_driver: 0,
            endpoint: choice.endpoint.to_owned(),
            database: choice.database.to_owned(),
            user: choice.user.to_owned(),
            password: String::new(),
            password_visible: false,
            query_text: choice.query.to_owned(),
            data_grid: ResultGridState::new(ResultModel::message(
                "连接数据库后执行查询，结果将在这里显示",
            )),
            slow_grid: ResultGridState::new(ResultModel::message("点击“慢查询”读取数据库原生统计")),
            session: None,
            objects: Vec::new(),
            expanded_object_keys: BTreeSet::new(),
            loaded_object_keys: BTreeSet::new(),
            loading_object_keys: BTreeSet::new(),
            result_tab: ResultTab::Data,
            plan_text: "尚未生成执行计划".to_owned(),
            status: "请选择数据库并建立连接".to_owned(),
            connection_busy: false,
            operation_busy: false,
            active_cancellation: None,
            active_operation_id: None,
            active_operation_kind: None,
            pending_write_confirmation: None,
            session_generation: 0,
            operation_generation: 0,
            query_settings: QuerySettings::default(),
            active_result: None,
            current_result: None,
            current_page: 0,
            last_query: None,
            left_panel_open: true,
            right_panel_open: true,
            table_editor: None,
            table_editor_generation: 0,
            table_browse: None,
            prepared_table_change: None,
            show_table_change_preview: false,
            query_read_only_reason: None,
        })
    }

    fn choice(&self) -> DriverChoice {
        DRIVER_CHOICES[self.selected_driver]
    }

    fn capability_support(&self) -> CapabilitySupport {
        let Some(factory) = self.registry.get(self.choice().id) else {
            return CapabilitySupport {
                explain: false,
                analyze: false,
                slow_queries: false,
                crud: false,
                table_data: false,
            };
        };
        let capabilities = &factory.descriptor().capabilities;
        let analyze = capabilities
            .capabilities()
            .iter()
            .find_map(|capability| match capability {
                Capability::Explain(settings) => Some(settings.analyzed),
                _ => None,
            })
            .unwrap_or(false);
        CapabilitySupport {
            explain: capabilities.supports_explain(),
            analyze,
            slow_queries: capabilities.supports_slow_queries(),
            crud: capabilities.supports_crud(),
            table_data: capabilities.supports_table_data(),
        }
    }

    fn select_driver(&mut self, index: usize) {
        if self.connection_busy
            || self.operation_busy
            || index >= DRIVER_CHOICES.len()
            || index == self.selected_driver
            || !self.allow_pending_navigation("切换驱动")
        {
            return;
        }
        self.close_current_session();
        self.selected_driver = index;
        let choice = self.choice();
        self.endpoint = choice.endpoint.to_owned();
        self.database = choice.database.to_owned();
        self.user = choice.user.to_owned();
        self.password.clear();
        self.password_visible = false;
        self.query_text = choice.query.to_owned();
        self.reset_session_view();
        self.status = format!("已选择 {}，请检查连接参数", choice.name);
    }

    fn connect(&mut self, ctx: &egui::Context) {
        if self.connection_busy
            || self.operation_busy
            || !self.allow_pending_navigation("重新连接")
        {
            return;
        }
        let choice = self.choice();
        let Some(factory) = self.registry.get(choice.id) else {
            self.status = format!("驱动 {} 未注册", choice.id);
            return;
        };
        if self.endpoint.trim().is_empty() {
            self.status = "连接地址不能为空".to_owned();
            return;
        }
        let password = optional_input(&self.password);
        let profile = ConnectionProfile {
            id: Uuid::new_v4().to_string(),
            driver_id: choice.id.to_owned(),
            display_name: choice.name.to_owned(),
            endpoint: self.endpoint.trim().to_owned(),
            database: optional_input(&self.database),
            user: optional_input(&self.user),
            secret_id: password.as_ref().map(|_| "desktop:ephemeral".to_owned()),
        };

        self.close_current_session();
        self.reset_session_view();
        self.connection_busy = true;
        self.status = format!("正在连接 {}…", choice.name);
        let session_generation = self.session_generation;
        let event_sender = self.event_sender.clone();
        let repaint = ctx.clone();
        let _cancellation = self.runtime.spawn_reported(
            move |_| async move {
                let secret = password.map(SecretValue::new);
                factory.connect(&profile, secret.as_ref()).await
            },
            move |result| {
                let _sent = event_sender.send(AppEvent::Connected {
                    session_generation,
                    result,
                });
                repaint.request_repaint();
            },
        );
    }

    fn disconnect(&mut self) {
        if !self.allow_pending_navigation("断开连接") {
            return;
        }
        self.cancel_active_operation();
        self.close_current_session();
        self.connection_busy = false;
        self.reset_session_view();
        self.status = "连接已断开".to_owned();
    }

    fn execute(&mut self, ctx: &egui::Context) {
        if self.operation_busy || !self.allow_pending_navigation("执行新查询") {
            return;
        }
        let Some(session) = self.session.clone() else {
            self.status = "请先建立数据库连接".to_owned();
            return;
        };
        let text = self.query_text.clone();
        if text.trim().is_empty() {
            self.status = "查询内容不能为空".to_owned();
            return;
        }
        if !self.confirm_write(WriteAction::Execute, &text) {
            return;
        }

        let language = self.choice().language;
        let settings = self.query_settings;
        let source_analysis = (language == QueryLanguage::Sql)
            .then(|| analyze_query_source(&text, self.sql_dialect()));
        let request = QueryRequest::new(
            Uuid::new_v4(),
            language,
            text.clone(),
            OPERATION_TIMEOUT,
            settings.row_limit,
        );
        let (query_sender, query_receiver) = mpsc::channel(QUERY_EVENT_CHANNEL_CAPACITY);
        let session_generation = self.session_generation;
        let operation_id = self.next_operation_id();
        self.last_query = Some(QuerySnapshot {
            operation_id,
            language,
            text,
            source_analysis,
        });
        let event_sender = self.event_sender.clone();
        let stream_repaint = ctx.clone();
        let completion_repaint = ctx.clone();
        let cancellation = self.runtime.spawn_reported(
            move |cancellation| async move {
                let mut stream = session.execute(request, cancellation).await?;
                while let Some(event) = stream.try_next().await? {
                    if query_sender.send(event).await.is_err() {
                        break;
                    }
                    stream_repaint.request_repaint();
                }
                Ok::<(), DriverError>(())
            },
            move |result| {
                let _sent = event_sender.send(AppEvent::QueryFinished {
                    session_generation,
                    operation_id,
                    result,
                });
                completion_repaint.request_repaint();
            },
        );

        self.query_events = Some(ActiveQueryEvents {
            session_generation,
            operation_id,
            receiver: query_receiver,
        });
        self.begin_operation(operation_id, OperationKind::Query, cancellation);
        self.status = "正在执行…".to_owned();
        self.result_tab = ResultTab::Data;
        self.current_page = 0;
        self.current_result = None;
        self.active_result = Some(BufferedQueryResult::new(settings.buffer_limits()));
        self.table_editor = None;
        self.table_browse = None;
        self.invalidate_table_change_plan();
        self.query_read_only_reason = None;
        self.data_grid
            .replace(ResultModel::message("正在接收查询结果…"));
    }

    fn start_explain(&mut self, mode: ExplainMode, ctx: &egui::Context) {
        if self.operation_busy {
            return;
        }
        let Some(session) = self.session.clone() else {
            self.status = "请先建立数据库连接".to_owned();
            return;
        };
        let text = self.query_text.clone();
        if text.trim().is_empty() {
            self.status = "查询内容不能为空".to_owned();
            return;
        }
        if mode == ExplainMode::Analyze {
            if !self.confirm_write(WriteAction::Analyze, &text) {
                return;
            }
        } else {
            self.pending_write_confirmation = None;
        }
        let request = ExplainRequest {
            id: Uuid::new_v4(),
            text,
            mode,
            timeout: OPERATION_TIMEOUT,
        };
        let session_generation = self.session_generation;
        let operation_id = self.next_operation_id();
        let event_sender = self.event_sender.clone();
        let repaint = ctx.clone();
        let cancellation =
            self.runtime.spawn_reported(
                move |task_cancellation| async move {
                    session.explain(request, task_cancellation).await
                },
                move |result| {
                    let _sent = event_sender.send(AppEvent::ExplainFinished {
                        session_generation,
                        operation_id,
                        result,
                    });
                    repaint.request_repaint();
                },
            );
        self.begin_operation(operation_id, OperationKind::Explain, cancellation);
        self.result_tab = ResultTab::Plan;
        self.status = match mode {
            ExplainMode::Estimated => "正在生成执行计划…".to_owned(),
            ExplainMode::Analyze => "正在执行并分析查询…".to_owned(),
        };
    }

    fn load_slow_queries(&mut self, ctx: &egui::Context) {
        if self.operation_busy {
            return;
        }
        let Some(session) = self.session.clone() else {
            self.status = "请先建立数据库连接".to_owned();
            return;
        };
        let request = SlowQueryRequest {
            id: Uuid::new_v4(),
            limit: 100,
            minimum_mean_time_millis: Some(0.0),
            order: SlowQueryOrder::TotalTime,
            timeout: OPERATION_TIMEOUT,
        };
        let session_generation = self.session_generation;
        let operation_id = self.next_operation_id();
        let event_sender = self.event_sender.clone();
        let repaint = ctx.clone();
        let cancellation = self.runtime.spawn_reported(
            move |task_cancellation| async move {
                session.slow_queries(request, task_cancellation).await
            },
            move |result| {
                let _sent = event_sender.send(AppEvent::SlowQueriesFinished {
                    session_generation,
                    operation_id,
                    result,
                });
                repaint.request_repaint();
            },
        );
        self.begin_operation(operation_id, OperationKind::SlowQueries, cancellation);
        self.result_tab = ResultTab::SlowQueries;
        self.status = "正在读取慢查询统计…".to_owned();
    }

    fn request_export(&mut self, scope: ExportScope, format: ExportFormat, ctx: &egui::Context) {
        if self.operation_busy {
            return;
        }
        match scope {
            ExportScope::Current => {
                if self
                    .current_result
                    .as_deref()
                    .and_then(|result| result.schema.as_ref())
                    .is_none()
                {
                    self.status = "当前没有可导出的结果集".to_owned();
                    return;
                }
            }
            ExportScope::Full => {
                let Some(snapshot) = self.last_query.as_ref() else {
                    self.status = "没有可重新执行的查询".to_owned();
                    return;
                };
                if self.session.is_none() {
                    self.status = "完整导出需要保持数据库连接".to_owned();
                    return;
                }
                let text = snapshot.text.clone();
                if !self.confirm_write(WriteAction::FullExport, &text) {
                    return;
                }
            }
        }

        let suggested_name = format!(
            "{}.{}",
            match scope {
                ExportScope::Current => "dbc-current-result",
                ExportScope::Full => "dbc-full-result",
            },
            format.extension()
        );
        let selected = rfd::FileDialog::new()
            .add_filter(format.label(), &[format.extension()])
            .set_file_name(suggested_name)
            .save_file();
        let Some(path) = selected else {
            self.status = "已取消导出".to_owned();
            return;
        };
        self.start_export(scope, format, path, ctx);
    }

    fn start_export(
        &mut self,
        scope: ExportScope,
        format: ExportFormat,
        path: PathBuf,
        ctx: &egui::Context,
    ) {
        if self.operation_busy {
            return;
        }
        let session_generation = self.session_generation;
        let operation_id = self.next_operation_id();
        let path_for_event = path.clone();
        let event_sender = self.event_sender.clone();
        let repaint = ctx.clone();
        let report = move |result| {
            let _sent = event_sender.send(AppEvent::ExportFinished {
                session_generation,
                operation_id,
                path: path_for_event,
                result,
            });
            repaint.request_repaint();
        };

        let cancellation = match scope {
            ExportScope::Current => {
                let Some(result) = self.current_result.clone() else {
                    self.status = "当前结果已不可用".to_owned();
                    return;
                };
                self.runtime.spawn_reported(
                    move |task_cancellation| async move {
                        tokio::task::spawn_blocking(move || {
                            export_buffer_cancellable(
                                &path,
                                format,
                                result.schema.as_ref(),
                                &result.buffer,
                                ExportLimits::FULL,
                                &task_cancellation,
                            )
                        })
                        .await
                        .map_err(|error| ExportError::Join(error.to_string()))?
                    },
                    report,
                )
            }
            ExportScope::Full => {
                let Some(session) = self.session.clone() else {
                    self.status = "完整导出需要保持数据库连接".to_owned();
                    return;
                };
                let Some(snapshot) = self.last_query.clone() else {
                    self.status = "没有可重新执行的查询".to_owned();
                    return;
                };
                let request = QueryRequest::new(
                    Uuid::new_v4(),
                    snapshot.language,
                    snapshot.text,
                    FULL_EXPORT_TIMEOUT,
                    FULL_EXPORT_QUERY_ROWS,
                );
                self.runtime.spawn_reported(
                    move |task_cancellation| async move {
                        export_query(session, request, path, format, task_cancellation).await
                    },
                    report,
                )
            }
        };

        self.begin_operation(operation_id, OperationKind::Export, cancellation);
        self.status = match scope {
            ExportScope::Current => {
                format!("正在导出当前缓冲结果为 {}…", format.label())
            }
            ExportScope::Full => {
                format!("正在重新执行并完整导出为 {}…", format.label())
            }
        };
    }

    fn load_objects(&mut self, parent: Option<ObjectPath>, depth: usize, ctx: &egui::Context) {
        let Some(session) = self.session.clone() else {
            return;
        };
        let key = object_path_key(parent.as_ref());
        if self.loaded_object_keys.contains(&key) || self.loading_object_keys.contains(&key) {
            return;
        }
        self.loading_object_keys.insert(key.clone());
        let request = ObjectListRequest {
            id: Uuid::new_v4(),
            parent: parent.clone(),
            include_system: false,
            limit: 500,
            cursor: None,
            timeout: OBJECT_TIMEOUT,
        };
        let session_generation = self.session_generation;
        let event_sender = self.event_sender.clone();
        let repaint = ctx.clone();
        let _cancellation = self.runtime.spawn_reported(
            move |task_cancellation| async move {
                session.list_objects(request, task_cancellation).await
            },
            move |result| {
                let _sent = event_sender.send(AppEvent::ObjectsLoaded {
                    session_generation,
                    key,
                    parent,
                    depth,
                    result,
                });
                repaint.request_repaint();
            },
        );
    }

    fn allow_pending_navigation(&mut self, action: &str) -> bool {
        let pending = self
            .table_editor
            .as_ref()
            .map_or(0, TableEditorState::pending_change_count);
        if pending == 0 {
            return true;
        }
        self.status = format!(
            "有 {pending} 行未保存变更；请先应用或放弃变更，再{action}"
        );
        false
    }

    fn sql_dialect(&self) -> SqlDialect {
        match self.choice().id {
            "postgresql" => SqlDialect::PostgreSql,
            "mysql" => SqlDialect::MySql,
            "sqlite" => SqlDialect::SQLite,
            _ => SqlDialect::Generic,
        }
    }

    fn open_table(&mut self, table: TableRef, ctx: &egui::Context) {
        if self.operation_busy
            || !self.capability_support().table_data
            || !self.allow_pending_navigation("打开其他对象")
        {
            return;
        }
        self.table_browse = Some(TableBrowseControls::new(table));
        self.table_editor = None;
        self.current_result = None;
        self.active_result = None;
        self.last_query = None;
        self.query_read_only_reason = None;
        self.invalidate_table_change_plan();
        self.load_active_table_page(0, ctx);
    }

    fn load_active_table_page(&mut self, page_index: u64, ctx: &egui::Context) {
        if self.operation_busy || !self.allow_pending_navigation("重新加载数据") {
            return;
        }
        let Some(session) = self.session.clone() else {
            self.status = "请先建立数据库连接".to_owned();
            return;
        };
        let Some(controls) = self.table_browse.clone() else {
            return;
        };
        let page_size = match u32::try_from(self.query_settings.page_size) {
            Ok(page_size) => page_size,
            Err(_) => {
                self.status = "分页大小超出驱动支持范围".to_owned();
                return;
            }
        };
        let sort = (!controls.sort_column.is_empty()).then(|| {
            vec![TableSort {
                column: controls.sort_column.clone(),
                direction: controls.sort_direction,
            }]
        });
        let table_name = controls.table.name.clone();
        let request = TableBrowseRequest {
            id: Uuid::new_v4(),
            table: controls.table.clone(),
            filters: controls.filters,
            sort,
            raw_where: optional_input(&controls.raw_where),
            raw_order_by: optional_input(&controls.raw_order_by),
            page_index,
            page_size,
            timeout: OPERATION_TIMEOUT,
        };
        let table = controls.table;
        let session_generation = self.session_generation;
        let operation_id = self.next_operation_id();
        let event_sender = self.event_sender.clone();
        let repaint = ctx.clone();
        let cancellation = self.runtime.spawn_reported(
            move |task_cancellation| async move {
                let metadata = session
                    .table_metadata(table, task_cancellation.clone())
                    .await?;
                let page = session
                    .browse_table(request, task_cancellation)
                    .await?;
                Ok::<LoadedTable, DriverError>(LoadedTable { metadata, page })
            },
            move |result| {
                let _sent = event_sender.send(AppEvent::TableLoaded {
                    session_generation,
                    operation_id,
                    result,
                });
                repaint.request_repaint();
            },
        );
        self.begin_operation(operation_id, OperationKind::TableLoad, cancellation);
        self.result_tab = ResultTab::Data;
        self.status = format!("正在读取 {table_name} 第 {} 页…", page_index + 1);
    }

    fn prepare_query_editor(&mut self, operation_id: u64, ctx: &egui::Context) {
        let Some(snapshot) = self
            .last_query
            .as_ref()
            .filter(|snapshot| snapshot.operation_id == operation_id)
        else {
            return;
        };
        let Some(analysis) = snapshot.source_analysis.clone() else {
            self.query_read_only_reason = Some("当前查询语言不支持表格式编辑".to_owned());
            return;
        };
        if let Some(reason) = analysis.reason {
            self.query_read_only_reason =
                Some(query_editability_reason_label(reason).to_owned());
            return;
        }
        let Some(table) = analysis.table.clone() else {
            self.query_read_only_reason = Some("无法确定查询的数据源表".to_owned());
            return;
        };
        let Some(session) = self.session.clone() else {
            return;
        };
        let session_generation = self.session_generation;
        let event_sender = self.event_sender.clone();
        let repaint = ctx.clone();
        let _cancellation = self.runtime.spawn_reported(
            move |task_cancellation| async move {
                session.table_metadata(table, task_cancellation).await
            },
            move |result| {
                let _sent = event_sender.send(AppEvent::QueryMetadataLoaded {
                    session_generation,
                    operation_id,
                    analysis,
                    result,
                });
                repaint.request_repaint();
            },
        );
    }

    fn preview_table_changes(&mut self, ctx: &egui::Context) {
        if self.operation_busy {
            return;
        }
        let Some(session) = self.session.clone() else {
            self.status = "数据库连接已断开".to_owned();
            return;
        };
        let Some(editor) = self.table_editor.as_ref() else {
            return;
        };
        let request = match editor.change_request(OPERATION_TIMEOUT) {
            Ok(request) if !request.is_empty() => request,
            Ok(_) => {
                self.status = "没有待提交的表数据变更".to_owned();
                return;
            }
            Err(error) => {
                self.status = format!("无法生成变更：{error}");
                return;
            }
        };
        let editor_generation = self.table_editor_generation;
        let revision = editor.revision();
        let session_generation = self.session_generation;
        let operation_id = self.next_operation_id();
        let event_sender = self.event_sender.clone();
        let repaint = ctx.clone();
        let cancellation = self.runtime.spawn_reported(
            move |task_cancellation| async move {
                session
                    .plan_table_changes(request, task_cancellation)
                    .await
            },
            move |result| {
                let _sent = event_sender.send(AppEvent::TablePlanReady {
                    session_generation,
                    operation_id,
                    editor_generation,
                    revision,
                    result,
                });
                repaint.request_repaint();
            },
        );
        self.begin_operation(operation_id, OperationKind::TablePlan, cancellation);
        self.status = "正在生成参数化 SQL 预览…".to_owned();
    }

    fn apply_prepared_table_changes(&mut self, ctx: &egui::Context) {
        if self.operation_busy {
            return;
        }
        let Some(prepared) = self.prepared_table_change.as_ref() else {
            return;
        };
        let Some(editor) = self.table_editor.as_ref() else {
            return;
        };
        if prepared.editor_generation != self.table_editor_generation
            || prepared.revision != editor.revision()
        {
            self.invalidate_table_change_plan();
            self.status = "数据已继续修改，请重新生成 SQL 预览".to_owned();
            return;
        }
        let Some(session) = self.session.clone() else {
            self.status = "数据库连接已断开".to_owned();
            return;
        };
        let plan = prepared.plan.clone();
        let editor_generation = prepared.editor_generation;
        let session_generation = self.session_generation;
        let operation_id = self.next_operation_id();
        let event_sender = self.event_sender.clone();
        let repaint = ctx.clone();
        let cancellation = self.runtime.spawn_reported(
            move |task_cancellation| async move {
                session
                    .apply_table_changes(plan, task_cancellation)
                    .await
            },
            move |result| {
                let _sent = event_sender.send(AppEvent::TableApplied {
                    session_generation,
                    operation_id,
                    editor_generation,
                    result,
                });
                repaint.request_repaint();
            },
        );
        self.begin_operation(operation_id, OperationKind::TableApply, cancellation);
        self.status = "正在原子提交表数据变更…".to_owned();
    }

    fn invalidate_table_change_plan(&mut self) {
        self.prepared_table_change = None;
        self.show_table_change_preview = false;
    }

    fn activate_object(&mut self, index: usize, ctx: &egui::Context) {
        let Some(row) = self.objects.get(index).cloned() else {
            return;
        };
        if row.object.has_children {
            let key = object_path_key(Some(&row.object.path));
            if self.expanded_object_keys.remove(&key) {
                self.status = format!("已收起 {}", row.object.name);
                return;
            }
            self.expanded_object_keys.insert(key);
            self.load_objects(Some(row.object.path), row.depth.saturating_add(1), ctx);
        } else {
            self.status = format!(
                "{} · {}",
                row.object.name,
                object_kind_label(&row.object.kind)
            );
        }
    }

    fn visible_object_indices(&self) -> Vec<usize> {
        let mut branch_visible = Vec::new();
        let mut visible = Vec::new();
        for (index, row) in self.objects.iter().enumerate() {
            let row_visible = row.depth == 0
                || branch_visible
                    .get(row.depth.saturating_sub(1))
                    .copied()
                    .unwrap_or(false);
            if branch_visible.len() <= row.depth {
                branch_visible.resize(row.depth + 1, false);
            }
            branch_visible.truncate(row.depth + 1);
            let key = object_path_key(Some(&row.object.path));
            branch_visible[row.depth] = row_visible && self.expanded_object_keys.contains(&key);
            if row_visible {
                visible.push(index);
            }
        }
        visible
    }

    fn next_operation_id(&mut self) -> u64 {
        self.operation_generation = self.operation_generation.wrapping_add(1);
        self.operation_generation
    }

    fn begin_operation(
        &mut self,
        operation_id: u64,
        kind: OperationKind,
        cancellation: CancellationToken,
    ) {
        self.operation_busy = true;
        self.active_operation_id = Some(operation_id);
        self.active_operation_kind = Some(kind);
        self.active_cancellation = Some(cancellation);
    }

    fn finish_operation(&mut self) {
        self.operation_busy = false;
        self.active_operation_id = None;
        self.active_operation_kind = None;
        self.active_cancellation = None;
    }

    fn cancel_active_operation(&mut self) -> bool {
        let Some(cancellation) = self.active_cancellation.take() else {
            return false;
        };
        cancellation.cancel();
        self.operation_generation = self.operation_generation.wrapping_add(1);
        self.active_operation_id = None;
        self.operation_busy = false;
        let kind = self.active_operation_kind.take();
        self.query_events = None;
        if let Some(active) = self.active_result.take() {
            self.current_result = Some(Arc::new(active));
            self.refresh_data_grid();
        }
        self.status = match kind {
            Some(OperationKind::Query) => {
                let rows = self
                    .current_result
                    .as_deref()
                    .map_or(0, |result| result.buffer.row_count());
                format!("查询已取消 · 已保留 {rows} 行")
            }
            Some(OperationKind::Export) => "导出已取消，目标文件不会被替换".to_owned(),
            Some(OperationKind::TableLoad) => "表数据加载已取消".to_owned(),
            Some(OperationKind::TablePlan) => "SQL 预览生成已取消".to_owned(),
            Some(OperationKind::TableApply) => {
                "表数据提交已取消，事务已回滚".to_owned()
            }
            Some(OperationKind::Explain | OperationKind::SlowQueries) | None => {
                "操作已取消".to_owned()
            }
        };
        true
    }

    fn close_current_session(&mut self) {
        self.session_generation = self.session_generation.wrapping_add(1);
        if let Some(session) = self.session.take() {
            self.close_session_detached(session);
        }
    }

    fn close_session_detached(&self, session: Arc<dyn DatabaseSession>) {
        let _close_task = self
            .runtime
            .spawn(move |_| async move { session.close().await });
    }

    fn reset_session_view(&mut self) {
        self.objects.clear();
        self.expanded_object_keys.clear();
        self.loaded_object_keys.clear();
        self.loading_object_keys.clear();
        self.pending_write_confirmation = None;
        self.active_result = None;
        self.current_result = None;
        self.current_page = 0;
        self.last_query = None;
        self.query_events = None;
        self.table_editor = None;
        self.table_browse = None;
        self.invalidate_table_change_plan();
        self.query_read_only_reason = None;
        self.data_grid.replace(ResultModel::message(
            "连接数据库后执行查询，结果将在这里显示",
        ));
    }

    fn confirm_write(&mut self, action: WriteAction, text: &str) -> bool {
        if !requires_confirmation(self.choice().language, text) {
            self.pending_write_confirmation = None;
            return true;
        }
        if write_confirmation_matches(self.pending_write_confirmation.as_ref(), action, text) {
            self.pending_write_confirmation = None;
            return true;
        }
        self.pending_write_confirmation = Some(PendingWriteConfirmation {
            action,
            text: text.to_owned(),
        });
        self.status = match action {
            WriteAction::Execute => "检测到写操作；再次点击“执行”确认".to_owned(),
            WriteAction::Analyze => "分析执行会真实运行写操作；再次点击“分析执行”确认".to_owned(),
            WriteAction::FullExport => {
                "完整导出会重新执行写操作；再次选择完整导出格式确认".to_owned()
            }
        };
        false
    }

    fn refresh_data_grid(&mut self) {
        let row_count = self
            .active_result
            .as_ref()
            .or(self.current_result.as_deref())
            .map_or(0, |result| result.buffer.row_count());
        let pages = page_count(row_count, self.query_settings.page_size);
        self.current_page = self.current_page.min(pages.saturating_sub(1));
        let offset = self
            .current_page
            .saturating_mul(self.query_settings.page_size);
        let model = self
            .active_result
            .as_ref()
            .or(self.current_result.as_deref())
            .map(|result| {
                let batches = result.buffer.slice(offset, self.query_settings.page_size);
                ResultModel::from_page(
                    result.schema.as_ref(),
                    &batches,
                    &result.messages,
                    result.affected_rows,
                )
            })
            .unwrap_or_else(|| ResultModel::message("尚未执行查询"));
        self.data_grid.replace(model);
    }

    fn query_completion_status(&self) -> String {
        let Some(result) = self.current_result.as_deref() else {
            return "执行完成".to_owned();
        };
        let Some(stats) = result.stats else {
            return format!("执行完成 · 已缓冲 {} 行", result.buffer.row_count());
        };
        let affected_rows = stats.affected_rows.max(result.affected_rows);
        let mut status = format!(
            "执行完成 · 返回 {} 行 · 影响 {} 行 · {} ms",
            stats.returned_rows, affected_rows, stats.elapsed_millis
        );
        if stats.row_limit_reached || result.buffer.limit_reached() {
            status.push_str(" · 已达到结果限制");
        }
        if result.buffer.row_count() < usize::try_from(stats.returned_rows).unwrap_or(usize::MAX) {
            status.push_str(&format!(" · 已缓冲 {} 行", result.buffer.row_count()));
        }
        status
    }

    fn previous_page(&mut self) {
        if self.current_page > 0 {
            self.current_page -= 1;
            self.refresh_data_grid();
        }
    }

    fn next_page(&mut self) {
        let rows = self
            .active_result
            .as_ref()
            .or(self.current_result.as_deref())
            .map_or(0, |result| result.buffer.row_count());
        let pages = page_count(rows, self.query_settings.page_size);
        if self.current_page + 1 < pages {
            self.current_page += 1;
            self.refresh_data_grid();
        }
    }

    fn set_page_size(&mut self, page_size: usize, ctx: &egui::Context) {
        if !PAGE_SIZE_PRESETS.contains(&page_size) || page_size == self.query_settings.page_size {
            return;
        }
        if !self.allow_pending_navigation("切换分页大小") {
            return;
        }
        let previous_page_size = self.query_settings.page_size;
        self.query_settings.page_size = page_size;
        self.current_page =
            page_after_page_size_change(self.current_page, previous_page_size, page_size);
        if self
            .table_editor
            .as_ref()
            .is_some_and(|editor| editor.origin() == EditorOrigin::Table)
        {
            let previous_page = self
                .table_editor
                .as_ref()
                .map_or(0, TableEditorState::page_index);
            let first_row = previous_page.saturating_mul(
                u64::try_from(previous_page_size).unwrap_or(u64::MAX),
            );
            let new_page =
                first_row / u64::try_from(page_size).unwrap_or(u64::MAX);
            self.load_active_table_page(new_page, ctx);
        } else {
            self.refresh_data_grid();
            self.status = format!("每页显示已切换为 {page_size} 行");
        }
    }

    fn set_row_limit(&mut self, row_limit: usize) {
        if ROW_LIMIT_PRESETS.contains(&row_limit) {
            self.query_settings.row_limit = row_limit;
            self.status = format!("行上限将在下次执行时使用：{row_limit}");
        }
    }

    fn set_memory_limit(&mut self, memory_limit_mib: usize) {
        if MEMORY_LIMIT_PRESETS_MIB.contains(&memory_limit_mib) {
            self.query_settings.memory_limit_mib = memory_limit_mib;
            self.status = format!("内存上限将在下次执行时使用：{memory_limit_mib} MiB");
        }
    }

    fn drain_query_events(&mut self) {
        let mut events = Vec::new();
        let mut discard_receiver = false;
        if let Some(active) = self.query_events.as_mut() {
            if !event_is_current(
                self.session_generation,
                self.active_operation_id,
                active.session_generation,
                active.operation_id,
            ) {
                discard_receiver = true;
            } else {
                while let Ok(event) = active.receiver.try_recv() {
                    events.push(event);
                }
            }
        }
        if discard_receiver {
            self.query_events = None;
            return;
        }
        if events.is_empty() {
            return;
        }
        if let Some(result) = self.active_result.as_mut() {
            for event in events {
                result.apply(event);
            }
            self.refresh_data_grid();
        }
    }

    fn drain_app_events(&mut self, ctx: &egui::Context) {
        while let Ok(event) = self.event_receiver.try_recv() {
            self.apply_event(event, ctx);
        }
    }

    fn apply_event(&mut self, event: AppEvent, ctx: &egui::Context) {
        match event {
            AppEvent::Connected {
                session_generation,
                result,
            } => {
                if session_generation != self.session_generation {
                    if let Ok(session) = result {
                        self.close_session_detached(session);
                    }
                    return;
                }
                self.connection_busy = false;
                match result {
                    Ok(session) => {
                        self.session = Some(session);
                        self.status = format!("{} 已连接", self.choice().name);
                        self.load_objects(None, 0, ctx);
                    }
                    Err(error) => {
                        self.session = None;
                        self.status = format!("连接失败：{error}");
                    }
                }
            }
            AppEvent::ObjectsLoaded {
                session_generation,
                key,
                parent,
                depth,
                result,
            } => {
                if session_generation != self.session_generation {
                    return;
                }
                self.loading_object_keys.remove(&key);
                match result {
                    Ok(page) => {
                        let rows = page
                            .items
                            .into_iter()
                            .map(|object| ObjectRow { object, depth })
                            .collect::<Vec<_>>();
                        if let Some(parent) = parent.as_ref() {
                            let parent_key = object_path_key(Some(parent));
                            if let Some(index) = self.objects.iter().position(|row| {
                                object_path_key(Some(&row.object.path)) == parent_key
                            }) {
                                self.objects.splice(index + 1..index + 1, rows);
                            }
                        } else {
                            self.objects = rows;
                        }
                        self.loaded_object_keys.insert(key);
                    }
                    Err(error) => {
                        self.status = format!("对象树加载失败：{error}");
                    }
                }
            }
            AppEvent::QueryFinished {
                session_generation,
                operation_id,
                result,
            } => {
                if !event_is_current(
                    self.session_generation,
                    self.active_operation_id,
                    session_generation,
                    operation_id,
                ) {
                    return;
                }
                self.query_events = None;
                self.finish_operation();
                let buffered_rows = self
                    .active_result
                    .as_ref()
                    .map_or(0, |query| query.buffer.row_count());
                if let Some(active) = self.active_result.take() {
                    self.current_result = Some(Arc::new(active));
                }
                match result {
                    Ok(()) => {
                        self.status = self.query_completion_status();
                        self.refresh_data_grid();
                        self.prepare_query_editor(operation_id, ctx);
                    }
                    Err(error) if buffered_rows == 0 => {
                        self.data_grid
                            .replace(ResultModel::message(format!("执行失败：{error}")));
                        self.status = format!("执行失败：{error}");
                    }
                    Err(TaskError::Cancelled) => {
                        self.status = format!("查询已取消 · 已保留 {buffered_rows} 行");
                        self.refresh_data_grid();
                    }
                    Err(error) => {
                        self.status = format!("执行失败：{error} · 已保留 {buffered_rows} 行");
                        self.refresh_data_grid();
                    }
                }
            }
            AppEvent::QueryMetadataLoaded {
                session_generation,
                operation_id,
                analysis,
                result,
            } => {
                if session_generation != self.session_generation
                    || self
                        .last_query
                        .as_ref()
                        .is_none_or(|snapshot| snapshot.operation_id != operation_id)
                {
                    return;
                }
                let metadata = match result {
                    Ok(metadata) => metadata,
                    Err(error) => {
                        self.query_read_only_reason =
                            Some(format!("查询结果只读：无法读取行标识元数据（{error}）"));
                        return;
                    }
                };
                let Some(result) = self.current_result.as_deref() else {
                    return;
                };
                let Some((columns, rows)) = tabular_cells(
                    result.schema.as_ref(),
                    result.buffer.batches(),
                ) else {
                    self.query_read_only_reason =
                        Some("查询结果不是可编辑的关系型表格".to_owned());
                    return;
                };
                let editability =
                    resolve_query_editability(&analysis, &metadata, &columns);
                if !editability.editable {
                    self.query_read_only_reason = Some(
                        editability
                            .reason
                            .map(query_editability_reason_label)
                            .unwrap_or("查询结果没有可编辑的直接列")
                            .to_owned(),
                    );
                    return;
                }
                let page_size =
                    u32::try_from(self.query_settings.page_size).unwrap_or(u32::MAX);
                self.table_editor = Some(TableEditorState::from_query(
                    metadata,
                    editability,
                    columns,
                    rows,
                    page_size,
                ));
                self.table_editor_generation =
                    self.table_editor_generation.wrapping_add(1);
                self.query_read_only_reason = None;
                self.invalidate_table_change_plan();
            }
            AppEvent::ExplainFinished {
                session_generation,
                operation_id,
                result,
            } => {
                if !event_is_current(
                    self.session_generation,
                    self.active_operation_id,
                    session_generation,
                    operation_id,
                ) {
                    return;
                }
                self.finish_operation();
                match result {
                    Ok(plan) => {
                        self.plan_text = serde_json::to_string_pretty(&plan.document)
                            .unwrap_or_else(|_| plan.document.to_string());
                        self.status = format!(
                            "{} 执行计划已生成{}",
                            plan.engine,
                            if plan.analyzed {
                                "（含实际执行）"
                            } else {
                                ""
                            }
                        );
                    }
                    Err(error) => {
                        self.plan_text = format!("无法生成执行计划\n\n{error}");
                        self.status = format!("执行计划失败：{error}");
                    }
                }
            }
            AppEvent::SlowQueriesFinished {
                session_generation,
                operation_id,
                result,
            } => {
                if !event_is_current(
                    self.session_generation,
                    self.active_operation_id,
                    session_generation,
                    operation_id,
                ) {
                    return;
                }
                self.finish_operation();
                match result {
                    Ok(page) => {
                        let source = page.source.clone();
                        let count = page.entries.len();
                        self.slow_grid.replace(ResultModel::from_slow_queries(page));
                        self.status = format!("已从 {source} 读取 {count} 条慢查询统计");
                    }
                    Err(error) => {
                        self.slow_grid
                            .replace(ResultModel::message(format!("慢查询不可用：{error}")));
                        self.status = format!("慢查询失败：{error}");
                    }
                }
            }
            AppEvent::ExportFinished {
                session_generation,
                operation_id,
                path,
                result,
            } => {
                if !event_is_current(
                    self.session_generation,
                    self.active_operation_id,
                    session_generation,
                    operation_id,
                ) {
                    return;
                }
                self.finish_operation();
                self.status = match result {
                    Ok(summary) => format!(
                        "导出完成 · {} 行 · {} 字节 · {}",
                        summary.rows,
                        summary.bytes,
                        path.display()
                    ),
                    Err(TaskError::Cancelled)
                    | Err(TaskError::Operation(ExportError::Cancelled)) => {
                        "导出已取消，目标文件未修改".to_owned()
                    }
                    Err(TaskError::Operation(error)) => {
                        format!("导出失败：{error}")
                    }
                    Err(TaskError::Join(error)) => {
                        format!("导出任务失败：{error}")
                    }
                };
            }
            AppEvent::TableLoaded {
                session_generation,
                operation_id,
                result,
            } => {
                if !event_is_current(
                    self.session_generation,
                    self.active_operation_id,
                    session_generation,
                    operation_id,
                ) {
                    return;
                }
                self.finish_operation();
                match result {
                    Ok(loaded) => {
                        if let Some(controls) = self.table_browse.as_mut()
                            && controls.filter_column.is_empty()
                        {
                            controls.filter_column = loaded
                                .metadata
                                .columns
                                .first()
                                .map_or_else(String::new, |column| column.name.clone());
                        }
                        let name = loaded.metadata.table.name.clone();
                        let total = loaded.page.total_rows;
                        let page = loaded.page.page_index;
                        self.table_editor = Some(TableEditorState::from_table(
                            loaded.metadata,
                            loaded.page,
                        ));
                        self.table_editor_generation =
                            self.table_editor_generation.wrapping_add(1);
                        self.invalidate_table_change_plan();
                        self.query_read_only_reason = None;
                        self.status = format!(
                            "已读取 {name} · 第 {} 页 · 精确统计 {total} 行",
                            page + 1
                        );
                    }
                    Err(error) => {
                        self.status = format!("表数据加载失败：{error}");
                    }
                }
            }
            AppEvent::TablePlanReady {
                session_generation,
                operation_id,
                editor_generation,
                revision,
                result,
            } => {
                if !event_is_current(
                    self.session_generation,
                    self.active_operation_id,
                    session_generation,
                    operation_id,
                ) {
                    return;
                }
                self.finish_operation();
                if editor_generation != self.table_editor_generation
                    || self
                        .table_editor
                        .as_ref()
                        .is_none_or(|editor| editor.revision() != revision)
                {
                    self.status = "数据已继续修改，请重新生成 SQL 预览".to_owned();
                    return;
                }
                match result {
                    Ok(plan) => {
                        let count = plan.statements.len();
                        self.prepared_table_change = Some(PreparedTableChange {
                            editor_generation,
                            revision,
                            plan,
                        });
                        self.show_table_change_preview = true;
                        self.status = format!("已生成 {count} 条参数化变更语句");
                    }
                    Err(error) => {
                        self.status = format!("SQL 预览生成失败：{error}");
                    }
                }
            }
            AppEvent::TableApplied {
                session_generation,
                operation_id,
                editor_generation,
                result,
            } => {
                if !event_is_current(
                    self.session_generation,
                    self.active_operation_id,
                    session_generation,
                    operation_id,
                ) || editor_generation != self.table_editor_generation
                {
                    return;
                }
                self.finish_operation();
                match result {
                    Ok(applied) => {
                        let origin = self
                            .table_editor
                            .as_ref()
                            .map(TableEditorState::origin);
                        let page = self
                            .table_editor
                            .as_ref()
                            .map_or(0, TableEditorState::page_index);
                        self.table_editor = None;
                        self.invalidate_table_change_plan();
                        self.status = format!(
                            "提交完成 · 新增 {} · 更新 {} · 删除 {}",
                            applied.summary.inserted,
                            applied.summary.updated,
                            applied.summary.deleted
                        );
                        match origin {
                            Some(EditorOrigin::Table) => {
                                self.load_active_table_page(page, ctx);
                            }
                            Some(EditorOrigin::Query) => {
                                if let Some(snapshot) = self.last_query.as_ref() {
                                    self.query_text.clone_from(&snapshot.text);
                                }
                                self.execute(ctx);
                            }
                            None => {}
                        }
                    }
                    Err(error) => {
                        self.invalidate_table_change_plan();
                        self.status = match &error {
                            TaskError::Operation(DriverError::Conflict(_)) => {
                                "提交冲突：数据已被其他事务修改；请放弃变更并重新加载"
                                    .to_owned()
                            }
                            _ => format!("表数据提交失败：{error}"),
                        };
                    }
                }
            }
        }
    }

    fn render_top_bar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal_centered(|ui| {
            ui.add_space(8.0);
            ui.heading("DBC");
            ui.separator();
            ui.toggle_value(&mut self.left_panel_open, "连接与对象");
            ui.toggle_value(&mut self.right_panel_open, "能力");
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.add_space(8.0);
                ui.label(
                    egui::RichText::new(format!("{} 个原生驱动", DRIVER_CHOICES.len())).weak(),
                );
                ui.separator();
                ui.label(if self.session.is_some() {
                    "● 在线"
                } else {
                    "○ 离线"
                });
            });
        });
    }

    fn render_sidebar(&mut self, ui: &mut egui::Ui) {
        ui.heading("数据库连接");
        ui.add_space(4.0);

        let mut selected_driver = self.selected_driver;
        ui.add_enabled_ui(!self.connection_busy && !self.operation_busy, |ui| {
            egui::ComboBox::from_id_salt("driver-choice")
                .selected_text(DRIVER_CHOICES[selected_driver].name)
                .width(ui.available_width())
                .show_ui(ui, |ui| {
                    for (index, choice) in DRIVER_CHOICES.iter().enumerate() {
                        ui.selectable_value(&mut selected_driver, index, choice.name);
                    }
                });
        });
        if selected_driver != self.selected_driver {
            self.select_driver(selected_driver);
        }

        ui.add_space(4.0);
        let endpoint_label = ui.label("连接地址");
        ui.add(
            egui::TextEdit::singleline(&mut self.endpoint)
                .id_salt("connection-endpoint")
                .desired_width(f32::INFINITY),
        )
        .labelled_by(endpoint_label.id);

        let database_label = ui.label("数据库（可选）");
        ui.add(
            egui::TextEdit::singleline(&mut self.database)
                .id_salt("connection-database")
                .desired_width(f32::INFINITY),
        )
        .labelled_by(database_label.id);

        let user_label = ui.label("用户名（可选）");
        ui.add(
            egui::TextEdit::singleline(&mut self.user)
                .id_salt("connection-user")
                .desired_width(f32::INFINITY),
        )
        .labelled_by(user_label.id);

        let password_label = ui
            .horizontal(|ui| {
                let password_label = ui.label("密码（仅保存在内存）");
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.toggle_value(&mut self.password_visible, "显示");
                });
                password_label
            })
            .inner;
        ui.add(
            egui::TextEdit::singleline(&mut self.password)
                .id_salt("connection-password")
                .password(!self.password_visible)
                .desired_width(f32::INFINITY),
        )
        .labelled_by(password_label.id);

        ui.horizontal(|ui| {
            if ui
                .add_enabled(
                    !self.connection_busy && !self.operation_busy,
                    egui::Button::new(if self.connection_busy {
                        "连接中…"
                    } else {
                        "连接"
                    }),
                )
                .clicked()
            {
                self.connect(ui.ctx());
            }
            if ui
                .add_enabled(self.session.is_some(), egui::Button::new("断开"))
                .clicked()
            {
                self.disconnect();
            }
        });

        ui.separator();
        ui.horizontal(|ui| {
            ui.strong("连接与对象");
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.weak(if self.session.is_some() {
                    "在线"
                } else {
                    "离线"
                });
            });
        });
        ui.separator();

        egui::ScrollArea::vertical()
            .id_salt("object-tree-scroll")
            .auto_shrink([false, false])
            .show(ui, |ui| {
                if self.objects.is_empty() {
                    ui.weak(if self.session.is_some() {
                        "正在读取对象…"
                    } else {
                        "连接后显示数据库对象"
                    });
                    return;
                }
                let mut activated = None;
                let mut opened_table = None;
                for index in self.visible_object_indices() {
                    let row = &self.objects[index];
                    let key = object_path_key(Some(&row.object.path));
                    let expanded = self.expanded_object_keys.contains(&key);
                    let loading = self.loading_object_keys.contains(&key);
                    let marker = if row.object.has_children {
                        if loading {
                            "…"
                        } else if expanded {
                            "▾"
                        } else {
                            "▸"
                        }
                    } else {
                        " "
                    };
                    let label = format!(
                        "{marker} {} {}",
                        object_kind_icon(&row.object.kind),
                        row.object.name
                    );
                    ui.horizontal(|ui| {
                        ui.add_space(row.depth as f32 * 16.0);
                        let response = ui
                            .add(egui::Button::new(label).frame(false).selected(expanded))
                            .on_hover_text(if object_table_ref(&row.object).is_some() {
                                "双击打开数据；单击展开对象"
                            } else {
                                object_kind_label(&row.object.kind)
                            });
                        if response.double_clicked() {
                            opened_table = object_table_ref(&row.object);
                        } else if response.clicked() {
                            activated = Some(index);
                        }
                    });
                }
                if let Some(table) = opened_table {
                    self.open_table(table, ui.ctx());
                } else if let Some(index) = activated {
                    self.activate_object(index, ui.ctx());
                }
            });
    }

    fn render_query_toolbar(&mut self, ui: &mut egui::Ui) {
        let capabilities = self.capability_support();
        let connected = self.session.is_some();
        let mut page_size = self.query_settings.page_size;
        let mut row_limit = self.query_settings.row_limit;
        let mut memory_limit = self.query_settings.memory_limit_mib;

        egui::ScrollArea::horizontal()
            .id_salt("query-toolbar-scroll")
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(connected && !self.operation_busy, egui::Button::new("执行"))
                        .clicked()
                    {
                        self.execute(ui.ctx());
                    }
                    if ui
                        .add_enabled(
                            connected && !self.operation_busy && capabilities.explain,
                            egui::Button::new("执行计划"),
                        )
                        .clicked()
                    {
                        self.start_explain(ExplainMode::Estimated, ui.ctx());
                    }
                    if ui
                        .add_enabled(
                            connected && !self.operation_busy && capabilities.analyze,
                            egui::Button::new("分析执行"),
                        )
                        .clicked()
                    {
                        self.start_explain(ExplainMode::Analyze, ui.ctx());
                    }
                    if ui
                        .add_enabled(
                            connected && !self.operation_busy && capabilities.slow_queries,
                            egui::Button::new("慢查询"),
                        )
                        .clicked()
                    {
                        self.load_slow_queries(ui.ctx());
                    }
                    if ui
                        .add_enabled(
                            self.active_cancellation.is_some(),
                            egui::Button::new("取消"),
                        )
                        .clicked()
                    {
                        self.cancel_active_operation();
                    }
                    ui.separator();
                    egui::ComboBox::from_id_salt("page-size")
                        .selected_text(format!("每页 {page_size}"))
                        .show_ui(ui, |ui| {
                            for &preset in PAGE_SIZE_PRESETS {
                                ui.selectable_value(&mut page_size, preset, preset.to_string());
                            }
                        });
                    egui::ComboBox::from_id_salt("row-limit")
                        .selected_text(format!("上限 {}", compact_count(row_limit)))
                        .show_ui(ui, |ui| {
                            for &preset in ROW_LIMIT_PRESETS {
                                ui.selectable_value(&mut row_limit, preset, compact_count(preset));
                            }
                        });
                    egui::ComboBox::from_id_salt("memory-limit")
                        .selected_text(format!("内存 {memory_limit} MiB"))
                        .show_ui(ui, |ui| {
                            for &preset in MEMORY_LIMIT_PRESETS_MIB {
                                ui.selectable_value(
                                    &mut memory_limit,
                                    preset,
                                    format!("{preset} MiB"),
                                );
                            }
                        });
                });
            });

        if page_size != self.query_settings.page_size {
            self.set_page_size(page_size, ui.ctx());
        }
        if row_limit != self.query_settings.row_limit {
            self.set_row_limit(row_limit);
        }
        if memory_limit != self.query_settings.memory_limit_mib {
            self.set_memory_limit(memory_limit);
        }
    }

    fn render_query_editor(&mut self, ui: &mut egui::Ui) {
        let syntax = self.choice().editor_syntax.syntax();
        let theme = if ui.visuals().dark_mode {
            ColorTheme::GITHUB_DARK
        } else {
            ColorTheme::GITHUB_LIGHT
        };
        let rows = (ui.available_height() / 18.0).floor().max(8.0) as usize;
        let mut editor = CodeEditor::default()
            .id_source("dbc-query-editor")
            .with_rows(rows)
            .with_theme(theme)
            .with_ui_fontsize(ui)
            .with_numlines(true)
            .with_wrap(false)
            .desired_width(f32::INFINITY);
        editor.show(ui, &mut self.query_text, &syntax);
    }

    fn render_table_browser_controls(&mut self, ui: &mut egui::Ui) {
        let columns = self
            .table_editor
            .as_ref()
            .map(|editor| {
                editor
                    .metadata()
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut add_filter = false;
        let mut remove_filter = None;
        let mut reload = false;
        let Some(controls) = self.table_browse.as_mut() else {
            return;
        };

        ui.horizontal(|ui| {
            ui.strong("结构化筛选");
            egui::ComboBox::from_id_salt("table-filter-column")
                .selected_text(if controls.filter_column.is_empty() {
                    "选择列"
                } else {
                    controls.filter_column.as_str()
                })
                .show_ui(ui, |ui| {
                    for column in &columns {
                        ui.selectable_value(
                            &mut controls.filter_column,
                            column.clone(),
                            column,
                        );
                    }
                });
            egui::ComboBox::from_id_salt("table-filter-operator")
                .selected_text(filter_operator_label(controls.filter_operator))
                .show_ui(ui, |ui| {
                    for operator in FILTER_OPERATORS {
                        ui.selectable_value(
                            &mut controls.filter_operator,
                            *operator,
                            filter_operator_label(*operator),
                        );
                    }
                });
            if filter_operator_value_count(controls.filter_operator) > 0 {
                ui.add(
                    egui::TextEdit::singleline(&mut controls.filter_value)
                        .hint_text("值")
                        .desired_width(120.0),
                );
            }
            if filter_operator_value_count(controls.filter_operator) == 2 {
                ui.add(
                    egui::TextEdit::singleline(&mut controls.second_filter_value)
                        .hint_text("第二个值")
                        .desired_width(120.0),
                );
            }
            if ui.small_button("添加").clicked() {
                add_filter = true;
            }
            ui.separator();
            egui::ComboBox::from_id_salt("table-sort-column")
                .selected_text(if controls.sort_column.is_empty() {
                    "稳定键排序"
                } else {
                    controls.sort_column.as_str()
                })
                .show_ui(ui, |ui| {
                    ui.selectable_value(
                        &mut controls.sort_column,
                        String::new(),
                        "稳定键排序",
                    );
                    for column in &columns {
                        ui.selectable_value(
                            &mut controls.sort_column,
                            column.clone(),
                            column,
                        );
                    }
                });
            ui.selectable_value(
                &mut controls.sort_direction,
                SortDirection::Ascending,
                "升序",
            );
            ui.selectable_value(
                &mut controls.sort_direction,
                SortDirection::Descending,
                "降序",
            );
        });
        ui.horizontal(|ui| {
            ui.strong("原始片段");
            ui.add(
                egui::TextEdit::singleline(&mut controls.raw_where)
                    .hint_text("WHERE（不含 WHERE）")
                    .desired_width(240.0),
            );
            ui.add(
                egui::TextEdit::singleline(&mut controls.raw_order_by)
                    .hint_text("ORDER BY（不含 ORDER BY）")
                    .desired_width(240.0),
            );
            if ui
                .add_enabled(!self.operation_busy, egui::Button::new("重新加载"))
                .clicked()
            {
                reload = true;
            }
        });
        if !controls.filters.is_empty() {
            ui.horizontal_wrapped(|ui| {
                ui.weak("已启用：");
                for (index, filter) in controls.filters.iter().enumerate() {
                    let label = format!(
                        "{} {} {}",
                        filter.column,
                        filter_operator_label(filter.operator),
                        filter_values_label(&filter.values)
                    );
                    if ui.small_button(format!("{label} ×")).clicked() {
                        remove_filter = Some(index);
                    }
                }
            });
        }

        if add_filter {
            self.add_structured_filter();
        }
        if let Some(index) = remove_filter
            && let Some(controls) = self.table_browse.as_mut()
            && index < controls.filters.len()
        {
            controls.filters.remove(index);
        }
        if reload {
            self.load_active_table_page(0, ui.ctx());
        }
    }

    fn add_structured_filter(&mut self) {
        let Some(controls) = self.table_browse.as_ref() else {
            return;
        };
        let column_name = controls.filter_column.clone();
        let operator = controls.filter_operator;
        let first = controls.filter_value.clone();
        let second = controls.second_filter_value.clone();
        let Some(column) = self
            .table_editor
            .as_ref()
            .and_then(|editor| editor.metadata().column(&column_name))
        else {
            self.status = "请先选择筛选列".to_owned();
            return;
        };
        let parse = |value: &str| input_cell_value(&column.database_type, value);
        let values = match operator {
            FilterOperator::IsNull | FilterOperator::IsNotNull => Ok(Vec::new()),
            FilterOperator::Between | FilterOperator::NotBetween => {
                parse(&first).and_then(|first| {
                    parse(&second).map(|second| vec![first, second])
                })
            }
            FilterOperator::In | FilterOperator::NotIn => first
                .split(',')
                .map(str::trim)
                .map(parse)
                .collect::<Result<Vec<_>, _>>(),
            _ => parse(&first).map(|value| vec![value]),
        };
        let values = match values {
            Ok(values) => values,
            Err(error) => {
                self.status = format!("筛选值无效：{error}");
                return;
            }
        };
        if let Some(controls) = self.table_browse.as_mut() {
            controls.filters.push(TableFilter {
                column: column_name,
                operator,
                values,
            });
            controls.filter_value.clear();
            controls.second_filter_value.clear();
        }
        self.status = "筛选条件已添加；点击“重新加载”生效".to_owned();
    }

    fn render_results(&mut self, ui: &mut egui::Ui) {
        egui::Panel::top("result-tabs")
            .exact_size(34.0)
            .show(ui, |ui| {
                ui.horizontal_centered(|ui| {
                    ui.selectable_value(&mut self.result_tab, ResultTab::Data, "数据");
                    ui.selectable_value(&mut self.result_tab, ResultTab::Plan, "执行计划");
                    ui.selectable_value(&mut self.result_tab, ResultTab::SlowQueries, "慢查询");
                });
            });

        let show_table_browser = self
            .table_editor
            .as_ref()
            .is_some_and(|editor| editor.origin() == EditorOrigin::Table);
        if self.result_tab == ResultTab::Data && show_table_browser {
            egui::Panel::top("table-browser-controls")
                .exact_size(110.0)
                .show(ui, |ui| {
                    self.render_table_browser_controls(ui);
                });
        }

        if self.result_tab == ResultTab::Data {
            egui::Panel::bottom("result-pagination")
                .exact_size(36.0)
                .show(ui, |ui| {
                    self.render_result_controls(ui);
                });
        }

        egui::CentralPanel::no_frame().show(ui, |ui| match self.result_tab {
            ResultTab::Data => {
                if let Some(editor) = self.table_editor.as_mut() {
                    if editor.show(
                        ui,
                        self.current_page,
                        self.query_settings.page_size,
                    ) {
                        self.invalidate_table_change_plan();
                    }
                } else {
                    self.data_grid.show(ui, "data-grid");
                }
            }
            ResultTab::SlowQueries => self.slow_grid.show(ui, "slow-query-grid"),
            ResultTab::Plan => {
                egui::ScrollArea::both()
                    .id_salt("execution-plan-scroll")
                    .show(ui, |ui| {
                        ui.add(
                            egui::Label::new(egui::RichText::new(&self.plan_text).monospace())
                                .selectable(true),
                        );
                    });
            }
        });
    }

    fn render_result_controls(&mut self, ui: &mut egui::Ui) {
        if self.table_editor.is_some() {
            self.render_editable_result_controls(ui);
            return;
        }
        let result_rows = self
            .active_result
            .as_ref()
            .or(self.current_result.as_deref())
            .map_or(0, |result| result.buffer.row_count());
        let result_pages = page_count(result_rows, self.query_settings.page_size);
        let can_export_current = self
            .current_result
            .as_deref()
            .and_then(|result| result.schema.as_ref())
            .is_some()
            && !self.operation_busy;
        let can_export_full =
            self.session.is_some() && self.last_query.is_some() && !self.operation_busy;
        let mut export = None;

        ui.horizontal_centered(|ui| {
            if ui
                .add_enabled(self.current_page > 0, egui::Button::new("上一页"))
                .clicked()
            {
                self.previous_page();
            }
            if ui
                .add_enabled(
                    self.current_page + 1 < result_pages,
                    egui::Button::new("下一页"),
                )
                .clicked()
            {
                self.next_page();
            }
            ui.weak(format!(
                "第 {} / {} 页 · {} 行",
                self.current_page + 1,
                result_pages,
                result_rows
            ));
            if let Some(reason) = &self.query_read_only_reason {
                ui.separator();
                ui.weak(reason);
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.add_enabled_ui(can_export_full, |ui| {
                    ui.menu_button("完整导出", |ui| {
                        if ui.button("CSV").clicked() {
                            export = Some((ExportScope::Full, ExportFormat::Csv));
                            ui.close();
                        }
                        if ui.button("JSONL").clicked() {
                            export = Some((ExportScope::Full, ExportFormat::JsonLines));
                            ui.close();
                        }
                    });
                });
                ui.add_enabled_ui(can_export_current, |ui| {
                    ui.menu_button("导出当前", |ui| {
                        if ui.button("CSV").clicked() {
                            export = Some((ExportScope::Current, ExportFormat::Csv));
                            ui.close();
                        }
                        if ui.button("JSONL").clicked() {
                            export = Some((ExportScope::Current, ExportFormat::JsonLines));
                            ui.close();
                        }
                    });
                });
            });
        });

        if let Some((scope, format)) = export {
            self.request_export(scope, format, ui.ctx());
        }
    }

    fn render_editable_result_controls(&mut self, ui: &mut egui::Ui) {
        let Some(editor) = self.table_editor.as_ref() else {
            return;
        };
        let origin = editor.origin();
        let pending = editor.pending_change_count();
        let allow_insert = editor.allow_insert();
        let read_only_reason = editor.read_only_reason().map(str::to_owned);
        let (page_index, page_count, total_rows) = match origin {
            EditorOrigin::Table => {
                let page_size = u64::from(editor.page_size()).max(1);
                let total = editor.total_rows();
                (
                    editor.page_index(),
                    total.div_ceil(page_size).max(1),
                    total,
                )
            }
            EditorOrigin::Query => {
                let total = editor.total_rows();
                let pages = u64::try_from(page_count(
                    usize::try_from(total).unwrap_or(usize::MAX),
                    self.query_settings.page_size,
                ))
                .unwrap_or(u64::MAX);
                (
                    u64::try_from(self.current_page).unwrap_or(u64::MAX),
                    pages,
                    total,
                )
            }
        };
        let mut previous = false;
        let mut next = false;
        let mut add = false;
        let mut discard = false;
        let mut preview = false;

        ui.horizontal_centered(|ui| {
            if ui
                .add_enabled(page_index > 0 && !self.operation_busy, egui::Button::new("上一页"))
                .clicked()
            {
                previous = true;
            }
            if ui
                .add_enabled(
                    page_index + 1 < page_count && !self.operation_busy,
                    egui::Button::new("下一页"),
                )
                .clicked()
            {
                next = true;
            }
            ui.weak(format!(
                "第 {} / {} 页 · {} 行",
                page_index + 1,
                page_count,
                total_rows
            ));
            if let Some(reason) = &read_only_reason {
                ui.separator();
                ui.weak(reason);
            }
            if let Some(reason) = &self.query_read_only_reason {
                ui.separator();
                ui.weak(reason);
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui
                    .add_enabled(
                        pending > 0 && !self.operation_busy,
                        egui::Button::new(format!("SQL 预览并应用 ({pending})")),
                    )
                    .clicked()
                {
                    preview = true;
                }
                if ui
                    .add_enabled(pending > 0, egui::Button::new("放弃变更"))
                    .clicked()
                {
                    discard = true;
                }
                if ui
                    .add_enabled(
                        allow_insert && !self.operation_busy,
                        egui::Button::new("新增行"),
                    )
                    .clicked()
                {
                    add = true;
                }
            });
        });

        if previous && self.allow_pending_navigation("切换页面") {
            match origin {
                EditorOrigin::Table => {
                    self.load_active_table_page(page_index.saturating_sub(1), ui.ctx());
                }
                EditorOrigin::Query => self.previous_page(),
            }
        }
        if next && self.allow_pending_navigation("切换页面") {
            match origin {
                EditorOrigin::Table => {
                    self.load_active_table_page(page_index.saturating_add(1), ui.ctx());
                }
                EditorOrigin::Query => self.next_page(),
            }
        }
        if add
            && self
                .table_editor
                .as_mut()
                .is_some_and(TableEditorState::add_row)
        {
            self.invalidate_table_change_plan();
        }
        if discard {
            if let Some(editor) = self.table_editor.as_mut() {
                editor.discard_changes();
            }
            self.invalidate_table_change_plan();
            self.status = "已放弃未提交的表数据变更".to_owned();
        }
        if preview {
            self.preview_table_changes(ui.ctx());
        }
    }

    fn render_table_change_preview(&mut self, ctx: &egui::Context) {
        if !self.show_table_change_preview {
            return;
        }
        let Some(prepared) = self.prepared_table_change.as_ref() else {
            self.show_table_change_preview = false;
            return;
        };
        let statements = prepared.plan.statements.clone();
        let summary = prepared.plan.summary;
        let mut open = self.show_table_change_preview;
        let mut apply = false;
        egui::Window::new("参数化 SQL 预览")
            .id(egui::Id::new("table-change-preview"))
            .open(&mut open)
            .resizable(true)
            .default_width(720.0)
            .show(ctx, |ui| {
                ui.label(format!(
                    "将原子提交：新增 {}，更新 {}，删除 {}",
                    summary.inserted, summary.updated, summary.deleted
                ));
                ui.separator();
                egui::ScrollArea::vertical()
                    .max_height(420.0)
                    .show(ui, |ui| {
                        for (statement_index, statement) in statements.iter().enumerate() {
                            ui.strong(format!(
                                "{}. {:?}",
                                statement_index + 1,
                                statement.kind
                            ));
                            ui.add(
                                egui::Label::new(
                                    egui::RichText::new(&statement.sql).monospace(),
                                )
                                .selectable(true)
                                .wrap(),
                            );
                            for (parameter_index, parameter) in
                                statement.parameters.iter().enumerate()
                            {
                                ui.monospace(format!(
                                    "  [{}] {} = {}",
                                    parameter_index + 1,
                                    parameter.database_type,
                                    parameter_value_label(&parameter.value)
                                ));
                            }
                            ui.add_space(10.0);
                        }
                    });
                ui.separator();
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(!self.operation_busy, egui::Button::new("确认应用"))
                        .clicked()
                    {
                        apply = true;
                    }
                    if ui.button("取消").clicked() {
                        self.show_table_change_preview = false;
                    }
                });
            });
        self.show_table_change_preview = open;
        if apply {
            self.apply_prepared_table_changes(ctx);
        }
    }

    fn render_workspace(&mut self, ui: &mut egui::Ui) {
        egui::Panel::top("query-tab")
            .exact_size(34.0)
            .show(ui, |ui| {
                ui.horizontal_centered(|ui| {
                    ui.strong(format!("查询 1 · {}", self.choice().name));
                });
            });
        egui::Panel::top("query-toolbar")
            .exact_size(44.0)
            .show(ui, |ui| {
                self.render_query_toolbar(ui);
            });
        egui::Panel::bottom("workspace-status")
            .exact_size(30.0)
            .show(ui, |ui| {
                ui.horizontal_centered(|ui| {
                    ui.add_space(8.0);
                    ui.weak(&self.status);
                });
            });
        egui::Panel::bottom("query-results")
            .resizable(true)
            .default_size(340.0)
            .min_size(170.0)
            .max_size(520.0)
            .show(ui, |ui| {
                self.render_results(ui);
            });
        egui::CentralPanel::no_frame().show(ui, |ui| {
            self.render_query_editor(ui);
        });
    }

    fn render_inspector(&self, ui: &mut egui::Ui) {
        let capabilities = self.capability_support();
        let languages = self
            .registry
            .get(self.choice().id)
            .map(|factory| {
                factory
                    .descriptor()
                    .capabilities
                    .query_languages()
                    .iter()
                    .map(|language| format!("{language:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();

        ui.heading("能力与上下文");
        ui.separator();
        capability_card(ui, "当前驱动", self.choice().name);
        capability_card(ui, "查询语言", &languages);
        capability_card(ui, "CRUD", yes_no(capabilities.crud));
        capability_card(ui, "表数据编辑", yes_no(capabilities.table_data));
        capability_card(ui, "执行计划", yes_no(capabilities.explain));
        capability_card(ui, "慢查询", yes_no(capabilities.slow_queries));
        ui.add_space(12.0);
        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.label(
                egui::RichText::new(
                    "SQL 写操作、MongoDB 写信封及 Redis 变更命令需要二次点击确认。",
                )
                .small()
                .weak(),
            );
        });
    }
}

impl eframe::App for DbcApp {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_query_events();
        self.drain_app_events(ctx);
        self.drain_query_events();
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        egui::Panel::top("dbc-top-bar")
            .exact_size(48.0)
            .show(ui, |ui| {
                self.render_top_bar(ui);
            });
        let mut left_panel_open = self.left_panel_open;
        let mut right_panel_open = self.right_panel_open;
        egui::Panel::left("dbc-left-panel")
            .resizable(true)
            .default_size(286.0)
            .size_range(220.0..=420.0)
            .show_collapsible(ui, &mut left_panel_open, |ui| {
                self.render_sidebar(ui);
            });
        egui::Panel::right("dbc-right-panel")
            .resizable(true)
            .default_size(236.0)
            .size_range(190.0..=360.0)
            .show_collapsible(ui, &mut right_panel_open, |ui| {
                self.render_inspector(ui);
            });
        self.left_panel_open = left_panel_open;
        self.right_panel_open = right_panel_open;
        egui::CentralPanel::no_frame().show(ui, |ui| {
            self.render_workspace(ui);
        });
        self.render_table_change_preview(ui.ctx());
    }
}

fn optional_input(value: &str) -> Option<String> {
    (!value.trim().is_empty()).then(|| value.trim().to_owned())
}

fn object_path_key(path: Option<&ObjectPath>) -> String {
    path.map(|path| path.segments.join("\u{1f}"))
        .unwrap_or_default()
}

const FILTER_OPERATORS: &[FilterOperator] = &[
    FilterOperator::Equals,
    FilterOperator::NotEquals,
    FilterOperator::GreaterThan,
    FilterOperator::GreaterThanOrEqual,
    FilterOperator::LessThan,
    FilterOperator::LessThanOrEqual,
    FilterOperator::Like,
    FilterOperator::NotLike,
    FilterOperator::In,
    FilterOperator::NotIn,
    FilterOperator::Between,
    FilterOperator::NotBetween,
    FilterOperator::IsNull,
    FilterOperator::IsNotNull,
];

fn object_table_ref(object: &DatabaseObject) -> Option<TableRef> {
    if !matches!(
        object.kind,
        DatabaseObjectKind::Table
            | DatabaseObjectKind::PartitionedTable
            | DatabaseObjectKind::View
            | DatabaseObjectKind::MaterializedView
            | DatabaseObjectKind::ForeignTable
    ) {
        return None;
    }
    let mut segments = object.path.segments.clone();
    let name = segments.pop()?;
    Some(TableRef::new(segments, name))
}

const fn filter_operator_label(operator: FilterOperator) -> &'static str {
    match operator {
        FilterOperator::Equals => "=",
        FilterOperator::NotEquals => "≠",
        FilterOperator::GreaterThan => ">",
        FilterOperator::GreaterThanOrEqual => "≥",
        FilterOperator::LessThan => "<",
        FilterOperator::LessThanOrEqual => "≤",
        FilterOperator::Like => "LIKE",
        FilterOperator::NotLike => "NOT LIKE",
        FilterOperator::In => "IN",
        FilterOperator::NotIn => "NOT IN",
        FilterOperator::Between => "BETWEEN",
        FilterOperator::NotBetween => "NOT BETWEEN",
        FilterOperator::IsNull => "IS NULL",
        FilterOperator::IsNotNull => "IS NOT NULL",
    }
}

const fn filter_operator_value_count(operator: FilterOperator) -> usize {
    match operator {
        FilterOperator::IsNull | FilterOperator::IsNotNull => 0,
        FilterOperator::Between | FilterOperator::NotBetween => 2,
        _ => 1,
    }
}

fn filter_values_label(values: &[CellValue]) -> String {
    values
        .iter()
        .map(compact_cell_value_label)
        .collect::<Vec<_>>()
        .join(", ")
}

fn parameter_value_label(value: &CellValue) -> String {
    match value {
        CellValue::Null => "NULL".to_owned(),
        CellValue::Default => "DEFAULT".to_owned(),
        CellValue::Binary(bytes) => {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            let mut value =
                String::with_capacity(bytes.len().saturating_mul(2).saturating_add(2));
            value.push_str("0x");
            for byte in bytes {
                value.push(char::from(HEX[usize::from(byte >> 4)]));
                value.push(char::from(HEX[usize::from(byte & 0x0f)]));
            }
            value
        }
        CellValue::Text(value) => format!("{value:?}"),
    }
}

fn compact_cell_value_label(value: &CellValue) -> String {
    let mut label = parameter_value_label(value);
    const LIMIT: usize = 80;
    if label.len() > LIMIT {
        let mut boundary = LIMIT;
        while !label.is_char_boundary(boundary) {
            boundary -= 1;
        }
        label.truncate(boundary);
        label.push('…');
    }
    label
}

const fn query_editability_reason_label(reason: QueryEditabilityReason) -> &'static str {
    match reason {
        QueryEditabilityReason::InvalidSql => "查询结果只读：SQL 无法解析",
        QueryEditabilityReason::MultipleStatements => "查询结果只读：包含多条 SQL 语句",
        QueryEditabilityReason::NotAQuery => "查询结果只读：不是 SELECT 查询",
        QueryEditabilityReason::Cte => "查询结果只读：包含 CTE",
        QueryEditabilityReason::SetOperation => {
            "查询结果只读：包含 UNION/INTERSECT/EXCEPT 等集合操作"
        }
        QueryEditabilityReason::MultipleSources => "查询结果只读：包含 JOIN 或多个数据源",
        QueryEditabilityReason::DerivedSource => "查询结果只读：数据源是子查询或表函数",
        QueryEditabilityReason::Distinct => "查询结果只读：包含 DISTINCT",
        QueryEditabilityReason::Aggregation => "查询结果只读：包含聚合或分组",
        QueryEditabilityReason::UnsupportedSelect => "查询结果只读：包含不支持的 SELECT 子句",
        QueryEditabilityReason::TableMismatch => "查询结果只读：元数据与数据源不匹配",
        QueryEditabilityReason::View => "查询结果只读：数据源是视图",
        QueryEditabilityReason::NoStableKey => {
            "查询结果只读：数据源没有非空主键或唯一键"
        }
        QueryEditabilityReason::PrimaryKeyNotReturned => {
            "查询结果只读：结果未返回完整的稳定键"
        }
    }
}

fn object_kind_icon(kind: &DatabaseObjectKind) -> &'static str {
    match kind {
        DatabaseObjectKind::Schema => "◫",
        DatabaseObjectKind::Table | DatabaseObjectKind::PartitionedTable => "▦",
        DatabaseObjectKind::View | DatabaseObjectKind::MaterializedView => "◉",
        DatabaseObjectKind::Column => "│",
        DatabaseObjectKind::Index => "⌕",
        DatabaseObjectKind::Collection => "◆",
        DatabaseObjectKind::Key => "◇",
        _ => "•",
    }
}

fn object_kind_label(kind: &DatabaseObjectKind) -> &'static str {
    match kind {
        DatabaseObjectKind::Schema => "模式/数据库",
        DatabaseObjectKind::Table => "表",
        DatabaseObjectKind::PartitionedTable => "分区表",
        DatabaseObjectKind::View => "视图",
        DatabaseObjectKind::MaterializedView => "物化视图",
        DatabaseObjectKind::ForeignTable => "外部表",
        DatabaseObjectKind::Sequence => "序列",
        DatabaseObjectKind::Column => "列",
        DatabaseObjectKind::Index => "索引",
        DatabaseObjectKind::Constraint => "约束",
        DatabaseObjectKind::Trigger => "触发器",
        DatabaseObjectKind::Routine => "存储过程/函数",
        DatabaseObjectKind::Collection => "集合",
        DatabaseObjectKind::Keyspace => "键空间",
        DatabaseObjectKind::Key => "键",
        DatabaseObjectKind::Other(_) => "其他对象",
    }
}

fn requires_confirmation(language: QueryLanguage, text: &str) -> bool {
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

fn mongo_requires_confirmation(value: &serde_json::Value) -> bool {
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

fn write_confirmation_matches(
    pending: Option<&PendingWriteConfirmation>,
    action: WriteAction,
    text: &str,
) -> bool {
    pending.is_some_and(|pending| pending.action == action && pending.text == text)
}

fn event_is_current(
    current_session_generation: u64,
    active_operation_id: Option<u64>,
    event_session_generation: u64,
    event_operation_id: u64,
) -> bool {
    current_session_generation == event_session_generation
        && active_operation_id == Some(event_operation_id)
}

fn capability_card(ui: &mut egui::Ui, label: &str, value: &str) {
    ui.weak(label);
    ui.strong(value);
    ui.add_space(8.0);
}

const fn yes_no(value: bool) -> &'static str {
    if value { "支持" } else { "不支持" }
}

fn page_count(row_count: usize, page_size: usize) -> usize {
    if page_size == 0 {
        return 1;
    }
    row_count.div_ceil(page_size).max(1)
}

fn page_after_page_size_change(
    current_page: usize,
    previous_page_size: usize,
    page_size: usize,
) -> usize {
    if page_size == 0 {
        return 0;
    }
    current_page.saturating_mul(previous_page_size) / page_size
}

fn compact_count(value: usize) -> String {
    if value >= 1_000 && value.is_multiple_of(1_000) {
        format!("{}k", value / 1_000)
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::{
        ActiveQueryEvents, BufferedQueryResult, DbcApp, ExportFormat, ExportScope, ObjectRow,
        OperationKind, PendingWriteConfirmation, QuerySettings, WriteAction, event_is_current,
        object_path_key, page_after_page_size_change, page_count, requires_confirmation,
        write_confirmation_matches,
    };
    use dbc_core::{
        capability::QueryLanguage,
        diagnostics::ExplainMode,
        driver::QueryEvent,
        metadata::{DatabaseObject, DatabaseObjectKind, ObjectPath},
        table_data::TableRef,
    };
    use dbc_data::{CellValue, DataBatch, DataSchema};
    use eframe::egui::accesskit::Role;
    use egui_kittest::{
        Harness,
        kittest::{NodeT as _, Queryable as _},
    };

    use crate::table_editor::EditorOrigin;

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
    fn object_paths_have_unambiguous_internal_keys() {
        assert_eq!(
            object_path_key(Some(&ObjectPath::new(["public", "items"]))),
            "public\u{1f}items"
        );
        assert_eq!(object_path_key(None), "");
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

    #[test]
    fn write_action_requires_a_matching_second_activation() {
        let mut app = DbcApp::new().expect("application should initialize");
        let text = "DELETE FROM items";

        assert!(!app.confirm_write(WriteAction::Execute, text));
        assert!(app.confirm_write(WriteAction::Execute, text));
        assert!(app.pending_write_confirmation.is_none());
    }

    #[test]
    fn pagination_counts_partial_and_empty_pages() {
        assert_eq!(page_count(0, 200), 1);
        assert_eq!(page_count(200, 200), 1);
        assert_eq!(page_count(201, 200), 2);
        assert_eq!(page_after_page_size_change(3, 200, 500), 1);
    }

    #[test]
    fn query_settings_use_the_fixed_default_limits() {
        let mut settings = QuerySettings::default();
        let current = BufferedQueryResult::new(settings.buffer_limits());
        settings.row_limit = 50_000;
        settings.memory_limit_mib = 128;
        let next = BufferedQueryResult::new(settings.buffer_limits());

        assert_eq!(settings.page_size, 200);
        assert_eq!(current.buffer.limits().max_rows, 10_000);
        assert_eq!(current.buffer.limits().max_bytes, 64 * 1024 * 1024);
        assert_eq!(next.buffer.limits().max_rows, 50_000);
        assert_eq!(next.buffer.limits().max_bytes, 128 * 1024 * 1024);
    }

    #[test]
    fn partial_query_result_remains_available_without_finished_event() {
        let mut result = BufferedQueryResult::new(QuerySettings::default().buffer_limits());
        result.apply(QueryEvent::Schema(DataSchema::Documents));
        result.apply(QueryEvent::Rows(DataBatch::Documents(vec![
            serde_json::json!({"id": 1}),
            serde_json::json!({"id": 2}),
        ])));

        assert_eq!(result.buffer.row_count(), 2);
        assert!(matches!(result.schema, Some(DataSchema::Documents)));
        assert!(result.stats.is_none());
    }

    #[test]
    fn query_stream_events_are_applied_before_completion() {
        let mut app = DbcApp::new().expect("application should initialize");
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        sender
            .try_send(QueryEvent::Schema(DataSchema::Documents))
            .expect("schema event should fit");
        sender
            .try_send(QueryEvent::Rows(DataBatch::Documents(vec![
                serde_json::json!({"id": 1}),
            ])))
            .expect("row event should fit");
        app.session_generation = 3;
        app.active_operation_id = Some(5);
        app.active_result = Some(BufferedQueryResult::new(
            QuerySettings::default().buffer_limits(),
        ));
        app.query_events = Some(ActiveQueryEvents {
            session_generation: 3,
            operation_id: 5,
            receiver,
        });

        app.drain_query_events();

        assert_eq!(
            app.active_result
                .as_ref()
                .expect("active result should remain")
                .buffer
                .row_count(),
            1
        );
    }

    #[test]
    fn cancelling_query_keeps_buffered_rows() {
        let mut app = DbcApp::new().expect("application should initialize");
        let mut result = BufferedQueryResult::new(QuerySettings::default().buffer_limits());
        result.apply(QueryEvent::Rows(DataBatch::Documents(vec![
            serde_json::json!({"id": 1}),
        ])));
        app.active_result = Some(result);
        let cancellation = tokio_util::sync::CancellationToken::new();
        app.begin_operation(9, OperationKind::Query, cancellation.clone());

        assert!(app.cancel_active_operation());
        assert!(cancellation.is_cancelled());
        assert_eq!(
            app.current_result
                .as_deref()
                .expect("partial result should be retained")
                .buffer
                .row_count(),
            1
        );
    }

    #[test]
    fn stale_session_and_operation_events_are_rejected() {
        assert!(event_is_current(7, Some(11), 7, 11));
        assert!(!event_is_current(7, Some(11), 6, 11));
        assert!(!event_is_current(7, Some(11), 7, 12));
        assert!(!event_is_current(7, None, 7, 11));
    }

    #[test]
    fn switching_driver_resets_connection_and_editor_defaults() {
        let mut app = DbcApp::new().expect("application should initialize");
        app.endpoint = "custom".to_owned();
        app.query_text = "custom query".to_owned();

        app.select_driver(3);

        assert_eq!(app.choice().id, "mongodb");
        assert_eq!(app.endpoint, "mongodb://127.0.0.1:27017");
        assert!(app.query_text.contains("\"operation\": \"find\""));
        assert!(app.password.is_empty());
    }

    #[test]
    fn collapsed_object_branch_hides_loaded_descendants() {
        let mut app = DbcApp::new().expect("application should initialize");
        app.objects = vec![
            object_row("public", 0, true),
            object_row("public.items", 1, true),
            object_row("public.items.id", 2, false),
            object_row("other", 0, false),
        ];
        app.expanded_object_keys
            .insert(object_path_key(Some(&ObjectPath::new(["public"]))));
        app.expanded_object_keys
            .insert(object_path_key(Some(&ObjectPath::new(["public", "items"]))));

        assert_eq!(app.visible_object_indices(), vec![0, 1, 2, 3]);

        app.expanded_object_keys
            .remove(&object_path_key(Some(&ObjectPath::new(["public"]))));
        assert_eq!(app.visible_object_indices(), vec![0, 3]);
    }

    #[test]
    fn minimum_and_default_windows_render_key_accessible_controls() {
        for size in [[960.0, 640.0], [1440.0, 900.0]] {
            let mut harness = Harness::builder()
                .with_size(size)
                .build_eframe(|_| DbcApp::new().expect("application should initialize"));
            harness.run();

            assert!(harness.get_by_label("执行").accesskit_node().is_disabled());
            assert!(!harness.get_by_label("连接").accesskit_node().is_disabled());
            let _endpoint = harness.get_by_label("连接地址");
            let _result_tab = harness.get_by_label("数据");

            harness.get_by_label("能力").click();
            harness.run();
            assert!(!harness.state().right_panel_open);
        }
    }

    #[test]
    fn sqlite_desktop_workflow_is_closed_loop() {
        let mut harness = Harness::builder()
            .with_size([1440.0, 900.0])
            .build_eframe(|_| DbcApp::new().expect("application should initialize"));
        let ctx = harness.ctx.clone();
        harness.state_mut().select_driver(2);
        harness.state_mut().connect(&ctx);

        wait_for_app(
            &mut harness,
            |app| !app.connection_busy,
            "SQLite connection",
        );
        assert!(harness.state().session.is_some());
        wait_for_app(
            &mut harness,
            |app| app.loaded_object_keys.contains(""),
            "initial object discovery",
        );
        assert!(!harness.get_by_label("执行").accesskit_node().is_disabled());
        assert!(!harness.get_by_label("断开").accesskit_node().is_disabled());
        assert!(!toolbar_button_is_disabled(&harness, "执行计划"));
        assert!(toolbar_button_is_disabled(&harness, "分析执行"));
        assert!(toolbar_button_is_disabled(&harness, "慢查询"));

        execute_query(
            &mut harness,
            "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            true,
        );
        execute_query(
            &mut harness,
            "INSERT INTO items (name) VALUES ('alpha'), ('beta')",
            true,
        );
        execute_query(
            &mut harness,
            "SELECT id, name FROM items ORDER BY id",
            false,
        );
        let result = harness
            .state()
            .current_result
            .as_deref()
            .expect("SELECT should retain a current result");
        assert_eq!(result.buffer.row_count(), 2);
        assert!(harness.state().status.starts_with("执行完成"));

        harness.state_mut().query_text = "SELECT 1; SELECT 2".to_owned();
        harness.state_mut().execute(&ctx);
        assert!(harness.state().pending_write_confirmation.is_some());
        harness.state_mut().execute(&ctx);
        assert!(harness.state().operation_busy);
        wait_for_app(
            &mut harness,
            |app| !app.operation_busy,
            "invalid query failure",
        );
        assert!(harness.state().status.starts_with("执行失败"));
        assert!(harness.state().active_cancellation.is_none());

        execute_query(
            &mut harness,
            "SELECT id, name FROM items ORDER BY id",
            false,
        );
        assert_eq!(
            harness
                .state()
                .current_result
                .as_deref()
                .expect("valid query should recover after an error")
                .buffer
                .row_count(),
            2
        );
        assert!(
            !harness
                .get_by_label("导出当前")
                .accesskit_node()
                .is_disabled()
        );
        assert!(
            !harness
                .get_by_label("完整导出")
                .accesskit_node()
                .is_disabled()
        );

        {
            let app = harness.state_mut();
            app.loaded_object_keys.remove("");
            app.load_objects(None, 0, &ctx);
        }
        wait_for_app(
            &mut harness,
            |app| app.objects.iter().any(|row| row.object.name == "items"),
            "object tree refresh",
        );
        let table_index = harness
            .state()
            .objects
            .iter()
            .position(|row| row.object.name == "items")
            .expect("created table should appear in object tree");
        harness.state_mut().activate_object(table_index, &ctx);
        wait_for_app(
            &mut harness,
            |app| {
                app.objects
                    .iter()
                    .any(|row| row.depth == 1 && row.object.name == "name")
            },
            "lazy child discovery",
        );

        harness
            .state_mut()
            .open_table(TableRef::new(Vec::<String>::new(), "items"), &ctx);
        wait_for_app(
            &mut harness,
            |app| {
                !app.operation_busy
                    && app
                        .table_editor
                        .as_ref()
                        .is_some_and(|editor| editor.origin() == EditorOrigin::Table)
            },
            "table data load",
        );
        {
            let editor = harness
                .state_mut()
                .table_editor
                .as_mut()
                .expect("table editor should be available");
            assert_eq!(editor.total_rows(), 2);
            assert!(editor.set_cell(
                0,
                1,
                CellValue::Text("alpha-edited".to_owned())
            ));
        }
        harness.state_mut().query_text = "SELECT 1".to_owned();
        harness.state_mut().execute(&ctx);
        assert!(!harness.state().operation_busy);
        assert!(harness.state().status.contains("未保存变更"));

        harness.state_mut().preview_table_changes(&ctx);
        wait_for_app(
            &mut harness,
            |app| !app.operation_busy && app.prepared_table_change.is_some(),
            "parameterized SQL preview",
        );
        let prepared = harness
            .state()
            .prepared_table_change
            .as_ref()
            .expect("change preview should be retained");
        assert_eq!(prepared.plan.statements.len(), 1);
        assert!(prepared.plan.statements[0].sql.contains('?'));
        assert!(!prepared.plan.statements[0].sql.contains("alpha-edited"));
        assert_eq!(
            prepared.plan.statements[0].parameters[0].value,
            CellValue::Text("alpha-edited".to_owned())
        );

        harness.state_mut().apply_prepared_table_changes(&ctx);
        wait_for_app(
            &mut harness,
            |app| {
                !app.operation_busy
                    && app
                        .table_editor
                        .as_ref()
                        .is_some_and(|editor| {
                            editor.origin() == EditorOrigin::Table
                                && editor.pending_change_count() == 0
                        })
            },
            "table change apply and reload",
        );
        execute_query(
            &mut harness,
            "SELECT id, name FROM items ORDER BY id",
            false,
        );
        wait_for_app(
            &mut harness,
            |app| {
                app.table_editor
                    .as_ref()
                    .is_some_and(|editor| editor.origin() == EditorOrigin::Query)
            },
            "single-table query editability",
        );

        harness.state_mut().query_text = "SELECT id, name FROM items WHERE id = 1".to_owned();
        harness
            .state_mut()
            .start_explain(ExplainMode::Estimated, &ctx);
        assert!(harness.state().operation_busy);
        wait_for_app(
            &mut harness,
            |app| !app.operation_busy,
            "estimated execution plan",
        );
        let plan: serde_json::Value =
            serde_json::from_str(&harness.state().plan_text).expect("SQLite plan should be JSON");
        assert!(plan.is_array());

        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let export_path = directory.path().join("items.csv");
        harness.state_mut().start_export(
            ExportScope::Current,
            ExportFormat::Csv,
            export_path.clone(),
            &ctx,
        );
        assert!(harness.state().operation_busy);
        wait_for_app(
            &mut harness,
            |app| !app.operation_busy,
            "current result export",
        );
        let export =
            std::fs::read_to_string(&export_path).expect("exported CSV should be readable");
        assert!(export.starts_with("id,name\n"));
        assert!(export.contains("1,alpha-edited\n"));
        assert!(export.contains("2,beta\n"));

        let full_export_path = directory.path().join("items.jsonl");
        harness.state_mut().start_export(
            ExportScope::Full,
            ExportFormat::JsonLines,
            full_export_path.clone(),
            &ctx,
        );
        assert!(harness.state().operation_busy);
        wait_for_app(&mut harness, |app| !app.operation_busy, "full query export");
        let full_export = std::fs::read_to_string(&full_export_path)
            .expect("full JSONL export should be readable");
        assert_eq!(full_export.lines().count(), 3);
        assert!(full_export.contains(r#""type":"schema""#));
        assert!(
            full_export.contains(r#""values":["1","alpha-edited"]"#),
            "unexpected full export: {full_export}"
        );

        harness.state_mut().disconnect();
        harness.step();
        assert!(harness.state().session.is_none());
        assert!(harness.state().objects.is_empty());
        assert_eq!(harness.state().status, "连接已断开");
        assert!(harness.get_by_label("执行").accesskit_node().is_disabled());
    }

    fn execute_query(
        harness: &mut Harness<'_, DbcApp>,
        text: &str,
        requires_write_confirmation: bool,
    ) {
        let ctx = harness.ctx.clone();
        harness.state_mut().query_text = text.to_owned();
        harness.state_mut().execute(&ctx);
        if requires_write_confirmation {
            assert!(!harness.state().operation_busy);
            assert!(harness.state().pending_write_confirmation.is_some());
            harness.state_mut().execute(&ctx);
        }
        assert!(harness.state().operation_busy);
        wait_for_app(harness, |app| !app.operation_busy, "query completion");
        assert!(
            !harness.state().status.contains("失败"),
            "query failed: {}",
            harness.state().status
        );
    }

    fn wait_for_app(
        harness: &mut Harness<'_, DbcApp>,
        mut predicate: impl FnMut(&DbcApp) -> bool,
        operation: &str,
    ) {
        for _ in 0..500 {
            harness.step();
            if predicate(harness.state()) {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!(
            "timed out waiting for {operation}; last status: {}",
            harness.state().status
        );
    }

    fn toolbar_button_is_disabled(harness: &Harness<'_, DbcApp>, label: &'static str) -> bool {
        harness
            .get_all_by_role_and_label(Role::Button, label)
            .into_iter()
            .find(|node| node.accesskit_node().toggled().is_none())
            .unwrap_or_else(|| panic!("toolbar button should exist: {label}"))
            .accesskit_node()
            .is_disabled()
    }

    fn object_row(path: &str, depth: usize, has_children: bool) -> ObjectRow {
        let segments = path.split('.').collect::<Vec<_>>();
        ObjectRow {
            object: DatabaseObject {
                id: path.to_owned(),
                name: segments.last().copied().unwrap_or_default().to_owned(),
                path: ObjectPath::new(segments),
                kind: if has_children {
                    DatabaseObjectKind::Schema
                } else {
                    DatabaseObjectKind::Column
                },
                has_children,
                properties: Default::default(),
            },
            depth,
        }
    }
}
