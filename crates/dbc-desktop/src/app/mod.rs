use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

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
        QuerySourceAnalysis, analyze_query_source,
        resolve_query_editability,
    },
    sql::SqlDialect,
    table_data::{
        FilterOperator, SortDirection, TableApplyResult, TableBrowseRequest, TableChangePlan,
        TableFilter, TableMetadata, TablePage, TableRef, TableSort,
    },
};
use dbc_core::result::{BufferLimits, DataSchema, ResultBuffer};
use dbc_drivers::builtin_factories;
use crate::file_picker::{FilePicker, PickerOutcome};
use crate::labels::{
    FILTER_OPERATORS, compact_count, driver_display_name,
    filter_operator_label, filter_operator_value_count, filter_values_label, history_label,
    object_kind_icon, object_kind_label, parameter_value_label,
    query_editability_reason_label,
};
use crate::paging::{page_after_page_size_change, page_count};
use crate::palette::{Palette, PaletteEntry, PaletteOutcome};
use crate::write_guard::{
    PendingWriteConfirmation, WriteAction, requires_confirmation, write_confirmation_matches,
};
use crate::shortcuts::{self, Shortcut};
use crate::store::{SavedConnection, Store, secrets::Vault};
use crate::vault_prompt::{PromptOutcome, VaultPrompt, VaultPurpose};
use crate::status::Status;
use crate::tasks::{RuntimeConfig, TaskError, TaskRuntime};
use eframe::egui;
use egui_code_editor::{CodeEditor, ColorTheme, Completer};
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
/// One object page. Branches larger than this expose a `加载更多` row.
const OBJECT_PAGE_SIZE: usize = 500;
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

/// Identifies a workspace tab across asynchronous completions.
///
/// Results are addressed by id rather than by index so closing or reordering a
/// tab while a query is in flight can never deliver rows to the wrong one.
type TabId = u64;

/// Everything that belongs to one query or one browsed table.
///
/// Each tab owns its operation slot, so a long query no longer blocks reading a
/// second table in another tab.
struct Tab {
    id: TabId,
    /// Driver name by default; the table name once a table is opened.
    title: String,
    query_events: Option<ActiveQueryEvents>,
    query_text: String,
    data_grid: ResultGridState,
    slow_grid: ResultGridState,
    result_tab: ResultTab,
    plan_text: String,
    operation_busy: bool,
    active_cancellation: Option<CancellationToken>,
    active_operation_id: Option<u64>,
    active_operation_kind: Option<OperationKind>,
    pending_write_confirmation: Option<PendingWriteConfirmation>,
    active_result: Option<BufferedQueryResult>,
    current_result: Option<Arc<BufferedQueryResult>>,
    current_page: usize,
    last_query: Option<QuerySnapshot>,
    table_editor: Option<TableEditorState>,
    table_editor_generation: u64,
    table_browse: Option<TableBrowseControls>,
    prepared_table_change: Option<PreparedTableChange>,
    show_table_change_preview: bool,
    query_read_only_reason: Option<String>,
}

impl Tab {
    /// Drop everything that belonged to the previous session, keeping the SQL
    /// the user typed.
    fn reset_results(&mut self) {
        self.query_events = None;
        self.result_tab = ResultTab::Data;
        self.plan_text = "尚未生成执行计划".to_owned();
        self.operation_busy = false;
        self.active_cancellation = None;
        self.active_operation_id = None;
        self.active_operation_kind = None;
        self.pending_write_confirmation = None;
        self.active_result = None;
        self.current_result = None;
        self.current_page = 0;
        self.last_query = None;
        self.table_editor = None;
        self.table_browse = None;
        self.prepared_table_change = None;
        self.show_table_change_preview = false;
        self.query_read_only_reason = None;
        self.data_grid.replace(ResultModel::message(
            "连接数据库后执行查询，结果将在这里显示",
        ));
    }

    fn new(id: TabId, title: impl Into<String>, query_text: String) -> Self {
        Self {
            id,
            title: title.into(),
            query_events: None,
            query_text,
            data_grid: ResultGridState::new(ResultModel::message(
                "执行查询后，结果将在这里显示",
            )),
            slow_grid: ResultGridState::new(ResultModel::message(
                "点击“慢查询”读取数据库原生统计",
            )),
            result_tab: ResultTab::Data,
            plan_text: "尚未生成执行计划".to_owned(),
            operation_busy: false,
            active_cancellation: None,
            active_operation_id: None,
            active_operation_kind: None,
            pending_write_confirmation: None,
            active_result: None,
            current_result: None,
            current_page: 0,
            last_query: None,
            table_editor: None,
            table_editor_generation: 0,
            table_browse: None,
            prepared_table_change: None,
            show_table_change_preview: false,
            query_read_only_reason: None,
        }
    }
}


#[derive(Debug, Clone, Copy)]
struct CapabilitySupport {
    explain: bool,
    analyze: bool,
    slow_queries: bool,
    table_data: bool,
}

pub struct DbcApp {
    registry: Arc<DriverRegistry>,
    /// Open workspace tabs; index `active_tab` is the one on screen.
    tabs: Vec<Tab>,
    active_tab: usize,
    next_tab_id: TabId,
    runtime: Arc<TaskRuntime>,
    event_sender: mpsc::UnboundedSender<AppEvent>,
    event_receiver: mpsc::UnboundedReceiver<AppEvent>,
    selected_driver: usize,
    endpoint: String,
    database: String,
    user: String,
    password: String,
    password_visible: bool,
    session: Option<Arc<dyn DatabaseSession>>,
    objects: Vec<ObjectRow>,
    expanded_object_keys: BTreeSet<String>,
    loaded_object_keys: BTreeSet<String>,
    loading_object_keys: BTreeSet<String>,
    /// Continuation cursor per already-loaded branch. Without it the
    /// first page of objects was all the user could ever see.
    object_cursors: BTreeMap<String, String>,
    status: Status,
    /// Open detail view for the current status message.
    status_detail_open: bool,
    connection_busy: bool,
    session_generation: u64,
    operation_generation: u64,
    query_settings: QuerySettings,
    left_panel_open: bool,
    store: Store,
    /// Unlocked credential vault; `None` while locked or not yet created.
    vault: Option<Vault>,
    vault_prompt: Option<VaultPrompt>,
    /// Name under which the current form is saved.
    connection_name: String,
    save_password: bool,
    selected_saved: Option<usize>,
    testing_connection: bool,
    /// Keyword and identifier dictionary for the query editor; rebuilt when the
    /// driver changes or the object tree grows.
    completer: Completer,
    palette: Option<Palette>,
    file_picker: Option<FilePicker>,
    /// Export that is waiting for the user to choose a destination.
    pending_export: Option<(ExportScope, ExportFormat)>,
    /// Last directory the picker was in, so repeat exports start where the
    /// previous one did.
    export_directory: PathBuf,
}

impl DbcApp {
    /// Build the application.
    ///
    /// The [`Store`] is injected rather than opened here so tests never touch
    /// the real per-user configuration directory. `missing_cjk_font` reports
    /// that no Chinese-capable system font was found, so the user is told
    /// instead of meeting tofu glyphs.
    pub fn new(store: Store, missing_cjk_font: bool) -> Result<Self> {
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
        let preferences = store.settings().ui.clone();
        let export_directory = crate::store::default_export_directory(&preferences);
        let query_settings = QuerySettings {
            page_size: preferences.page_size,
            row_limit: preferences.max_buffered_rows,
            memory_limit_mib: preferences.max_buffered_bytes / (1024 * 1024),
        };

        let first_tab = Tab::new(0, choice.name, choice.query.to_owned());
        Ok(Self {
            registry: Arc::new(registry),
            tabs: vec![first_tab],
            active_tab: 0,
            next_tab_id: 1,
            runtime: Arc::new(runtime),
            event_sender,
            event_receiver,
            selected_driver: 0,
            endpoint: choice.endpoint.to_owned(),
            database: choice.database.to_owned(),
            user: choice.user.to_owned(),
            password: String::new(),
            password_visible: false,
            session: None,
            objects: Vec::new(),
            expanded_object_keys: BTreeSet::new(),
            loaded_object_keys: BTreeSet::new(),
            loading_object_keys: BTreeSet::new(),
            object_cursors: BTreeMap::new(),
            status: if missing_cjk_font {
                Status::error("未找到中文字体，界面可能显示为方块；Linux 可安装 fonts-noto-cjk")
            } else {
                Status::info("请选择数据库并建立连接")
            },
            status_detail_open: false,
            connection_busy: false,
            session_generation: 0,
            operation_generation: 0,
            query_settings,
            left_panel_open: true,
            store,
            vault: None,
            vault_prompt: None,
            connection_name: String::new(),
            save_password: false,
            selected_saved: None,
            testing_connection: false,
            completer: Completer::new_with_syntax(&choice.editor_syntax.syntax())
                .with_auto_indent(),
            palette: None,
            file_picker: None,
            pending_export: None,
            export_directory,
        })
    }

    /// The tab currently on screen.
    fn tab(&self) -> &Tab {
        &self.tabs[self.active_tab]
    }

    fn tab_mut(&mut self) -> &mut Tab {
        &mut self.tabs[self.active_tab]
    }

    fn tab_index(&self, id: TabId) -> Option<usize> {
        self.tabs.iter().position(|tab| tab.id == id)
    }

    /// Run `body` with the tab that owns `id` selected, then restore the
    /// on-screen tab.
    ///
    /// Completions are addressed by tab id, so switching tabs — or opening a
    /// new one — while a query is in flight can never deliver its rows to
    /// whatever tab happens to be visible. A completion for a closed tab is
    /// dropped.
    fn with_tab<R>(&mut self, id: TabId, body: impl FnOnce(&mut Self) -> R) -> Option<R> {
        let index = self.tab_index(id)?;
        let previous = self.active_tab;
        self.active_tab = index;
        let result = body(self);
        // The tab list only grows or shrinks between frames, never inside
        // `body`, so the saved index stays valid.
        self.active_tab = previous.min(self.tabs.len().saturating_sub(1));
        Some(result)
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
            table_data: capabilities.supports_table_data(),
        }
    }

    fn select_driver(&mut self, index: usize) {
        if self.connection_busy
            || self.tab().operation_busy
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
        // A different driver means a different query language, so the open tabs
        // cannot carry over. Collapsing to one fresh tab is predictable;
        // silently keeping SQL text in a Redis tab would not be.
        let id = self.next_tab_id;
        self.next_tab_id = self.next_tab_id.saturating_add(1);
        self.tabs = vec![Tab::new(id, "查询 1", choice.query.to_owned())];
        self.active_tab = 0;
        self.reset_session_view();
        self.rebuild_completer();
        self.status = Status::info(format!("已选择 {}，请检查连接参数", choice.name));
    }










    fn connect(&mut self, ctx: &egui::Context) {
        if self.connection_busy
            || self.tab().operation_busy
            || !self.allow_pending_navigation("重新连接")
        {
            return;
        }
        let choice = self.choice();
        let Some(factory) = self.registry.get(choice.id) else {
            self.status = Status::error(format!("驱动 {} 未注册", choice.id));
            return;
        };
        if self.endpoint.trim().is_empty() {
            self.status = Status::error("连接地址不能为空");
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
        self.status = Status::progress(format!("正在连接 {}…", choice.name));
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
        self.status = Status::info("连接已断开");
    }

    fn execute(&mut self, ctx: &egui::Context) {
        // Completions are addressed by tab id, not by whatever tab is visible.
        let tab = self.tab().id;
        if self.tab().operation_busy || !self.allow_pending_navigation("执行新查询") {
            return;
        }
        let Some(session) = self.session.clone() else {
            self.status = Status::error("请先建立数据库连接");
            return;
        };
        let text = self.tab().query_text.clone();
        if text.trim().is_empty() {
            self.status = Status::error("查询内容不能为空");
            return;
        }
        if !self.confirm_write(WriteAction::Execute, &text) {
            return;
        }
        let driver_id = self.choice().id;
        self.store.remember_query(driver_id, &text);
        self.save_settings();

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
        self.tab_mut().last_query = Some(QuerySnapshot {
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
                    tab,
                    session_generation,
                    operation_id,
                    result,
                });
                completion_repaint.request_repaint();
            },
        );

        self.tab_mut().query_events = Some(ActiveQueryEvents {
            session_generation,
            operation_id,
            receiver: query_receiver,
        });
        self.begin_operation(operation_id, OperationKind::Query, cancellation);
        self.status = Status::progress("正在执行…");
        self.tab_mut().result_tab = ResultTab::Data;
        self.tab_mut().current_page = 0;
        self.tab_mut().current_result = None;
        self.tab_mut().active_result = Some(BufferedQueryResult::new(settings.buffer_limits()));
        self.tab_mut().table_editor = None;
        self.tab_mut().table_browse = None;
        self.invalidate_table_change_plan();
        self.tab_mut().query_read_only_reason = None;
        self.tab_mut().data_grid
            .replace(ResultModel::message("正在接收查询结果…"));
    }

    fn start_explain(&mut self, mode: ExplainMode, ctx: &egui::Context) {
        // Completions are addressed by tab id, not by whatever tab is visible.
        let tab = self.tab().id;
        if self.tab().operation_busy {
            return;
        }
        let Some(session) = self.session.clone() else {
            self.status = Status::error("请先建立数据库连接");
            return;
        };
        let text = self.tab().query_text.clone();
        if text.trim().is_empty() {
            self.status = Status::error("查询内容不能为空");
            return;
        }
        if mode == ExplainMode::Analyze {
            if !self.confirm_write(WriteAction::Analyze, &text) {
                return;
            }
        } else {
            self.tab_mut().pending_write_confirmation = None;
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
                        tab,
                        session_generation,
                        operation_id,
                        result,
                    });
                    repaint.request_repaint();
                },
            );
        self.begin_operation(operation_id, OperationKind::Explain, cancellation);
        self.tab_mut().result_tab = ResultTab::Plan;
        self.status = Status::progress(match mode {
            ExplainMode::Estimated => "正在生成执行计划…".to_owned(),
            ExplainMode::Analyze => "正在执行并分析查询…".to_owned(),
        });
    }

    fn load_slow_queries(&mut self, ctx: &egui::Context) {
        // Completions are addressed by tab id, not by whatever tab is visible.
        let tab = self.tab().id;
        if self.tab().operation_busy {
            return;
        }
        let Some(session) = self.session.clone() else {
            self.status = Status::error("请先建立数据库连接");
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
                    tab,
                    session_generation,
                    operation_id,
                    result,
                });
                repaint.request_repaint();
            },
        );
        self.begin_operation(operation_id, OperationKind::SlowQueries, cancellation);
        self.tab_mut().result_tab = ResultTab::SlowQueries;
        self.status = Status::progress("正在读取慢查询统计…");
    }

    fn request_export(&mut self, scope: ExportScope, format: ExportFormat, ctx: &egui::Context) {
        if self.tab().operation_busy {
            return;
        }
        match scope {
            ExportScope::Current => {
                if self
                    .tab()
                    .current_result
                    .as_deref()
                    .and_then(|result| result.schema.as_ref())
                    .is_none()
                {
                    self.status = Status::error("当前没有可导出的结果集");
                    return;
                }
            }
            ExportScope::Full => {
                let Some(snapshot) = self.tab().last_query.as_ref() else {
                    self.status = Status::error("没有可重新执行的查询");
                    return;
                };
                if self.session.is_none() {
                    self.status = Status::error("完整导出需要保持数据库连接");
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
        // The picker is drawn inside the egui frame rather than blocking on a
        // native dialog, so `start_export` runs once the user confirms.
        let _ = ctx;
        self.file_picker = Some(FilePicker::save(
            format!("导出为 {}", format.label()),
            self.export_directory.clone(),
            suggested_name,
            format.extension(),
        ));
        self.pending_export = Some((scope, format));
    }



    fn start_export(
        &mut self,
        scope: ExportScope,
        format: ExportFormat,
        path: PathBuf,
        ctx: &egui::Context,
    ) {
        // Completions are addressed by tab id, not by whatever tab is visible.
        let tab = self.tab().id;
        if self.tab().operation_busy {
            return;
        }
        let session_generation = self.session_generation;
        let operation_id = self.next_operation_id();
        let path_for_event = path.clone();
        let event_sender = self.event_sender.clone();
        let repaint = ctx.clone();
        let report = move |result| {
            let _sent = event_sender.send(AppEvent::ExportFinished {
                tab,
                session_generation,
                operation_id,
                path: path_for_event,
                result,
            });
            repaint.request_repaint();
        };

        let cancellation = match scope {
            ExportScope::Current => {
                let Some(result) = self.tab().current_result.clone() else {
                    self.status = Status::error("当前结果已不可用");
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
                    self.status = Status::error("完整导出需要保持数据库连接");
                    return;
                };
                let Some(snapshot) = self.tab().last_query.clone() else {
                    self.status = Status::error("没有可重新执行的查询");
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
        self.status = Status::progress(match scope {
            ExportScope::Current => {
                format!("正在导出当前缓冲结果为 {}…", format.label())
            }
            ExportScope::Full => {
                format!("正在重新执行并完整导出为 {}…", format.label())
            }
        });
    }

    /// Load one page of children. `cursor` continues a branch that was
    /// truncated by the page limit instead of starting it over.
    fn load_objects(
        &mut self,
        parent: Option<ObjectPath>,
        depth: usize,
        cursor: Option<String>,
        ctx: &egui::Context,
    ) {
        let Some(session) = self.session.clone() else {
            return;
        };
        let key = object_path_key(parent.as_ref());
        if self.loading_object_keys.contains(&key) {
            return;
        }
        // A continuation revisits a branch that is already marked as loaded.
        if cursor.is_none() && self.loaded_object_keys.contains(&key) {
            return;
        }
        self.loading_object_keys.insert(key.clone());
        let appending = cursor.is_some();
        let request = ObjectListRequest {
            id: Uuid::new_v4(),
            parent: parent.clone(),
            include_system: false,
            limit: OBJECT_PAGE_SIZE,
            cursor,
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
                    appending,
                    result,
                });
                repaint.request_repaint();
            },
        );
    }

    fn allow_pending_navigation(&mut self, action: &str) -> bool {
        let pending = self
            .tab()
            .table_editor
            .as_ref()
            .map_or(0, TableEditorState::pending_change_count);
        if pending == 0 {
            return true;
        }
        self.status = Status::error(format!(
            "有 {pending} 行未保存变更；请先应用或放弃变更，再{action}"
        ));
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

    /// Open a table in its own tab.
    ///
    /// A table used to replace whatever the active tab held, so opening one
    /// threw away the query being written. An already-open table is raised
    /// instead of duplicated.
    fn open_table(&mut self, table: TableRef, ctx: &egui::Context) {
        if !self.capability_support().table_data {
            return;
        }
        if let Some(index) = self
            .tabs
            .iter()
            .position(|tab| tab.table_browse.as_ref().is_some_and(|browse| browse.table == table))
        {
            self.active_tab = index;
            return;
        }

        let id = self.next_tab_id;
        self.next_tab_id = self.next_tab_id.saturating_add(1);
        let query_text = self.choice().query.to_owned();
        self.tabs.push(Tab::new(id, table.name.clone(), query_text));
        self.active_tab = self.tabs.len() - 1;

        self.tab_mut().table_browse = Some(TableBrowseControls::new(table));
        self.tab_mut().table_editor = None;
        self.tab_mut().current_result = None;
        self.tab_mut().active_result = None;
        self.tab_mut().last_query = None;
        self.tab_mut().query_read_only_reason = None;
        self.invalidate_table_change_plan();
        self.load_active_table_page(0, ctx);
    }

    fn load_active_table_page(&mut self, page_index: u64, ctx: &egui::Context) {
        // Completions are addressed by tab id, not by whatever tab is visible.
        let tab = self.tab().id;
        if self.tab().operation_busy || !self.allow_pending_navigation("重新加载数据") {
            return;
        }
        let Some(session) = self.session.clone() else {
            self.status = Status::error("请先建立数据库连接");
            return;
        };
        let Some(controls) = self.tab().table_browse.clone() else {
            return;
        };
        let page_size = match u32::try_from(self.query_settings.page_size) {
            Ok(page_size) => page_size,
            Err(_) => {
                self.status = Status::error("分页大小超出驱动支持范围");
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
                    tab,
                    session_generation,
                    operation_id,
                    result,
                });
                repaint.request_repaint();
            },
        );
        self.begin_operation(operation_id, OperationKind::TableLoad, cancellation);
        self.tab_mut().result_tab = ResultTab::Data;
        self.status = Status::progress(format!("正在读取 {table_name} 第 {} 页…", page_index + 1));
    }

    fn prepare_query_editor(&mut self, operation_id: u64, ctx: &egui::Context) {
        // Completions are addressed by tab id, not by whatever tab is visible.
        let tab = self.tab().id;
        let Some(snapshot) = self
            .tab()
            .last_query
            .as_ref()
            .filter(|snapshot| snapshot.operation_id == operation_id)
        else {
            return;
        };
        let Some(analysis) = snapshot.source_analysis.clone() else {
            self.tab_mut().query_read_only_reason = Some("当前查询语言不支持表格式编辑".to_owned());
            return;
        };
        if let Some(reason) = analysis.reason {
            self.tab_mut().query_read_only_reason =
                Some(query_editability_reason_label(reason).to_owned());
            return;
        }
        let Some(table) = analysis.table.clone() else {
            self.tab_mut().query_read_only_reason = Some("无法确定查询的数据源表".to_owned());
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
                    tab,
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
        // Completions are addressed by tab id, not by whatever tab is visible.
        let tab = self.tab().id;
        if self.tab().operation_busy {
            return;
        }
        let Some(session) = self.session.clone() else {
            self.status = Status::error("数据库连接已断开");
            return;
        };
        let Some(editor) = self.tab().table_editor.as_ref() else {
            return;
        };
        let request = match editor.change_request(OPERATION_TIMEOUT) {
            Ok(request) if !request.is_empty() => request,
            Ok(_) => {
                self.status = Status::info("没有待提交的表数据变更");
                return;
            }
            Err(error) => {
                self.status = Status::error(format!("无法生成变更：{error}"));
                return;
            }
        };
        let editor_generation = self.tab().table_editor_generation;
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
                    tab,
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
        self.status = Status::progress("正在生成参数化 SQL 预览…");
    }

    fn apply_prepared_table_changes(&mut self, ctx: &egui::Context) {
        // Completions are addressed by tab id, not by whatever tab is visible.
        let tab = self.tab().id;
        if self.tab().operation_busy {
            return;
        }
        let Some(prepared) = self.tab().prepared_table_change.as_ref() else {
            return;
        };
        let Some(editor) = self.tab().table_editor.as_ref() else {
            return;
        };
        if prepared.editor_generation != self.tab().table_editor_generation
            || prepared.revision != editor.revision()
        {
            self.invalidate_table_change_plan();
            self.status = Status::info("数据已继续修改，请重新生成 SQL 预览");
            return;
        }
        let Some(session) = self.session.clone() else {
            self.status = Status::error("数据库连接已断开");
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
                    tab,
                    session_generation,
                    operation_id,
                    editor_generation,
                    result,
                });
                repaint.request_repaint();
            },
        );
        self.begin_operation(operation_id, OperationKind::TableApply, cancellation);
        self.status = Status::progress("正在原子提交表数据变更…");
    }

    fn invalidate_table_change_plan(&mut self) {
        self.tab_mut().prepared_table_change = None;
        self.tab_mut().show_table_change_preview = false;
    }

    fn activate_object(&mut self, index: usize, ctx: &egui::Context) {
        let Some(row) = self.objects.get(index).cloned() else {
            return;
        };
        if row.object.has_children {
            let key = object_path_key(Some(&row.object.path));
            if self.expanded_object_keys.remove(&key) {
                self.status = Status::info(format!("已收起 {}", row.object.name));
                return;
            }
            self.expanded_object_keys.insert(key);
            self.load_objects(Some(row.object.path), row.depth.saturating_add(1), None, ctx);
        } else {
            self.status = Status::info(format!(
                "{} · {}",
                row.object.name,
                object_kind_label(&row.object.kind)
            ));
        }
    }

    /// Where a freshly loaded page belongs.
    ///
    /// A continuation goes after the branch's existing children so the order
    /// the database returned is preserved; a first page goes directly under
    /// its parent.
    fn branch_insertion_point(
        &self,
        parent: Option<&ObjectPath>,
        appending: bool,
    ) -> Option<usize> {
        let Some(parent) = parent else {
            return appending.then_some(self.objects.len());
        };
        let parent_key = object_path_key(Some(parent));
        let parent_index = self
            .objects
            .iter()
            .position(|row| object_path_key(Some(&row.object.path)) == parent_key)?;
        if !appending {
            return Some(parent_index + 1);
        }
        let parent_depth = self.objects[parent_index].depth;
        let mut index = parent_index + 1;
        while self
            .objects
            .get(index)
            .is_some_and(|row| row.depth > parent_depth)
        {
            index += 1;
        }
        Some(index)
    }

    /// Reload the object tree from the root.
    ///
    /// Without this the tree could never show objects created after the branch
    /// was first loaded — the only workaround was reconnecting.
    fn refresh_objects(&mut self, ctx: &egui::Context) {
        if self.session.is_none() {
            self.status = Status::error("请先建立数据库连接");
            return;
        }
        if !self.allow_pending_navigation("刷新对象树") {
            return;
        }
        self.objects.clear();
        self.loaded_object_keys.clear();
        self.loading_object_keys.clear();
        self.expanded_object_keys.clear();
        self.object_cursors.clear();
        self.load_objects(None, 0, None, ctx);
        self.status = Status::progress("正在刷新对象树…");
    }

    fn handle_shortcuts(&mut self, ctx: &egui::Context) {
        // While a modal is up, `Esc` belongs to the modal.
        let modal_open = self.tab().show_table_change_preview
            || self.status_detail_open
            || self.file_picker.is_some()
            || self.vault_prompt.is_some()
            || self.palette.is_some();
        let Some(shortcut) = shortcuts::consume(ctx, !modal_open) else {
            return;
        };
        match shortcut {
            Shortcut::Execute => self.execute(ctx),
            Shortcut::Cancel => {
                self.cancel_active_operation();
            }
            Shortcut::RefreshObjects => self.refresh_objects(ctx),
            Shortcut::ApplyTableChanges => self.preview_table_changes(ctx),
            Shortcut::CommandPalette => self.palette = Some(Palette::new()),
            Shortcut::NewTab => self.open_query_tab(),
            Shortcut::CloseTab => self.close_tab(self.active_tab),
            Shortcut::NextTab => {
                if !self.tabs.is_empty() {
                    self.active_tab = (self.active_tab + 1) % self.tabs.len();
                }
            }
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
        self.tab_mut().operation_busy = true;
        self.tab_mut().active_operation_id = Some(operation_id);
        self.tab_mut().active_operation_kind = Some(kind);
        self.tab_mut().active_cancellation = Some(cancellation);
    }

    fn finish_operation(&mut self) {
        self.tab_mut().operation_busy = false;
        self.tab_mut().active_operation_id = None;
        self.tab_mut().active_operation_kind = None;
        self.tab_mut().active_cancellation = None;
    }

    fn cancel_active_operation(&mut self) -> bool {
        let Some(cancellation) = self.tab_mut().active_cancellation.take() else {
            return false;
        };
        cancellation.cancel();
        self.operation_generation = self.operation_generation.wrapping_add(1);
        self.tab_mut().active_operation_id = None;
        self.tab_mut().operation_busy = false;
        let kind = self.tab_mut().active_operation_kind.take();
        self.tab_mut().query_events = None;
        if let Some(active) = self.tab_mut().active_result.take() {
            self.tab_mut().current_result = Some(Arc::new(active));
            self.refresh_data_grid();
        }
        self.status = Status::info(match kind {
            Some(OperationKind::Query) => {
                let rows = self
                    .tab()
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
        });
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

    /// Invalidate everything that belonged to the previous session.
    ///
    /// Every tab is reset, not just the visible one: a table tab without its
    /// session can only mislead, and a stale editor would submit against a
    /// connection that no longer exists. The SQL the user typed is kept.
    fn reset_session_view(&mut self) {
        self.objects.clear();
        self.expanded_object_keys.clear();
        self.loaded_object_keys.clear();
        self.loading_object_keys.clear();
        self.object_cursors.clear();

        for tab in &mut self.tabs {
            if let Some(cancellation) = tab.active_cancellation.take() {
                cancellation.cancel();
            }
        }
        // Table tabs describe objects from the old session; query tabs still
        // hold text worth keeping.
        self.tabs.retain(|tab| tab.table_browse.is_none());
        if self.tabs.is_empty() {
            let id = self.next_tab_id;
            self.next_tab_id = self.next_tab_id.saturating_add(1);
            let choice = DRIVER_CHOICES[self.selected_driver];
            self.tabs
                .push(Tab::new(id, "查询 1", choice.query.to_owned()));
        }
        self.active_tab = self.active_tab.min(self.tabs.len() - 1);
        for tab in &mut self.tabs {
            tab.reset_results();
        }
    }

    fn confirm_write(&mut self, action: WriteAction, text: &str) -> bool {
        if !requires_confirmation(self.choice().language, text) {
            self.tab_mut().pending_write_confirmation = None;
            return true;
        }
        if write_confirmation_matches(self.tab().pending_write_confirmation.as_ref(), action, text) {
            self.tab_mut().pending_write_confirmation = None;
            return true;
        }
        self.tab_mut().pending_write_confirmation = Some(PendingWriteConfirmation {
            action,
            text: text.to_owned(),
        });
        self.status = Status::info(match action {
            WriteAction::Execute => "检测到写操作；再次点击“执行”确认".to_owned(),
            WriteAction::Analyze => "分析执行会真实运行写操作；再次点击“分析执行”确认".to_owned(),
            WriteAction::FullExport => {
                "完整导出会重新执行写操作；再次选择完整导出格式确认".to_owned()
            }
        });
        false
    }

    fn refresh_data_grid(&mut self) {
        let row_count = self
            .tab()
            .active_result
            .as_ref()
            .or(self.tab().current_result.as_deref())
            .map_or(0, |result| result.buffer.row_count());
        let pages = page_count(row_count, self.query_settings.page_size);
        self.tab_mut().current_page = self.tab().current_page.min(pages.saturating_sub(1));
        let offset = self
            .tab()
            .current_page
            .saturating_mul(self.query_settings.page_size);
        let model = self
            .tab()
            .active_result
            .as_ref()
            .or(self.tab().current_result.as_deref())
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
        self.tab_mut().data_grid.replace(model);
    }

    fn query_completion_status(&self) -> String {
        let Some(result) = self.tab().current_result.as_deref() else {
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
        if self.tab().current_page > 0 {
            self.tab_mut().current_page -= 1;
            self.refresh_data_grid();
        }
    }

    fn next_page(&mut self) {
        let rows = self
            .tab()
            .active_result
            .as_ref()
            .or(self.tab().current_result.as_deref())
            .map_or(0, |result| result.buffer.row_count());
        let pages = page_count(rows, self.query_settings.page_size);
        if self.tab().current_page + 1 < pages {
            self.tab_mut().current_page += 1;
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
        self.tab_mut().current_page =
            page_after_page_size_change(self.tab().current_page, previous_page_size, page_size);
        if self
            .tab()
            .table_editor
            .as_ref()
            .is_some_and(|editor| editor.origin() == EditorOrigin::Table)
        {
            let previous_page = self
                .tab()
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
            self.status = Status::info(format!("每页显示已切换为 {page_size} 行"));
        }
        self.capture_preferences();
        self.save_settings();
    }

    fn set_row_limit(&mut self, row_limit: usize) {
        if ROW_LIMIT_PRESETS.contains(&row_limit) {
            self.query_settings.row_limit = row_limit;
            self.status = Status::info(format!("行上限将在下次执行时使用：{row_limit}"));
            self.capture_preferences();
            self.save_settings();
        }
    }

    fn set_memory_limit(&mut self, memory_limit_mib: usize) {
        if MEMORY_LIMIT_PRESETS_MIB.contains(&memory_limit_mib) {
            self.query_settings.memory_limit_mib = memory_limit_mib;
            self.status = Status::info(format!("内存上限将在下次执行时使用：{memory_limit_mib} MiB"));
            self.capture_preferences();
            self.save_settings();
        }
    }





    fn pending_more_rows(&self) -> BTreeMap<usize, (Option<ObjectPath>, usize)> {
        let mut rows = BTreeMap::new();
        for key in self.object_cursors.keys() {
            if key.is_empty() {
                rows.insert(self.objects.len(), (None, 0));
                continue;
            }
            let Some(parent_index) = self
                .objects
                .iter()
                .position(|row| object_path_key(Some(&row.object.path)) == *key)
            else {
                continue;
            };
            // Only offer the row while the branch is actually expanded.
            if !self.expanded_object_keys.contains(key) {
                continue;
            }
            let parent = &self.objects[parent_index];
            let child_depth = parent.depth + 1;
            let mut end = parent_index + 1;
            while self.objects.get(end).is_some_and(|row| row.depth > parent.depth) {
                end += 1;
            }
            rows.insert(end, (Some(parent.object.path.clone()), child_depth));
        }
        rows
    }

    fn rebuild_completer(&mut self) {
        let mut completer =
            Completer::new_with_syntax(&self.choice().editor_syntax.syntax()).with_auto_indent();
        for row in &self.objects {
            completer.push_word(&row.object.name);
        }
        self.completer = completer;
    }

    fn add_structured_filter(&mut self) {
        let Some(controls) = self.tab().table_browse.as_ref() else {
            return;
        };
        let column_name = controls.filter_column.clone();
        let operator = controls.filter_operator;
        let first = controls.filter_value.clone();
        let second = controls.second_filter_value.clone();
        let Some(column) = self
            .tab()
            .table_editor
            .as_ref()
            .and_then(|editor| editor.metadata().column(&column_name))
        else {
            self.status = Status::error("请先选择筛选列");
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
                self.status = Status::error(format!("筛选值无效：{error}"));
                return;
            }
        };
        if let Some(controls) = self.tab_mut().table_browse.as_mut() {
            controls.filters.push(TableFilter {
                column: column_name,
                operator,
                values,
            });
            controls.filter_value.clear();
            controls.second_filter_value.clear();
        }
        self.status = Status::info("筛选条件已应用");
    }

    fn select_tab(&mut self, index: usize) {
        if index < self.tabs.len() {
            self.active_tab = index;
        }
    }

    /// Open an empty query tab seeded with the driver's sample query.
    fn open_query_tab(&mut self) {
        let choice = self.choice();
        let title = format!("查询 {}", self.next_tab_id);
        let id = self.next_tab_id;
        self.next_tab_id = self.next_tab_id.saturating_add(1);
        self.tabs.push(Tab::new(id, title, choice.query.to_owned()));
        self.active_tab = self.tabs.len() - 1;
    }

    /// Close a tab, cancelling whatever it was running.
    ///
    /// Refuses while the tab holds unsaved table edits so a stray click cannot
    /// discard them, and never leaves the workspace without a tab.
    fn close_tab(&mut self, index: usize) {
        if self.tabs.len() <= 1 || index >= self.tabs.len() {
            return;
        }
        let previous = self.active_tab;
        self.active_tab = index;
        if !self.allow_pending_navigation("关闭标签页") {
            self.active_tab = previous;
            return;
        }
        self.cancel_active_operation();
        self.active_tab = previous;
        self.tabs.remove(index);
        // Keep the same tab on screen unless it was the one that closed.
        self.active_tab = if previous > index {
            previous - 1
        } else {
            previous.min(self.tabs.len() - 1)
        };
    }

    /// Explain why a toolbar action is unavailable, at the button itself.
    ///
    /// This replaces a permanent capability panel: the question is only ever
    /// asked about a greyed-out button, so the answer belongs there rather than
    /// on 190-360px of screen that showed the same table all day.
    fn disabled_reason(&self, connected: bool, supported: bool, unsupported: &str) -> String {
        if !connected {
            return "请先建立数据库连接".to_owned();
        }
        if !supported {
            return format!("{unsupported}（{}）", self.choice().name);
        }
        if self.tab().operation_busy {
            return "这个标签页正在执行；可以新建标签页并行工作".to_owned();
        }
        String::new()
    }
}

impl eframe::App for DbcApp {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_query_events();
        self.drain_app_events(ctx);
        self.drain_query_events();
        self.handle_shortcuts(ctx);
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        egui::Panel::top("dbc-top-bar")
            .exact_size(48.0)
            .show(ui, |ui| {
                self.render_top_bar(ui);
            });
        let mut left_panel_open = self.left_panel_open;
        egui::Panel::left("dbc-left-panel")
            .resizable(true)
            .default_size(286.0)
            .size_range(220.0..=420.0)
            .show_collapsible(ui, &mut left_panel_open, |ui| {
                self.render_sidebar(ui);
            });
        self.left_panel_open = left_panel_open;
        egui::CentralPanel::no_frame().show(ui, |ui| {
            self.render_workspace(ui);
        });
        self.render_table_change_preview(ui.ctx());
        self.render_status_detail(ui.ctx());
        self.render_palette(ui.ctx());
        self.render_vault_prompt(ui.ctx());
        self.render_file_picker(ui.ctx());
    }
}

fn optional_input(value: &str) -> Option<String> {
    (!value.trim().is_empty()).then(|| value.trim().to_owned())
}

fn object_path_key(path: Option<&ObjectPath>) -> String {
    path.map(|path| path.segments.join("\u{1f}"))
        .unwrap_or_default()
}

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


#[cfg(test)]
mod tests;
mod commands;
mod connections;
mod events;
mod views;

use events::{ActiveQueryEvents, AppEvent};
