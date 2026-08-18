//! Asynchronous completions and how they reach the right tab.
//!
//! Every payload carries the tab that started it plus the session and
//! operation generation it belonged to, so a result that arrives after the
//! user moved on is dropped instead of overwriting newer state.

use super::*;

pub(super) struct ActiveQueryEvents {
    pub(super) session_generation: u64,
    pub(super) operation_id: u64,
    pub(super) receiver: mpsc::Receiver<QueryEvent>,
}

pub(super) enum AppEvent {
    Connected {
        session_generation: u64,
        result: DriverTaskResult<Arc<dyn DatabaseSession>>,
    },
    /// A throwaway connection used only to validate the form.
    ConnectionTested {
        result: DriverTaskResult<Arc<dyn DatabaseSession>>,
    },
    ObjectsLoaded {
        session_generation: u64,
        key: String,
        parent: Option<ObjectPath>,
        depth: usize,
        /// True when this page continues an already-rendered branch.
        appending: bool,
        result: DriverTaskResult<ObjectPage>,
    },
    QueryFinished {
        /// Tab that started the operation; results never move between tabs.
        tab: TabId,
        session_generation: u64,
        operation_id: u64,
        result: DriverTaskResult<()>,
    },
    QueryMetadataLoaded {
        /// Tab that started the operation; results never move between tabs.
        tab: TabId,
        session_generation: u64,
        operation_id: u64,
        analysis: QuerySourceAnalysis,
        result: DriverTaskResult<TableMetadata>,
    },
    ExplainFinished {
        /// Tab that started the operation; results never move between tabs.
        tab: TabId,
        session_generation: u64,
        operation_id: u64,
        result: DriverTaskResult<ExecutionPlan>,
    },
    SlowQueriesFinished {
        /// Tab that started the operation; results never move between tabs.
        tab: TabId,
        session_generation: u64,
        operation_id: u64,
        result: DriverTaskResult<SlowQueryPage>,
    },
    ExportFinished {
        /// Tab that started the operation; results never move between tabs.
        tab: TabId,
        session_generation: u64,
        operation_id: u64,
        path: PathBuf,
        result: ExportTaskResult,
    },
    TableLoaded {
        /// Tab that started the operation; results never move between tabs.
        tab: TabId,
        session_generation: u64,
        operation_id: u64,
        result: DriverTaskResult<LoadedTable>,
    },
    TablePlanReady {
        /// Tab that started the operation; results never move between tabs.
        tab: TabId,
        session_generation: u64,
        operation_id: u64,
        editor_generation: u64,
        revision: u64,
        result: DriverTaskResult<TableChangePlan>,
    },
    TableApplied {
        /// Tab that started the operation; results never move between tabs.
        tab: TabId,
        session_generation: u64,
        operation_id: u64,
        editor_generation: u64,
        result: DriverTaskResult<TableApplyResult>,
    },
}

pub(super) fn event_is_current(
    current_session_generation: u64,
    active_operation_id: Option<u64>,
    event_session_generation: u64,
    event_operation_id: u64,
) -> bool {
    current_session_generation == event_session_generation
        && active_operation_id == Some(event_operation_id)
}

impl DbcApp {
    /// Drain streamed rows for every tab, not only the visible one, so a query
    /// keeps filling its own tab while the user works in another.
    pub(super) fn drain_query_events(&mut self) {
        let ids = self.tabs.iter().map(|tab| tab.id).collect::<Vec<_>>();
        for id in ids {
            self.with_tab(id, Self::drain_tab_query_events);
        }
    }

    fn drain_tab_query_events(&mut self) {
        let mut events = Vec::new();
        let mut discard_receiver = false;
        let session_generation = self.session_generation;
        let active_operation_id = self.tab().active_operation_id;
        if let Some(active) = self.tab_mut().query_events.as_mut() {
            if !event_is_current(
                session_generation,
                active_operation_id,
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
            self.tab_mut().query_events = None;
            return;
        }
        if events.is_empty() {
            return;
        }
        if let Some(result) = self.tab_mut().active_result.as_mut() {
            for event in events {
                result.apply(event);
            }
            self.refresh_data_grid();
        }
    }

    pub(super) fn drain_app_events(&mut self, ctx: &egui::Context) {
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
                        self.status = Status::success(format!("{} 已连接", self.choice().name));
                        self.load_objects(None, 0, None, ctx);
                    }
                    Err(error) => {
                        self.session = None;
                        self.status = Status::error(format!("连接失败：{error}"));
                    }
                }
            }
            AppEvent::ConnectionTested { result } => {
                self.testing_connection = false;
                self.status = match result {
                    Ok(session) => {
                        // The probe connection is never used for queries.
                        self.close_session_detached(session);
                        Status::success("连接测试成功")
                    }
                    Err(TaskError::Cancelled) => Status::info("连接测试已取消"),
                    Err(TaskError::Operation(error)) => {
                        Status::error(format!("连接测试失败：{error}"))
                    }
                    Err(TaskError::Join(error)) => {
                        Status::error(format!("连接测试任务失败：{error}"))
                    }
                };
            }
            AppEvent::ObjectsLoaded {
                session_generation,
                key,
                parent,
                depth,
                appending,
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
                        let insert_at = self.branch_insertion_point(parent.as_ref(), appending);
                        match insert_at {
                            Some(index) => {
                                self.objects.splice(index..index, rows);
                            }
                            None if parent.is_none() && !appending => self.objects = rows,
                            // The parent row disappeared (collapsed refresh); drop the page.
                            None => {}
                        }
                        match page.next_cursor {
                            Some(cursor) => {
                                self.object_cursors.insert(key.clone(), cursor);
                            }
                            None => {
                                self.object_cursors.remove(&key);
                            }
                        }
                        self.loaded_object_keys.insert(key);
                        self.rebuild_completer();
                    }
                    Err(error) => {
                        self.status = Status::error(format!("对象树加载失败：{error}"));
                    }
                }
            }
            AppEvent::QueryFinished {
                tab,
                session_generation,
                operation_id,
                result,
            } => {
                self.with_tab(tab, |app| {
                    if !event_is_current(
                        app.session_generation,
                        app.tab().active_operation_id,
                        session_generation,
                        operation_id,
                    ) {
                        return;
                    }
                    app.tab_mut().query_events = None;
                    app.finish_operation();
                    let buffered_rows = app
                        .tab()
                        .active_result
                        .as_ref()
                        .map_or(0, |query| query.buffer.row_count());
                    if let Some(active) = app.tab_mut().active_result.take() {
                        app.tab_mut().current_result = Some(Arc::new(active));
                    }
                    match result {
                        Ok(()) => {
                            app.status = Status::success(app.query_completion_status());
                            app.refresh_data_grid();
                            app.prepare_query_editor(operation_id, ctx);
                        }
                        Err(error) if buffered_rows == 0 => {
                            app.tab_mut().data_grid
                                .replace(ResultModel::message(format!("执行失败：{error}")));
                            app.status = Status::error(format!("执行失败：{error}"));
                        }
                        Err(TaskError::Cancelled) => {
                            app.status = Status::info(format!("查询已取消 · 已保留 {buffered_rows} 行"));
                            app.refresh_data_grid();
                        }
                        Err(error) => {
                            app.status = Status::error(format!("执行失败：{error} · 已保留 {buffered_rows} 行"));
                            app.refresh_data_grid();
                        }
                    }
                });
            }
            AppEvent::QueryMetadataLoaded {
                tab,
                session_generation,
                operation_id,
                analysis,
                result,
            } => {
                self.with_tab(tab, |app| {
                    if session_generation != app.session_generation
                        || app
                            .tab()
                            .last_query
                            .as_ref()
                            .is_none_or(|snapshot| snapshot.operation_id != operation_id)
                    {
                        return;
                    }
                    let metadata = match result {
                        Ok(metadata) => metadata,
                        Err(error) => {
                            app.tab_mut().query_read_only_reason =
                                Some(format!("查询结果只读：无法读取行标识元数据（{error}）"));
                            return;
                        }
                    };
                    let Some(result) = app.tab().current_result.as_deref() else {
                        return;
                    };
                    let Some((columns, rows)) = tabular_cells(
                        result.schema.as_ref(),
                        result.buffer.batches(),
                    ) else {
                        app.tab_mut().query_read_only_reason =
                            Some("查询结果不是可编辑的关系型表格".to_owned());
                        return;
                    };
                    let editability =
                        resolve_query_editability(&analysis, &metadata, &columns);
                    if !editability.editable {
                        app.tab_mut().query_read_only_reason = Some(
                            editability
                                .reason
                                .map(query_editability_reason_label)
                                .unwrap_or("查询结果没有可编辑的直接列")
                                .to_owned(),
                        );
                        return;
                    }
                    let page_size =
                        u32::try_from(app.query_settings.page_size).unwrap_or(u32::MAX);
                    app.tab_mut().table_editor = Some(TableEditorState::from_query(
                        metadata,
                        editability,
                        columns,
                        rows,
                        page_size,
                    ));
                    app.tab_mut().table_editor_generation =
                        app.tab().table_editor_generation.wrapping_add(1);
                    app.tab_mut().query_read_only_reason = None;
                    app.invalidate_table_change_plan();
                });
            }
            AppEvent::ExplainFinished {
                tab,
                session_generation,
                operation_id,
                result,
            } => {
                self.with_tab(tab, |app| {
                    if !event_is_current(
                        app.session_generation,
                        app.tab().active_operation_id,
                        session_generation,
                        operation_id,
                    ) {
                        return;
                    }
                    app.finish_operation();
                    match result {
                        Ok(plan) => {
                            app.tab_mut().plan_text = serde_json::to_string_pretty(&plan.document)
                                .unwrap_or_else(|_| plan.document.to_string());
                            app.status = Status::success(format!(
                                "{} 执行计划已生成{}",
                                plan.engine,
                                if plan.analyzed {
                                    "（含实际执行）"
                                } else {
                                    ""
                                }
                            ));
                        }
                        Err(error) => {
                            app.tab_mut().plan_text = format!("无法生成执行计划\n\n{error}");
                            app.status = Status::error(format!("执行计划失败：{error}"));
                        }
                    }
                });
            }
            AppEvent::SlowQueriesFinished {
                tab,
                session_generation,
                operation_id,
                result,
            } => {
                self.with_tab(tab, |app| {
                    if !event_is_current(
                        app.session_generation,
                        app.tab().active_operation_id,
                        session_generation,
                        operation_id,
                    ) {
                        return;
                    }
                    app.finish_operation();
                    match result {
                        Ok(page) => {
                            let source = page.source.clone();
                            let count = page.entries.len();
                            app.tab_mut().slow_grid.replace(ResultModel::from_slow_queries(page));
                            app.status = Status::success(format!("已从 {source} 读取 {count} 条慢查询统计"));
                        }
                        Err(error) => {
                            app.tab_mut().slow_grid
                                .replace(ResultModel::message(format!("慢查询不可用：{error}")));
                            app.status = Status::error(format!("慢查询失败：{error}"));
                        }
                    }
                });
            }
            AppEvent::ExportFinished {
                tab,
                session_generation,
                operation_id,
                path,
                result,
            } => {
                self.with_tab(tab, |app| {
                    if !event_is_current(
                        app.session_generation,
                        app.tab().active_operation_id,
                        session_generation,
                        operation_id,
                    ) {
                        return;
                    }
                    app.finish_operation();
                    app.status = match result {
                        Ok(summary) => Status::success(format!(
                            "导出完成 · {} 行 · {} 字节 · {}",
                            summary.rows,
                            summary.bytes,
                            path.display()
                        )),
                        Err(TaskError::Cancelled)
                        | Err(TaskError::Operation(ExportError::Cancelled)) => {
                            Status::info("导出已取消，目标文件未修改")
                        }
                        Err(TaskError::Operation(error)) => {
                            Status::error(format!("导出失败：{error}"))
                        }
                        Err(TaskError::Join(error)) => {
                            Status::error(format!("导出任务失败：{error}"))
                        }
                    };
                });
            }
            AppEvent::TableLoaded {
                tab,
                session_generation,
                operation_id,
                result,
            } => {
                self.with_tab(tab, |app| {
                    if !event_is_current(
                        app.session_generation,
                        app.tab().active_operation_id,
                        session_generation,
                        operation_id,
                    ) {
                        return;
                    }
                    app.finish_operation();
                    match result {
                        Ok(loaded) => {
                            if let Some(controls) = app.tab_mut().table_browse.as_mut()
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
                            app.tab_mut().table_editor = Some(TableEditorState::from_table(
                                loaded.metadata,
                                loaded.page,
                            ));
                            app.tab_mut().table_editor_generation =
                                app.tab().table_editor_generation.wrapping_add(1);
                            app.invalidate_table_change_plan();
                            app.tab_mut().query_read_only_reason = None;
                            app.status = Status::success(format!(
                                "已读取 {name} · 第 {} 页 · 精确统计 {total} 行",
                                page + 1
                            ));
                        }
                        Err(error) => {
                            app.status = Status::error(format!("表数据加载失败：{error}"));
                        }
                    }
                });
            }
            AppEvent::TablePlanReady {
                tab,
                session_generation,
                operation_id,
                editor_generation,
                revision,
                result,
            } => {
                self.with_tab(tab, |app| {
                    if !event_is_current(
                        app.session_generation,
                        app.tab().active_operation_id,
                        session_generation,
                        operation_id,
                    ) {
                        return;
                    }
                    app.finish_operation();
                    if editor_generation != app.tab().table_editor_generation
                        || app
                            .tab()
                            .table_editor
                            .as_ref()
                            .is_none_or(|editor| editor.revision() != revision)
                    {
                        app.status = Status::info("数据已继续修改，请重新生成 SQL 预览");
                        return;
                    }
                    match result {
                        Ok(plan) => {
                            let count = plan.statements.len();
                            app.tab_mut().prepared_table_change = Some(PreparedTableChange {
                                editor_generation,
                                revision,
                                plan,
                            });
                            app.tab_mut().show_table_change_preview = true;
                            app.status = Status::success(format!("已生成 {count} 条参数化变更语句"));
                        }
                        Err(error) => {
                            app.status = Status::error(format!("SQL 预览生成失败：{error}"));
                        }
                    }
                });
            }
            AppEvent::TableApplied {
                tab,
                session_generation,
                operation_id,
                editor_generation,
                result,
            } => {
                self.with_tab(tab, |app| {
                    if !event_is_current(
                        app.session_generation,
                        app.tab().active_operation_id,
                        session_generation,
                        operation_id,
                    ) || editor_generation != app.tab().table_editor_generation
                    {
                        return;
                    }
                    app.finish_operation();
                    match result {
                        Ok(applied) => {
                            let origin = app
                                .tab()
                                .table_editor
                                .as_ref()
                                .map(TableEditorState::origin);
                            let page = app
                                .tab()
                                .table_editor
                                .as_ref()
                                .map_or(0, TableEditorState::page_index);
                            app.tab_mut().table_editor = None;
                            app.invalidate_table_change_plan();
                            app.status = Status::info(format!(
                                "提交完成 · 新增 {} · 更新 {} · 删除 {}",
                                applied.summary.inserted,
                                applied.summary.updated,
                                applied.summary.deleted
                            ));
                            match origin {
                                Some(EditorOrigin::Table) => {
                                    app.load_active_table_page(page, ctx);
                                }
                                Some(EditorOrigin::Query) => {
                                    if let Some(text) =
                                        app.tab().last_query.as_ref().map(|snapshot| snapshot.text.clone())
                                    {
                                        app.tab_mut().query_text = text;
                                    }
                                    app.execute(ctx);
                                }
                                None => {}
                            }
                        }
                        Err(error) => {
                            app.invalidate_table_change_plan();
                            app.status = Status::error(match &error {
                                TaskError::Operation(DriverError::Conflict(_)) => {
                                    "提交冲突：数据已被其他事务修改；请放弃变更并重新加载"
                                        .to_owned()
                                }
                                _ => format!("表数据提交失败：{error}"),
                            });
                        }
                    }
                });
            }
        }
    }
}
