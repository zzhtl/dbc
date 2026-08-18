//! Widget layout for the workspace.
//!
//! Split from the state and scheduling code so a layout change never has to
//! be read alongside the generation checks that keep results in the right
//! tab.

use super::*;

impl DbcApp {
    pub(super) fn render_vault_prompt(&mut self, ctx: &egui::Context) {
        let Some(prompt) = self.vault_prompt.as_mut() else {
            return;
        };
        match prompt.show(ctx) {
            PromptOutcome::Pending => {}
            PromptOutcome::Cancelled => self.vault_prompt = None,
            PromptOutcome::Submitted(master) => self.submit_vault_password(&master),
        }
    }


    /// Full status text, selectable and copyable — the status bar only has
    /// room for the first line.
    pub(super) fn render_status_detail(&mut self, ctx: &egui::Context) {
        if !self.status_detail_open {
            return;
        }
        let mut close = false;
        let modal = egui::Modal::new(egui::Id::new("dbc-status-detail")).show(ctx, |ui| {
            ui.set_width(680.0);
            ui.heading("详细信息");
            ui.add_space(6.0);
            egui::Frame::group(ui.style()).show(ui, |ui| {
                ui.set_height(260.0);
                egui::ScrollArea::both()
                    .id_salt("dbc-status-detail-scroll")
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        ui.add(
                            egui::Label::new(
                                egui::RichText::new(self.status.full_text()).monospace(),
                            )
                            .selectable(true)
                            .wrap(),
                        );
                    });
            });
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                if ui.button("复制全部").clicked() {
                    ui.ctx().copy_text(self.status.full_text().to_owned());
                }
                if ui.button("关闭").clicked() {
                    close = true;
                }
            });
        });
        if close || modal.should_close() {
            self.status_detail_open = false;
        }
    }

    pub(super) fn render_palette(&mut self, ctx: &egui::Context) {
        let Some(mut palette) = self.palette.take() else {
            return;
        };
        let mut commands = self.palette_commands();
        let entries = commands
            .iter()
            .map(|(entry, _)| PaletteEntry::new(entry.label.clone(), entry.hint.clone()))
            .collect::<Vec<_>>();
        match palette.show(ctx, &entries) {
            PaletteOutcome::Pending => self.palette = Some(palette),
            PaletteOutcome::Cancelled => {}
            PaletteOutcome::Chosen(index) => {
                if index < commands.len() {
                    let (_, command) = commands.swap_remove(index);
                    self.run_palette_command(command, ctx);
                }
            }
        }
    }


    pub(super) fn render_file_picker(&mut self, ctx: &egui::Context) {
        let Some(picker) = self.file_picker.as_mut() else {
            return;
        };
        match picker.show(ctx) {
            PickerOutcome::Pending => {}
            PickerOutcome::Cancelled => {
                self.file_picker = None;
                self.pending_export = None;
                self.status = Status::info("已取消导出");
            }
            PickerOutcome::Selected(path) => {
                self.export_directory = picker.directory().to_path_buf();
                self.file_picker = None;
                self.capture_preferences();
                self.save_settings();
                if let Some((scope, format)) = self.pending_export.take() {
                    self.start_export(scope, format, path, ctx);
                }
            }
        }
    }

    pub(super) fn render_top_bar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal_centered(|ui| {
            ui.add_space(8.0);
            ui.heading("DBC");
            ui.separator();
            ui.toggle_value(&mut self.left_panel_open, "连接与对象");
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


    pub(super) fn render_sidebar(&mut self, ui: &mut egui::Ui) {
        ui.heading("数据库连接");
        ui.add_space(4.0);

        let mut selected_driver = self.selected_driver;
        ui.add_enabled_ui(!self.connection_busy && !self.tab().operation_busy, |ui| {
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
                    !self.connection_busy && !self.tab().operation_busy,
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
            if ui
                .add_enabled(
                    !self.testing_connection && !self.connection_busy,
                    egui::Button::new(if self.testing_connection {
                        "测试中…"
                    } else {
                        "测试"
                    }),
                )
                .on_hover_text("只建立一次连接验证参数，不影响当前会话")
                .clicked()
            {
                self.test_connection(ui.ctx());
            }
        });

        self.render_saved_connections(ui);

        ui.separator();
        let mut refresh = false;
        ui.horizontal(|ui| {
            ui.strong("数据库对象");
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.weak(if self.session.is_some() {
                    "在线"
                } else {
                    "离线"
                });
                if ui
                    .add_enabled(
                        self.session.is_some() && !self.tab().operation_busy,
                        egui::Button::new("⟳").frame(false),
                    )
                    .on_hover_text(format!(
                        "刷新对象树 ({})",
                        shortcuts::hint(Shortcut::RefreshObjects)
                    ))
                    .clicked()
                {
                    refresh = true;
                }
            });
        });
        if refresh {
            self.refresh_objects(ui.ctx());
        }
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
                let mut load_more = None;
                let pending_more = self.pending_more_rows();
                for index in self.visible_object_indices() {
                    if let Some((parent, depth)) = pending_more.get(&index)
                        && Self::show_load_more(ui, *depth)
                    {
                        load_more = Some((parent.clone(), *depth));
                    }
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
                if let Some((parent, depth)) = pending_more.get(&self.objects.len())
                    && Self::show_load_more(ui, *depth)
                {
                    load_more = Some((parent.clone(), *depth));
                }
                if let Some((parent, depth)) = load_more {
                    let cursor = self.object_cursors.get(&object_path_key(parent.as_ref())).cloned();
                    self.load_objects(parent, depth, cursor, ui.ctx());
                } else if let Some(table) = opened_table {
                    self.open_table(table, ui.ctx());
                } else if let Some(index) = activated {
                    self.activate_object(index, ui.ctx());
                }
            });
    }

    /// A branch that hit the page limit gets an explicit continuation row;
    /// silently dropping the remainder used to make objects unreachable.
    fn show_load_more(ui: &mut egui::Ui, depth: usize) -> bool {
        let mut clicked = false;
        ui.horizontal(|ui| {
            ui.add_space(depth as f32 * 16.0);
            clicked = ui
                .add(
                    egui::Button::new(
                        egui::RichText::new(format!("… 加载更多（每页 {OBJECT_PAGE_SIZE} 条）"))
                            .weak(),
                    )
                    .frame(false),
                )
                .clicked();
        });
        clicked
    }

    /// Saved connections, the vault state, and the save controls.
    ///
    /// Before this the sidebar was four bare text fields that had to be retyped
    /// on every start.
    pub(super) fn render_saved_connections(&mut self, ui: &mut egui::Ui) {
        ui.separator();
        let mut apply = None;
        let mut delete = None;
        let mut save = false;
        let mut unlock = false;

        ui.horizontal(|ui| {
            ui.strong(format!(
                "已保存连接 ({})",
                self.store.settings().connections.len()
            ));
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                let (label, hint) = match (self.vault.is_some(), self.store.vault_exists()) {
                    (true, _) => ("🔓", "凭据库已解锁"),
                    (false, true) => ("🔒", "凭据库已锁定，点击解锁"),
                    (false, false) => ("🔑", "尚未创建凭据库，点击设置主密码"),
                };
                if ui
                    .add(egui::Button::new(label).frame(false))
                    .on_hover_text(hint)
                    .clicked()
                {
                    unlock = true;
                }
            });
        });

        if self.store.settings().connections.is_empty() {
            ui.weak("填好参数后可保存，下次直接选用");
        } else {
            egui::ScrollArea::vertical()
                .id_salt("saved-connections-scroll")
                .max_height(140.0)
                .auto_shrink([false, true])
                .show(ui, |ui| {
                    for (index, saved) in
                        self.store.settings().connections.iter().enumerate()
                    {
                        ui.horizontal(|ui| {
                            let selected = self.selected_saved == Some(index);
                            let label = format!(
                                "{} · {}",
                                saved.name,
                                driver_display_name(&saved.driver_id)
                            );
                            if ui.selectable_label(selected, label).clicked() {
                                apply = Some(index);
                            }
                            ui.with_layout(
                                egui::Layout::right_to_left(egui::Align::Center),
                                |ui| {
                                    if ui
                                        .add(egui::Button::new("✕").frame(false))
                                        .on_hover_text("删除这条连接")
                                        .clicked()
                                    {
                                        delete = Some(index);
                                    }
                                },
                            );
                        });
                    }
                });
        }

        ui.horizontal(|ui| {
            ui.add(
                egui::TextEdit::singleline(&mut self.connection_name)
                    .hint_text("连接名称")
                    .desired_width(ui.available_width() - 60.0),
            );
            if ui.button("保存").clicked() {
                save = true;
            }
        });
        ui.add_enabled_ui(self.vault.is_some(), |ui| {
            ui.checkbox(&mut self.save_password, "保存密码到本地加密凭据库")
                .on_disabled_hover_text("先解锁凭据库才能保存密码");
        });

        if unlock {
            self.open_vault_prompt();
        }
        if let Some(index) = apply {
            self.apply_saved_connection(index);
        }
        if let Some(index) = delete {
            self.delete_saved_connection(index);
        }
        if save {
            self.save_current_connection();
        }
    }


    pub(super) fn render_query_toolbar(&mut self, ui: &mut egui::Ui) {
        let capabilities = self.capability_support();
        let mut replay: Option<String> = None;
        let connected = self.session.is_some();
        let mut page_size = self.query_settings.page_size;
        let mut row_limit = self.query_settings.row_limit;
        let mut memory_limit = self.query_settings.memory_limit_mib;

        egui::ScrollArea::horizontal()
            .id_salt("query-toolbar-scroll")
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    if ui
                        .add_enabled(connected && !self.tab().operation_busy, egui::Button::new("执行"))
                        .on_hover_text(shortcuts::hint(Shortcut::Execute))
                        .on_disabled_hover_text(self.disabled_reason(connected, true, ""))
                        .clicked()
                    {
                        self.execute(ui.ctx());
                    }
                    ui.menu_button("历史", |ui| {
                        let driver_id = self.choice().id;
                        let recent = self
                            .store
                            .settings()
                            .history
                            .iter()
                            .filter(|entry| entry.driver_id == driver_id)
                            .take(20)
                            .map(|entry| entry.text.clone())
                            .collect::<Vec<_>>();
                        if recent.is_empty() {
                            ui.weak("这个驱动还没有执行记录");
                            return;
                        }
                        egui::ScrollArea::vertical()
                            .id_salt("query-history-scroll")
                            .max_height(300.0)
                            .show(ui, |ui| {
                                for text in recent {
                                    let label = history_label(&text);
                                    if ui.button(label).on_hover_text(&text).clicked() {
                                        replay = Some(text);
                                        ui.close();
                                    }
                                }
                            });
                    });
                    if ui
                        .add_enabled(
                            connected && !self.tab().operation_busy && capabilities.explain,
                            egui::Button::new("执行计划"),
                        )
                        .on_disabled_hover_text(self.disabled_reason(
                            connected,
                            capabilities.explain,
                            "该驱动不提供执行计划",
                        ))
                        .clicked()
                    {
                        self.start_explain(ExplainMode::Estimated, ui.ctx());
                    }
                    if ui
                        .add_enabled(
                            connected && !self.tab().operation_busy && capabilities.analyze,
                            egui::Button::new("分析执行"),
                        )
                        .on_disabled_hover_text(self.disabled_reason(
                            connected,
                            capabilities.analyze,
                            "该驱动不提供 Analyze 执行计划",
                        ))
                        .clicked()
                    {
                        self.start_explain(ExplainMode::Analyze, ui.ctx());
                    }
                    if ui
                        .add_enabled(
                            connected && !self.tab().operation_busy && capabilities.slow_queries,
                            egui::Button::new("慢查询"),
                        )
                        .on_disabled_hover_text(self.disabled_reason(
                            connected,
                            capabilities.slow_queries,
                            "该驱动没有原生慢查询统计",
                        ))
                        .clicked()
                    {
                        self.load_slow_queries(ui.ctx());
                    }
                    if ui
                        .add_enabled(
                            self.tab().active_cancellation.is_some(),
                            egui::Button::new("取消"),
                        )
                        .on_hover_text(shortcuts::hint(Shortcut::Cancel))
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
        if let Some(text) = replay {
            self.tab_mut().query_text = text;
            self.status = Status::info("已从历史载入查询");
        }
    }


    pub(super) fn render_query_editor(&mut self, ui: &mut egui::Ui) {
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
        // The completer needs the editor and the dictionary at the same time,
        // so take it out for the call and put it straight back.
        let mut completer = std::mem::take(&mut self.completer);
        editor.show_with_completer(ui, &mut self.tab_mut().query_text, &syntax, &mut completer);
        self.completer = completer;
    }

    /// Refresh the editor dictionary from the loaded objects.
    ///
    /// Table, column and index names come from whatever the object tree has
    /// already fetched, so completion never triggers extra queries.
    pub(super) fn render_table_browser_controls(&mut self, ui: &mut egui::Ui) {
        let columns = self
            .tab()
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
        // Read before the mutable borrow below; the tab is borrowed for the
        // whole widget body.
        let operation_busy = self.tab().operation_busy;
        let Some(controls) = self.tab_mut().table_browse.as_mut() else {
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
            // Enter applies the filter directly: picking a column, an operator
            // and a value used to need two more clicks before anything happened.
            if filter_operator_value_count(controls.filter_operator) > 0 {
                let response = ui.add(
                    egui::TextEdit::singleline(&mut controls.filter_value)
                        .hint_text("值，回车即生效")
                        .desired_width(120.0),
                );
                if response.lost_focus() && ui.input(|input| input.key_pressed(egui::Key::Enter)) {
                    add_filter = true;
                }
            }
            if filter_operator_value_count(controls.filter_operator) == 2 {
                let response = ui.add(
                    egui::TextEdit::singleline(&mut controls.second_filter_value)
                        .hint_text("第二个值")
                        .desired_width(120.0),
                );
                if response.lost_focus() && ui.input(|input| input.key_pressed(egui::Key::Enter)) {
                    add_filter = true;
                }
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
            if ui
                .selectable_value(
                    &mut controls.sort_direction,
                    SortDirection::Ascending,
                    "升序",
                )
                .changed()
            {
                reload = true;
            }
            if ui
                .selectable_value(
                    &mut controls.sort_direction,
                    SortDirection::Descending,
                    "降序",
                )
                .changed()
            {
                reload = true;
            }
        });
        ui.horizontal(|ui| {
            ui.strong("原始片段");
            let response = ui.add(
                egui::TextEdit::singleline(&mut controls.raw_where)
                    .hint_text("WHERE（不含 WHERE），回车即生效")
                    .desired_width(240.0),
            );
            if response.lost_focus() && ui.input(|input| input.key_pressed(egui::Key::Enter)) {
                reload = true;
            }
            ui.add(
                egui::TextEdit::singleline(&mut controls.raw_order_by)
                    .hint_text("ORDER BY（不含 ORDER BY）")
                    .desired_width(240.0),
            );
            if ui
                .add_enabled(!operation_busy, egui::Button::new("重新加载"))
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
            reload = self.status.kind_is_not_error();
        }
        if let Some(index) = remove_filter
            && let Some(controls) = self.tab_mut().table_browse.as_mut()
            && index < controls.filters.len()
        {
            controls.filters.remove(index);
            reload = true;
        }
        if reload {
            self.load_active_table_page(0, ui.ctx());
        }
    }


    pub(super) fn render_results(&mut self, ui: &mut egui::Ui) {
        egui::Panel::top("result-tabs")
            .exact_size(34.0)
            .show(ui, |ui| {
                ui.horizontal_centered(|ui| {
                    ui.selectable_value(&mut self.tab_mut().result_tab, ResultTab::Data, "数据");
                    ui.selectable_value(&mut self.tab_mut().result_tab, ResultTab::Plan, "执行计划");
                    ui.selectable_value(&mut self.tab_mut().result_tab, ResultTab::SlowQueries, "慢查询");
                });
            });

        let show_table_browser = self
            .tab()
            .table_editor
            .as_ref()
            .is_some_and(|editor| editor.origin() == EditorOrigin::Table);
        if self.tab().result_tab == ResultTab::Data && show_table_browser {
            egui::Panel::top("table-browser-controls")
                .exact_size(110.0)
                .show(ui, |ui| {
                    self.render_table_browser_controls(ui);
                });
        }

        if self.tab().result_tab == ResultTab::Data {
            egui::Panel::bottom("result-pagination")
                .exact_size(36.0)
                .show(ui, |ui| {
                    self.render_result_controls(ui);
                });
        }

        egui::CentralPanel::no_frame().show(ui, |ui| match self.tab().result_tab {
            ResultTab::Data => {
                let current_page = self.tab().current_page;
                let page_size = self.query_settings.page_size;
                if let Some(editor) = self.tab_mut().table_editor.as_mut() {
                    if editor.show(ui, current_page, page_size) {
                        self.invalidate_table_change_plan();
                    }
                } else {
                    self.tab_mut().data_grid.show(ui, "data-grid");
                }
            }
            ResultTab::SlowQueries => self.tab_mut().slow_grid.show(ui, "slow-query-grid"),
            ResultTab::Plan => {
                egui::ScrollArea::both()
                    .id_salt("execution-plan-scroll")
                    .show(ui, |ui| {
                        ui.add(
                            egui::Label::new(egui::RichText::new(&self.tab().plan_text).monospace())
                                .selectable(true),
                        );
                    });
            }
        });
    }

    /// Export entry points, shared by the read-only and the editable result
    /// bars. They used to exist only on the read-only bar, so running a
    /// `SELECT *` that happened to be editable made both buttons disappear.
    pub(super) fn render_export_menu(
        &self,
        ui: &mut egui::Ui,
    ) -> Option<(ExportScope, ExportFormat)> {
        let can_export_current = self
            .tab()
            .current_result
            .as_deref()
            .and_then(|result| result.schema.as_ref())
            .is_some()
            && !self.tab().operation_busy;
        let can_export_full =
            self.session.is_some() && self.tab().last_query.is_some() && !self.tab().operation_busy;
        let mut export = None;
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
                if ui.button("Markdown").clicked() {
                    export = Some((ExportScope::Full, ExportFormat::Markdown));
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
                if ui.button("Markdown").clicked() {
                    export = Some((ExportScope::Current, ExportFormat::Markdown));
                    ui.close();
                }
            });
        });
        export
    }


    pub(super) fn render_result_controls(&mut self, ui: &mut egui::Ui) {
        if self.tab().table_editor.is_some() {
            self.render_editable_result_controls(ui);
            return;
        }
        let result_rows = self
            .tab()
            .active_result
            .as_ref()
            .or(self.tab().current_result.as_deref())
            .map_or(0, |result| result.buffer.row_count());
        let result_pages = page_count(result_rows, self.query_settings.page_size);
        let mut export = None;

        ui.horizontal_centered(|ui| {
            if ui
                .add_enabled(self.tab().current_page > 0, egui::Button::new("上一页"))
                .clicked()
            {
                self.previous_page();
            }
            if ui
                .add_enabled(
                    self.tab().current_page + 1 < result_pages,
                    egui::Button::new("下一页"),
                )
                .clicked()
            {
                self.next_page();
            }
            ui.weak(format!(
                "第 {} / {} 页 · {} 行",
                self.tab().current_page + 1,
                result_pages,
                result_rows
            ));
            if let Some(reason) = &self.tab().query_read_only_reason {
                ui.separator();
                ui.weak(reason);
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                export = self.render_export_menu(ui);
            });
        });

        if let Some((scope, format)) = export {
            self.request_export(scope, format, ui.ctx());
        }
    }


    pub(super) fn render_editable_result_controls(&mut self, ui: &mut egui::Ui) {
        let Some(editor) = self.tab().table_editor.as_ref() else {
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
                    u64::try_from(self.tab().current_page).unwrap_or(u64::MAX),
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
        let mut export = None;

        ui.horizontal_centered(|ui| {
            if ui
                .add_enabled(page_index > 0 && !self.tab().operation_busy, egui::Button::new("上一页"))
                .clicked()
            {
                previous = true;
            }
            if ui
                .add_enabled(
                    page_index + 1 < page_count && !self.tab().operation_busy,
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
            if let Some(reason) = &self.tab().query_read_only_reason {
                ui.separator();
                ui.weak(reason);
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui
                    .add_enabled(
                        pending > 0 && !self.tab().operation_busy,
                        egui::Button::new(format!("SQL 预览并应用 ({pending})")),
                    )
                    .on_hover_text(shortcuts::hint(Shortcut::ApplyTableChanges))
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
                        allow_insert && !self.tab().operation_busy,
                        egui::Button::new("新增行"),
                    )
                    .clicked()
                {
                    add = true;
                }
                ui.separator();
                export = self.render_export_menu(ui);
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
                .tab_mut()
                .table_editor
                .as_mut()
                .is_some_and(TableEditorState::add_row)
        {
            self.invalidate_table_change_plan();
        }
        if discard {
            if let Some(editor) = self.tab_mut().table_editor.as_mut() {
                editor.discard_changes();
            }
            self.invalidate_table_change_plan();
            self.status = Status::info("已放弃未提交的表数据变更");
        }
        if preview {
            self.preview_table_changes(ui.ctx());
        }
        if let Some((scope, format)) = export {
            self.request_export(scope, format, ui.ctx());
        }
    }


    pub(super) fn render_table_change_preview(&mut self, ctx: &egui::Context) {
        if !self.tab().show_table_change_preview {
            return;
        }
        let Some(prepared) = self.tab().prepared_table_change.as_ref() else {
            self.tab_mut().show_table_change_preview = false;
            return;
        };
        let statements = prepared.plan.statements.clone();
        let summary = prepared.plan.summary;
        let mut open = self.tab().show_table_change_preview;
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
                        .add_enabled(!self.tab().operation_busy, egui::Button::new("确认应用"))
                        .clicked()
                    {
                        apply = true;
                    }
                    if ui.button("取消").clicked() {
                        self.tab_mut().show_table_change_preview = false;
                    }
                });
            });
        self.tab_mut().show_table_change_preview = open;
        if apply {
            self.apply_prepared_table_changes(ctx);
        }
    }


    pub(super) fn render_workspace(&mut self, ui: &mut egui::Ui) {
        egui::Panel::top("workspace-tabs")
            .exact_size(34.0)
            .show(ui, |ui| {
                self.render_tab_strip(ui);
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
                    if self.status.show(ui) {
                        self.status_detail_open = true;
                    }
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

    /// Tab strip: switch, close, and open a new query tab.
    pub(super) fn render_tab_strip(&mut self, ui: &mut egui::Ui) {
        let mut select = None;
        let mut close = None;
        let mut open_new = false;

        egui::ScrollArea::horizontal()
            .id_salt("workspace-tab-scroll")
            .show(ui, |ui| {
                ui.horizontal_centered(|ui| {
                    ui.add_space(4.0);
                    let closable = self.tabs.len() > 1;
                    for (index, tab) in self.tabs.iter().enumerate() {
                        let running = tab.operation_busy;
                        let label = if running {
                            format!("● {}", tab.title)
                        } else {
                            tab.title.clone()
                        };
                        if ui
                            .selectable_label(index == self.active_tab, label)
                            .on_hover_text(if running {
                                "这个标签页正在执行"
                            } else {
                                "切换到这个标签页"
                            })
                            .clicked()
                        {
                            select = Some(index);
                        }
                        if closable
                            && ui
                                .add(egui::Button::new("×").frame(false))
                                .on_hover_text(format!(
                                    "关闭 ({})",
                                    shortcuts::hint(Shortcut::CloseTab)
                                ))
                                .clicked()
                        {
                            close = Some(index);
                        }
                        ui.separator();
                    }
                    if ui
                        .add(egui::Button::new("＋").frame(false))
                        .on_hover_text(format!("新建查询 ({})", shortcuts::hint(Shortcut::NewTab)))
                        .clicked()
                    {
                        open_new = true;
                    }
                });
            });

        if let Some(index) = select {
            self.select_tab(index);
        }
        if let Some(index) = close {
            self.close_tab(index);
        }
        if open_new {
            self.open_query_tab();
        }
    }

}
