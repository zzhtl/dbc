//! What the command palette can run.
//!
//! Every command maps to an action the UI already exposes; keeping the list
//! here means adding a feature does not also mean finding room for another
//! toolbar button.

use super::*;

/// Something the command palette can run.
///
/// Every entry maps to an action the UI already exposes; the palette exists so
/// none of them needs a permanent button.
pub(super) enum PaletteCommand {
    Execute,
    Explain(ExplainMode),
    SlowQueries,
    Cancel,
    NewTab,
    CloseTab,
    SwitchTab(usize),
    RefreshObjects,
    Connect,
    Disconnect,
    TestConnection,
    UseSavedConnection(usize),
    OpenTable(TableRef),
    ReplayQuery(String),
    Export(ExportScope, ExportFormat),
    UnlockVault,
}

impl DbcApp {
    pub(super) fn palette_commands(&self) -> Vec<(PaletteEntry, PaletteCommand)> {
        let capabilities = self.capability_support();
        let connected = self.session.is_some();
        let busy = self.tab().operation_busy;
        let mut commands = Vec::new();

        if connected && !busy {
            commands.push((
                PaletteEntry::new("执行查询", shortcuts::hint(Shortcut::Execute)),
                PaletteCommand::Execute,
            ));
            if capabilities.explain {
                commands.push((
                    PaletteEntry::new("生成执行计划", "Estimated"),
                    PaletteCommand::Explain(ExplainMode::Estimated),
                ));
            }
            if capabilities.analyze {
                commands.push((
                    PaletteEntry::new("分析执行", "Analyze，会真实执行查询"),
                    PaletteCommand::Explain(ExplainMode::Analyze),
                ));
            }
            if capabilities.slow_queries {
                commands.push((
                    PaletteEntry::new("读取慢查询统计", "数据库原生统计"),
                    PaletteCommand::SlowQueries,
                ));
            }
        }
        if self.tab().active_cancellation.is_some() {
            commands.push((
                PaletteEntry::new("取消当前操作", shortcuts::hint(Shortcut::Cancel)),
                PaletteCommand::Cancel,
            ));
        }

        commands.push((
            PaletteEntry::new("新建查询标签页", shortcuts::hint(Shortcut::NewTab)),
            PaletteCommand::NewTab,
        ));
        if self.tabs.len() > 1 {
            commands.push((
                PaletteEntry::new("关闭当前标签页", shortcuts::hint(Shortcut::CloseTab)),
                PaletteCommand::CloseTab,
            ));
            for (index, tab) in self.tabs.iter().enumerate() {
                if index != self.active_tab {
                    commands.push((
                        PaletteEntry::new(format!("切换到 {}", tab.title), "标签页"),
                        PaletteCommand::SwitchTab(index),
                    ));
                }
            }
        }

        if connected {
            commands.push((
                PaletteEntry::new("刷新对象树", shortcuts::hint(Shortcut::RefreshObjects)),
                PaletteCommand::RefreshObjects,
            ));
            commands.push((
                PaletteEntry::new("断开连接", self.choice().name),
                PaletteCommand::Disconnect,
            ));
            if capabilities.table_data {
                for row in &self.objects {
                    if let Some(table) = object_table_ref(&row.object) {
                        commands.push((
                            PaletteEntry::new(
                                format!("打开表 {}", row.object.name),
                                object_kind_label(&row.object.kind),
                            ),
                            PaletteCommand::OpenTable(table),
                        ));
                    }
                }
            }
        } else {
            commands.push((
                PaletteEntry::new("连接数据库", self.choice().name),
                PaletteCommand::Connect,
            ));
        }
        commands.push((
            PaletteEntry::new("测试连接", "只验证参数，不影响当前会话"),
            PaletteCommand::TestConnection,
        ));

        for (index, saved) in self.store.settings().connections.iter().enumerate() {
            commands.push((
                PaletteEntry::new(
                    format!("载入连接 {}", saved.name),
                    driver_display_name(&saved.driver_id),
                ),
                PaletteCommand::UseSavedConnection(index),
            ));
        }
        if self.vault.is_none() {
            commands.push((
                PaletteEntry::new("解锁凭据库", "读取已保存的数据库密码"),
                PaletteCommand::UnlockVault,
            ));
        }

        for (scope, label) in [
            (ExportScope::Current, "导出当前缓冲结果"),
            (ExportScope::Full, "重新执行并完整导出"),
        ] {
            for format in [
                ExportFormat::Csv,
                ExportFormat::JsonLines,
                ExportFormat::Markdown,
            ] {
                commands.push((
                    PaletteEntry::new(format!("{label}为 {}", format.label()), "导出"),
                    PaletteCommand::Export(scope, format),
                ));
            }
        }

        let driver_id = self.choice().id;
        for entry in self
            .store
            .settings()
            .history
            .iter()
            .filter(|entry| entry.driver_id == driver_id)
            .take(20)
        {
            commands.push((
                PaletteEntry::new(history_label(&entry.text), "历史查询"),
                PaletteCommand::ReplayQuery(entry.text.clone()),
            ));
        }
        commands
    }

    pub(super) fn run_palette_command(&mut self, command: PaletteCommand, ctx: &egui::Context) {
        match command {
            PaletteCommand::Execute => self.execute(ctx),
            PaletteCommand::Explain(mode) => self.start_explain(mode, ctx),
            PaletteCommand::SlowQueries => self.load_slow_queries(ctx),
            PaletteCommand::Cancel => {
                self.cancel_active_operation();
            }
            PaletteCommand::NewTab => self.open_query_tab(),
            PaletteCommand::CloseTab => self.close_tab(self.active_tab),
            PaletteCommand::SwitchTab(index) => self.select_tab(index),
            PaletteCommand::RefreshObjects => self.refresh_objects(ctx),
            PaletteCommand::Connect => self.connect(ctx),
            PaletteCommand::Disconnect => self.disconnect(),
            PaletteCommand::TestConnection => self.test_connection(ctx),
            PaletteCommand::UseSavedConnection(index) => self.apply_saved_connection(index),
            PaletteCommand::OpenTable(table) => self.open_table(table, ctx),
            PaletteCommand::ReplayQuery(text) => {
                self.tab_mut().query_text = text;
                self.status = Status::info("已从历史载入查询");
            }
            PaletteCommand::Export(scope, format) => self.request_export(scope, format, ctx),
            PaletteCommand::UnlockVault => self.open_vault_prompt(),
        }
    }
}
