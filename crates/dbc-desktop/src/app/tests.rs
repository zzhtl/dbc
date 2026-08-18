//! Desktop behaviour tests.
//!
//! Kept in their own file so the application module reads as production
//! code; they still live inside `app` and can reach its private state.

use std::{thread, time::Duration};

use super::events::{ActiveQueryEvents, event_is_current};
use super::{
    BufferedQueryResult, DbcApp, ExportFormat, ExportScope, ObjectRow, OperationKind,
    QuerySettings, object_path_key,
};
use crate::write_guard::WriteAction;
use dbc_core::{
    diagnostics::ExplainMode,
    driver::QueryEvent,
    metadata::{DatabaseObject, DatabaseObjectKind, ObjectPath},
    table_data::TableRef,
};
use dbc_core::result::{CellValue, DataBatch, DataSchema};
use eframe::egui::accesskit::Role;
use egui_kittest::{
    Harness,
    kittest::{NodeT as _, Queryable as _},
};

use crate::table_editor::EditorOrigin;

/// Build an application whose settings live in a throwaway directory.
///
/// `Store::open()` would otherwise point at the real per-user config, and a
/// test run would append its fixture queries to the developer's history.
fn test_app() -> DbcApp {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let directory = std::env::temp_dir().join(format!(
        "dbc-test-config-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let _ignored = std::fs::remove_dir_all(&directory);
    DbcApp::new(crate::store::Store::open_in(directory), false)
        .expect("application should initialize")
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
fn write_action_requires_a_matching_second_activation() {
    let mut app = test_app();
    let text = "DELETE FROM items";

    assert!(!app.confirm_write(WriteAction::Execute, text));
    assert!(app.confirm_write(WriteAction::Execute, text));
    assert!(app.tab_mut().pending_write_confirmation.is_none());
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
    let mut app = test_app();
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
    app.tab_mut().active_operation_id = Some(5);
    app.tab_mut().active_result = Some(BufferedQueryResult::new(
        QuerySettings::default().buffer_limits(),
    ));
    app.tab_mut().query_events = Some(ActiveQueryEvents {
        session_generation: 3,
        operation_id: 5,
        receiver,
    });

    app.drain_query_events();

    assert_eq!(
        app.tab_mut().active_result
            .as_ref()
            .expect("active result should remain")
            .buffer
            .row_count(),
        1
    );
}

#[test]
fn cancelling_query_keeps_buffered_rows() {
    let mut app = test_app();
    let mut result = BufferedQueryResult::new(QuerySettings::default().buffer_limits());
    result.apply(QueryEvent::Rows(DataBatch::Documents(vec![
        serde_json::json!({"id": 1}),
    ])));
    app.tab_mut().active_result = Some(result);
    let cancellation = tokio_util::sync::CancellationToken::new();
    app.begin_operation(9, OperationKind::Query, cancellation.clone());

    assert!(app.cancel_active_operation());
    assert!(cancellation.is_cancelled());
    assert_eq!(
        app.tab_mut().current_result
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
    let mut app = test_app();
    app.endpoint = "custom".to_owned();
    app.tab_mut().query_text = "custom query".to_owned();

    app.select_driver(3);

    assert_eq!(app.choice().id, "mongodb");
    assert_eq!(app.endpoint, "mongodb://127.0.0.1:27017");
    assert!(app.tab().query_text.starts_with("db.items.find("));
    assert!(app.password.is_empty());
}

#[test]
fn collapsed_object_branch_hides_loaded_descendants() {
    let mut app = test_app();
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
fn tabs_keep_their_own_results_and_tables_open_in_their_own_tab() {
    let mut harness = Harness::builder()
        .with_size([1440.0, 900.0])
        .build_eframe(|_| test_app());
    let ctx = harness.ctx.clone();
    harness.state_mut().select_driver(2);
    harness.state_mut().connect(&ctx);
    wait_for_app(&mut harness, |app| !app.connection_busy, "SQLite connection");

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
    execute_query(&mut harness, "SELECT id, name FROM items ORDER BY id", false);
    let first_tab = harness.state().tab().id;
    assert_eq!(
        harness
            .state()
            .tab()
            .current_result
            .as_deref()
            .expect("first tab keeps its result")
            .buffer
            .row_count(),
        2
    );

    // A second tab must not inherit or overwrite the first tab's result.
    harness.state_mut().open_query_tab();
    assert_eq!(harness.state().tabs.len(), 2);
    assert_ne!(harness.state().tab().id, first_tab);
    assert!(harness.state().tab().current_result.is_none());

    execute_query(&mut harness, "SELECT id FROM items WHERE id = 1", false);
    assert_eq!(
        harness
            .state()
            .tab()
            .current_result
            .as_deref()
            .expect("second tab has its own result")
            .buffer
            .row_count(),
        1
    );
    let first_index = harness
        .state()
        .tab_index(first_tab)
        .expect("first tab is still open");
    assert_eq!(
        harness.state().tabs[first_index]
            .current_result
            .as_deref()
            .expect("first tab still holds its own rows")
            .buffer
            .row_count(),
        2,
        "a query in one tab must not disturb another"
    );

    // Opening a table adds a tab instead of replacing the query being written.
    harness
        .state_mut()
        .open_table(TableRef::new(Vec::<String>::new(), "items"), &ctx);
    assert_eq!(harness.state().tabs.len(), 3);
    assert_eq!(harness.state().tab().title, "items");
    wait_for_app(
        &mut harness,
        |app| !app.tab().operation_busy && app.tab().table_editor.is_some(),
        "table data load",
    );

    // Re-opening the same table raises the existing tab rather than duplicating it.
    let table_tab = harness.state().tab().id;
    harness.state_mut().select_tab(0);
    harness
        .state_mut()
        .open_table(TableRef::new(Vec::<String>::new(), "items"), &ctx);
    assert_eq!(harness.state().tabs.len(), 3);
    assert_eq!(harness.state().tab().id, table_tab);

    // Closing the table tab leaves the query tabs untouched.
    let index = harness
        .state()
        .tab_index(table_tab)
        .expect("table tab is open");
    harness.state_mut().close_tab(index);
    assert_eq!(harness.state().tabs.len(), 2);
    assert!(harness.state().tab_index(table_tab).is_none());

    harness.state_mut().disconnect();
}

#[test]
fn minimum_and_default_windows_render_key_accessible_controls() {
    for size in [[960.0, 640.0], [1440.0, 900.0]] {
        let mut harness = Harness::builder()
            .with_size(size)
            .build_eframe(|_| test_app());
        harness.run();

        assert!(harness.get_by_label("执行").accesskit_node().is_disabled());
        assert!(!harness.get_by_label("连接").accesskit_node().is_disabled());
        let _endpoint = harness.get_by_label("连接地址");
        let _result_tab = harness.get_by_label("数据");

        // The sidebar toggle is the only panel switch left; the capability
        // panel it used to sit next to is gone.
        harness.get_by_label("连接与对象").click();
        harness.run();
        assert!(!harness.state().left_panel_open);
    }
}

#[test]
fn sqlite_desktop_workflow_is_closed_loop() {
    let mut harness = Harness::builder()
        .with_size([1440.0, 900.0])
        .build_eframe(|_| test_app());
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
        .tab()
        .current_result
        .as_deref()
        .expect("SELECT should retain a current result");
    assert_eq!(result.buffer.row_count(), 2);
    assert!(harness.state().status.full_text().starts_with("执行完成"));

    harness.state_mut().tab_mut().query_text = "SELECT 1; SELECT 2".to_owned();
    harness.state_mut().execute(&ctx);
    assert!(harness.state_mut().tab_mut().pending_write_confirmation.is_some());
    harness.state_mut().execute(&ctx);
    assert!(harness.state_mut().tab_mut().operation_busy);
    wait_for_app(
        &mut harness,
        |app| !app.tab().operation_busy,
        "invalid query failure",
    );
    assert!(harness.state().status.full_text().starts_with("执行失败"));
    assert!(harness.state().tab().active_cancellation.is_none());

    execute_query(
        &mut harness,
        "SELECT id, name FROM items ORDER BY id",
        false,
    );
    assert_eq!(
        harness
            .state()
            .tab()
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
        app.load_objects(None, 0, None, &ctx);
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
            !app.tab().operation_busy
                && app
                    .tab()
                    .table_editor
                    .as_ref()
                    .is_some_and(|editor| editor.origin() == EditorOrigin::Table)
        },
        "table data load",
    );
    {
        let editor = harness
            .state_mut()
            .tab_mut()
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
    harness.state_mut().tab_mut().query_text = "SELECT 1".to_owned();
    harness.state_mut().execute(&ctx);
    assert!(!harness.state().tab().operation_busy);
    assert!(harness.state().status.full_text().contains("未保存变更"));

    harness.state_mut().preview_table_changes(&ctx);
    wait_for_app(
        &mut harness,
        |app| !app.tab().operation_busy && app.tab().prepared_table_change.is_some(),
        "parameterized SQL preview",
    );
    let prepared = harness
        .state()
        .tab()
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
            !app.tab().operation_busy
                && app
                    .tab()
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
            app.tab().table_editor
                .as_ref()
                .is_some_and(|editor| editor.origin() == EditorOrigin::Query)
        },
        "single-table query editability",
    );

    harness.state_mut().tab_mut().query_text = "SELECT id, name FROM items WHERE id = 1".to_owned();
    harness
        .state_mut()
        .start_explain(ExplainMode::Estimated, &ctx);
    assert!(harness.state_mut().tab_mut().operation_busy);
    wait_for_app(
        &mut harness,
        |app| !app.tab().operation_busy,
        "estimated execution plan",
    );
    let plan: serde_json::Value =
        serde_json::from_str(&harness.state().tab().plan_text).expect("SQLite plan should be JSON");
    assert!(plan.is_array());

    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let export_path = directory.path().join("items.csv");
    harness.state_mut().start_export(
        ExportScope::Current,
        ExportFormat::Csv,
        export_path.clone(),
        &ctx,
    );
    assert!(harness.state_mut().tab_mut().operation_busy);
    wait_for_app(
        &mut harness,
        |app| !app.tab().operation_busy,
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
    assert!(harness.state_mut().tab_mut().operation_busy);
    wait_for_app(&mut harness, |app| !app.tab().operation_busy, "full query export");
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
    assert_eq!(harness.state().status.full_text(), "连接已断开");
    assert!(harness.get_by_label("执行").accesskit_node().is_disabled());
}

fn execute_query(
    harness: &mut Harness<'_, DbcApp>,
    text: &str,
    requires_write_confirmation: bool,
) {
    let ctx = harness.ctx.clone();
    harness.state_mut().tab_mut().query_text = text.to_owned();
    harness.state_mut().execute(&ctx);
    if requires_write_confirmation {
        assert!(!harness.state_mut().tab_mut().operation_busy);
        assert!(harness.state_mut().tab_mut().pending_write_confirmation.is_some());
        harness.state_mut().execute(&ctx);
    }
    assert!(harness.state_mut().tab_mut().operation_busy);
    wait_for_app(harness, |app| !app.tab().operation_busy, "query completion");
    assert!(
        !harness.state().status.full_text().contains("失败"),
        "query failed: {}",
        harness.state().status.full_text()
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
        harness.state().status.full_text()
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

