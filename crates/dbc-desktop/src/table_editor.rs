use std::collections::{BTreeMap, BTreeSet};

use dbc_core::{
    query_editability::QueryEditability,
    table_data::{
        TableChangeRequest, TableColumn, TableDelete, TableInsert, TableKind, TableMetadata,
        TablePage, TableUpdate,
    },
};
use dbc_data::CellValue;
use eframe::egui;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EditorOrigin {
    Table,
    Query,
}

#[derive(Debug, Clone)]
struct EditorColumn {
    label: String,
    source_column: Option<String>,
    metadata: Option<TableColumn>,
    editable: bool,
}

#[derive(Debug, Clone)]
struct EditorRow {
    original: Vec<CellValue>,
    values: Vec<CellValue>,
    inserted: bool,
    deleted: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct TableEditorState {
    origin: EditorOrigin,
    metadata: TableMetadata,
    columns: Vec<EditorColumn>,
    rows: Vec<EditorRow>,
    total_rows: u64,
    page_index: u64,
    page_size: u32,
    allow_insert: bool,
    allow_delete: bool,
    read_only_reason: Option<String>,
    revision: u64,
}

impl TableEditorState {
    pub(crate) fn from_table(metadata: TableMetadata, page: TablePage) -> Self {
        let stable_key = metadata.stable_key().is_some();
        let writable = metadata.kind == TableKind::Table && stable_key;
        let read_only_reason = if metadata.kind == TableKind::View {
            Some("视图以只读方式打开".to_owned())
        } else if !stable_key {
            Some("表没有非空主键或唯一键，无法安全定位行".to_owned())
        } else {
            None
        };
        let columns = metadata
            .columns
            .iter()
            .map(|column| EditorColumn {
                label: column.name.clone(),
                source_column: Some(column.name.clone()),
                metadata: Some(column.clone()),
                editable: writable && column.writable(),
            })
            .collect();
        let rows = page
            .rows
            .into_iter()
            .map(|values| EditorRow {
                original: values.clone(),
                values,
                inserted: false,
                deleted: false,
            })
            .collect();
        Self {
            origin: EditorOrigin::Table,
            metadata,
            columns,
            rows,
            total_rows: page.total_rows,
            page_index: page.page_index,
            page_size: page.page_size,
            allow_insert: writable,
            allow_delete: writable,
            read_only_reason,
            revision: 0,
        }
    }

    pub(crate) fn from_query(
        metadata: TableMetadata,
        editability: QueryEditability,
        result_columns: Vec<String>,
        mut rows: Vec<Vec<CellValue>>,
        page_size: u32,
    ) -> Self {
        let unsupported_sources = editability
            .columns
            .iter()
            .enumerate()
            .filter(|(index, column)| {
                column.source_column.is_some()
                    && rows.iter().any(|row| {
                        row.get(*index).is_some_and(|value| {
                            matches!(
                                value,
                                CellValue::Text(text) if text.starts_with("<unsupported ")
                            )
                        })
                    })
            })
            .filter_map(|(_, column)| column.source_column.clone())
            .collect::<BTreeSet<_>>();
        let unsupported_identity = metadata.stable_key().is_some_and(|key| {
            key.columns
                .iter()
                .any(|column| unsupported_sources.contains(column))
        });
        let columns = result_columns
            .into_iter()
            .zip(editability.columns)
            .map(|(label, editability)| {
                let unsupported = editability
                    .source_column
                    .as_ref()
                    .is_some_and(|source| unsupported_sources.contains(source));
                let metadata_column = editability
                    .source_column
                    .as_deref()
                    .and_then(|name| metadata.column(name))
                    .cloned();
                EditorColumn {
                    label,
                    source_column: editability.source_column,
                    metadata: metadata_column,
                    editable: editability.editable && !unsupported_identity && !unsupported,
                }
            })
            .collect::<Vec<_>>();
        for row in &mut rows {
            for (index, value) in row.iter_mut().enumerate() {
                let Some(column) = columns
                    .get(index)
                    .and_then(|column| column.metadata.as_ref())
                else {
                    continue;
                };
                normalize_binary_value(value, &column.database_type);
            }
        }
        let total_rows = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        let rows = rows
            .into_iter()
            .map(|values| EditorRow {
                original: values.clone(),
                values,
                inserted: false,
                deleted: false,
            })
            .collect();
        let read_only_reason = if unsupported_identity {
            Some("稳定键包含当前驱动无法无损解码的类型，查询结果只读".to_owned())
        } else if !unsupported_sources.is_empty() {
            Some("部分列类型无法无损解码；这些列、插入和删除操作已禁用".to_owned())
        } else {
            editability.reason.map(query_reason_fallback)
        };
        Self {
            origin: EditorOrigin::Query,
            metadata,
            columns,
            rows,
            total_rows,
            page_index: 0,
            page_size,
            allow_insert: editability.allow_insert
                && unsupported_sources.is_empty()
                && !unsupported_identity,
            allow_delete: editability.allow_delete
                && unsupported_sources.is_empty()
                && !unsupported_identity,
            read_only_reason,
            revision: 0,
        }
    }

    pub(crate) fn show(
        &mut self,
        ui: &mut egui::Ui,
        local_page: usize,
        local_page_size: usize,
    ) -> bool {
        let start = if self.origin == EditorOrigin::Query {
            local_page.saturating_mul(local_page_size)
        } else {
            0
        };
        let end = if self.origin == EditorOrigin::Query {
            start.saturating_add(local_page_size).min(self.rows.len())
        } else {
            self.rows.len()
        };
        let mut changed = false;

        egui::ScrollArea::both()
            .id_salt("editable-table-grid-scroll")
            .show(ui, |ui| {
                egui::Grid::new("editable-table-grid")
                    .striped(true)
                    .min_col_width(150.0)
                    .show(ui, |ui| {
                        ui.strong("操作");
                        for column in &self.columns {
                            let mut title = column.label.clone();
                            if !column.editable {
                                title.push_str(" 🔒");
                            }
                            ui.strong(title);
                        }
                        ui.end_row();

                        for row_index in start..end {
                            let row = &mut self.rows[row_index];
                            if row.inserted {
                                if ui.small_button("移除").clicked() {
                                    row.deleted = true;
                                    changed = true;
                                }
                            } else if self.allow_delete {
                                let label = if row.deleted {
                                    "撤销删除"
                                } else {
                                    "删除"
                                };
                                if ui.small_button(label).clicked() {
                                    row.deleted = !row.deleted;
                                    changed = true;
                                }
                            } else {
                                ui.weak("只读");
                            }

                            for (column_index, column) in self.columns.iter().enumerate() {
                                let Some(value) = row.values.get_mut(column_index) else {
                                    ui.weak("<缺失>");
                                    continue;
                                };
                                if row.deleted || !column.editable {
                                    ui.add(
                                        egui::Label::new(cell_display(value))
                                            .selectable(true)
                                            .truncate(),
                                    );
                                } else if edit_cell(ui, value, column, row.inserted) {
                                    changed = true;
                                }
                            }
                            ui.end_row();
                        }
                    });
            });

        if changed {
            self.rows.retain(|row| !(row.inserted && row.deleted));
            self.revision = self.revision.wrapping_add(1);
        }
        changed
    }

    pub(crate) fn add_row(&mut self) -> bool {
        if !self.allow_insert {
            return false;
        }
        let values = self
            .columns
            .iter()
            .map(|column| {
                let Some(metadata) = column.metadata.as_ref() else {
                    return CellValue::Default;
                };
                if !metadata.writable()
                    || metadata.auto_increment
                    || metadata.default_expression.is_some()
                {
                    CellValue::Default
                } else if metadata.nullable {
                    CellValue::Null
                } else if is_binary_type(&metadata.database_type) {
                    CellValue::Binary(Vec::new())
                } else {
                    CellValue::Text(String::new())
                }
            })
            .collect::<Vec<_>>();
        self.rows.push(EditorRow {
            original: Vec::new(),
            values,
            inserted: true,
            deleted: false,
        });
        self.revision = self.revision.wrapping_add(1);
        true
    }

    #[cfg(test)]
    pub(crate) fn set_cell(
        &mut self,
        row_index: usize,
        column_index: usize,
        value: CellValue,
    ) -> bool {
        let Some(column) = self.columns.get(column_index) else {
            return false;
        };
        let Some(row) = self.rows.get_mut(row_index) else {
            return false;
        };
        if row.deleted || !column.editable {
            return false;
        }
        let Some(current) = row.values.get_mut(column_index) else {
            return false;
        };
        if *current == value {
            return false;
        }
        *current = value;
        self.revision = self.revision.wrapping_add(1);
        true
    }

    pub(crate) fn discard_changes(&mut self) {
        self.rows.retain(|row| !row.inserted);
        for row in &mut self.rows {
            row.values.clone_from(&row.original);
            row.deleted = false;
        }
        self.revision = self.revision.wrapping_add(1);
    }

    pub(crate) fn change_request(
        &self,
        timeout: std::time::Duration,
    ) -> Result<TableChangeRequest, String> {
        let key = self
            .metadata
            .stable_key()
            .ok_or_else(|| "当前对象没有稳定行标识".to_owned())?;
        let mut inserts = Vec::new();
        let mut updates = Vec::new();
        let mut deletes = Vec::new();

        for row in &self.rows {
            if row.inserted {
                let mut values = BTreeMap::new();
                for (column_index, column) in self.columns.iter().enumerate() {
                    let Some(source) = column.source_column.as_ref() else {
                        continue;
                    };
                    if !column.metadata.as_ref().is_some_and(TableColumn::writable) {
                        continue;
                    }
                    if let Some(value) = row.values.get(column_index) {
                        values.insert(source.clone(), value.clone());
                    }
                }
                inserts.push(TableInsert { values });
                continue;
            }

            let identity = row_identity(key, &self.columns, &row.original)?;
            if row.deleted {
                deletes.push(TableDelete {
                    identity,
                    original_values: direct_values(&self.columns, &row.original),
                });
                continue;
            }

            let mut original_values = BTreeMap::new();
            let mut values = BTreeMap::new();
            for (column_index, column) in self.columns.iter().enumerate() {
                if !column.editable {
                    continue;
                }
                let (Some(original), Some(value), Some(source)) = (
                    row.original.get(column_index),
                    row.values.get(column_index),
                    column.source_column.as_ref(),
                ) else {
                    continue;
                };
                if original != value {
                    original_values.insert(source.clone(), original.clone());
                    values.insert(source.clone(), value.clone());
                }
            }
            if !values.is_empty() {
                updates.push(TableUpdate {
                    identity,
                    original_values,
                    values,
                });
            }
        }

        Ok(TableChangeRequest {
            id: Uuid::new_v4(),
            table: self.metadata.table.clone(),
            inserts,
            updates,
            deletes,
            timeout,
        })
    }

    pub(crate) fn pending_change_count(&self) -> usize {
        self.rows
            .iter()
            .filter(|row| {
                row.inserted
                    || row.deleted
                    || (!row.original.is_empty() && row.original != row.values)
            })
            .count()
    }

    #[cfg(test)]
    pub(crate) fn has_pending_changes(&self) -> bool {
        self.pending_change_count() > 0
    }

    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn origin(&self) -> EditorOrigin {
        self.origin
    }

    pub(crate) fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    pub(crate) fn total_rows(&self) -> u64 {
        self.total_rows
    }

    pub(crate) fn page_index(&self) -> u64 {
        self.page_index
    }

    pub(crate) fn page_size(&self) -> u32 {
        self.page_size
    }

    pub(crate) fn allow_insert(&self) -> bool {
        self.allow_insert
    }

    pub(crate) fn read_only_reason(&self) -> Option<&str> {
        self.read_only_reason.as_deref()
    }
}

fn row_identity(
    key: &dbc_core::table_data::UniqueKey,
    columns: &[EditorColumn],
    values: &[CellValue],
) -> Result<BTreeMap<String, CellValue>, String> {
    key.columns
        .iter()
        .map(|key_column| {
            let index = columns
                .iter()
                .position(|column| column.source_column.as_deref() == Some(key_column))
                .ok_or_else(|| format!("结果未返回行标识列 {key_column}"))?;
            let value = values
                .get(index)
                .cloned()
                .ok_or_else(|| format!("结果缺少行标识值 {key_column}"))?;
            Ok((key_column.clone(), value))
        })
        .collect()
}

fn direct_values(columns: &[EditorColumn], values: &[CellValue]) -> BTreeMap<String, CellValue> {
    columns
        .iter()
        .zip(values)
        .filter_map(|(column, value)| {
            column
                .source_column
                .as_ref()
                .map(|source| (source.clone(), value.clone()))
        })
        .collect()
}

fn edit_cell(
    ui: &mut egui::Ui,
    value: &mut CellValue,
    column: &EditorColumn,
    inserted: bool,
) -> bool {
    let mut replacement = None;
    let mut changed = false;
    ui.horizontal(|ui| {
        ui.menu_button("⋯", |ui| {
            if column
                .metadata
                .as_ref()
                .is_some_and(|metadata| metadata.nullable)
                && ui.button("设为 NULL").clicked()
            {
                replacement = Some(CellValue::Null);
                ui.close();
            }
            if inserted && ui.button("使用 DEFAULT").clicked() {
                replacement = Some(CellValue::Default);
                ui.close();
            }
            if matches!(value, CellValue::Null | CellValue::Default)
                && ui.button("输入值").clicked()
            {
                replacement = Some(
                    if column
                        .metadata
                        .as_ref()
                        .is_some_and(|metadata| is_binary_type(&metadata.database_type))
                    {
                        CellValue::Binary(Vec::new())
                    } else {
                        CellValue::Text(String::new())
                    },
                );
                ui.close();
            }
        });
        match value {
            CellValue::Null => {
                ui.weak("NULL");
            }
            CellValue::Default => {
                ui.weak("DEFAULT");
            }
            CellValue::Text(text) => {
                if ui
                    .add(
                        egui::TextEdit::singleline(text)
                            .desired_width(140.0)
                            .clip_text(true),
                    )
                    .changed()
                {
                    changed = true;
                }
            }
            CellValue::Binary(bytes) => {
                let mut text = encode_hex(bytes);
                let response = ui.add(
                    egui::TextEdit::singleline(&mut text)
                        .desired_width(140.0)
                        .clip_text(true),
                );
                if response.changed()
                    && let Some(decoded) = decode_hex(&text)
                {
                    replacement = Some(CellValue::Binary(decoded));
                }
            }
        }
    });
    if let Some(replacement) = replacement
        && *value != replacement
    {
        *value = replacement;
        return true;
    }
    changed
}

fn cell_display(value: &CellValue) -> String {
    match value {
        CellValue::Null => "NULL".to_owned(),
        CellValue::Text(value) => value.clone(),
        CellValue::Binary(value) => encode_hex(value),
        CellValue::Default => "DEFAULT".to_owned(),
    }
}

fn normalize_binary_value(value: &mut CellValue, database_type: &str) {
    if !is_binary_type(database_type) {
        return;
    }
    let CellValue::Text(text) = value else {
        return;
    };
    if let Some(bytes) = decode_hex(text) {
        *value = CellValue::Binary(bytes);
    }
}

pub(crate) fn input_cell_value(database_type: &str, input: &str) -> Result<CellValue, String> {
    if !is_binary_type(database_type) {
        return Ok(CellValue::Text(input.to_owned()));
    }
    decode_hex(input)
        .map(CellValue::Binary)
        .ok_or_else(|| "二进制值必须使用 0x 十六进制格式".to_owned())
}

fn is_binary_type(database_type: &str) -> bool {
    let normalized = database_type.to_ascii_lowercase();
    normalized == "bytea"
        || normalized.contains("binary")
        || normalized.contains("blob")
        || normalized.starts_with("bit")
        || normalized.starts_with("geometry")
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len().saturating_mul(2).saturating_add(2));
    result.push_str("0x");
    for byte in bytes {
        result.push(char::from(HEX[usize::from(byte >> 4)]));
        result.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    result
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    let trimmed = value.trim();
    let digits = if let Some(digits) = trimmed.strip_prefix("0x") {
        digits
    } else if let Some(digits) = trimmed.strip_prefix("\\x") {
        digits
    } else if trimmed.starts_with("X'") && trimmed.ends_with('\'') {
        &trimmed[2..trimmed.len().saturating_sub(1)]
    } else {
        return None;
    };
    if !digits.len().is_multiple_of(2) {
        return None;
    }
    digits
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_value(pair[0])?;
            let low = hex_value(pair[1])?;
            Some((high << 4) | low)
        })
        .collect()
}

const fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn query_reason_fallback(reason: dbc_core::query_editability::QueryEditabilityReason) -> String {
    format!("查询结果只读：{reason:?}")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use dbc_core::table_data::{
        TableColumn, TableKind, TableMetadata, TablePage, TableRef, UniqueKey,
    };
    use dbc_data::CellValue;
    use uuid::Uuid;

    use super::TableEditorState;

    fn metadata() -> TableMetadata {
        TableMetadata {
            table: TableRef::new(Vec::<String>::new(), "items"),
            kind: TableKind::Table,
            columns: vec![
                TableColumn {
                    name: "id".to_owned(),
                    database_type: "INTEGER".to_owned(),
                    nullable: false,
                    ordinal: 1,
                    default_expression: None,
                    generated: false,
                    auto_increment: true,
                },
                TableColumn {
                    name: "name".to_owned(),
                    database_type: "TEXT".to_owned(),
                    nullable: false,
                    ordinal: 2,
                    default_expression: None,
                    generated: false,
                    auto_increment: false,
                },
            ],
            unique_keys: vec![UniqueKey {
                name: "items_pk".to_owned(),
                columns: vec!["id".to_owned()],
                primary: true,
            }],
        }
    }

    #[test]
    fn staged_changes_keep_original_identity_and_can_be_discarded() {
        let page = TablePage {
            request_id: Uuid::new_v4(),
            table: metadata().table,
            columns: metadata().columns,
            rows: vec![vec![
                CellValue::Text("1".to_owned()),
                CellValue::Text("alpha".to_owned()),
            ]],
            total_rows: 1,
            page_index: 0,
            page_size: 200,
        };
        let mut editor = TableEditorState::from_table(metadata(), page);
        assert!(editor.set_cell(0, 1, CellValue::Text("changed".to_owned())));

        let request = editor
            .change_request(Duration::from_secs(5))
            .expect("change request should build");
        assert_eq!(
            request.updates[0].identity["id"],
            CellValue::Text("1".to_owned())
        );
        assert_eq!(
            request.updates[0].original_values["name"],
            CellValue::Text("alpha".to_owned())
        );

        editor.discard_changes();
        assert!(!editor.has_pending_changes());
    }

    #[test]
    fn tables_without_stable_keys_are_read_only() {
        let mut without_key = metadata();
        without_key.unique_keys.clear();
        let page = TablePage {
            request_id: Uuid::new_v4(),
            table: without_key.table.clone(),
            columns: without_key.columns.clone(),
            rows: Vec::new(),
            total_rows: 0,
            page_index: 0,
            page_size: 200,
        };
        let editor = TableEditorState::from_table(without_key, page);

        assert!(!editor.allow_insert());
        assert!(editor.read_only_reason().is_some());
    }
}
