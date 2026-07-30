use arrow_array::{
    Array, BinaryArray, BooleanArray, Float64Array, Int64Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, UInt64Array,
};
use dbc_core::diagnostics::SlowQueryPage;
use dbc_data::{CellValue, DataBatch, DataSchema};
use eframe::egui;
use egui_extras::{Column, TableBuilder};

const DISPLAY_CELL_MAX_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, Default)]
pub struct ResultModel {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl ResultModel {
    pub fn message(message: impl Into<String>) -> Self {
        Self {
            columns: vec!["消息".to_owned()],
            rows: vec![vec![message.into()]],
        }
    }

    pub fn from_page(
        schema: Option<&DataSchema>,
        batches: &[DataBatch],
        messages: &[String],
        affected_rows: u64,
    ) -> Self {
        let mut model = Self::default();
        if let Some(schema) = schema {
            model.columns = schema_columns(schema);
        }
        for batch in batches {
            append_batch(&mut model, batch);
        }
        if model.columns.is_empty() {
            for message in messages {
                append_message(&mut model, message.clone());
            }
            if affected_rows > 0 {
                append_message(&mut model, format!("影响 {affected_rows} 行"));
            }
        }
        if model.columns.is_empty() {
            model = Self::message("命令执行成功，没有返回结果集");
        }
        model
    }

    pub fn from_slow_queries(page: SlowQueryPage) -> Self {
        let rows = page
            .entries
            .into_iter()
            .map(|entry| {
                vec![
                    entry.query,
                    entry.database.unwrap_or_default(),
                    entry.user.unwrap_or_default(),
                    entry.calls.to_string(),
                    format!("{:.3}", entry.mean_time_millis),
                    format!("{:.3}", entry.total_time_millis),
                    entry.rows.to_string(),
                ]
            })
            .collect();
        Self {
            columns: vec![
                "查询".to_owned(),
                "数据库".to_owned(),
                "用户".to_owned(),
                "次数".to_owned(),
                "平均耗时(ms)".to_owned(),
                "总耗时(ms)".to_owned(),
                "返回行".to_owned(),
            ],
            rows,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResultGridState {
    model: ResultModel,
    column_order: Vec<usize>,
}

impl ResultGridState {
    pub fn new(model: ResultModel) -> Self {
        let column_order = (0..model.columns.len()).collect();
        Self {
            model,
            column_order,
        }
    }

    pub fn replace(&mut self, model: ResultModel) {
        if self.model.columns != model.columns {
            self.column_order = (0..model.columns.len()).collect();
        }
        self.model = model;
    }

    pub fn show(&mut self, ui: &mut egui::Ui, id_salt: &'static str) {
        let order = self.column_order.clone();
        let minimum_width = order
            .iter()
            .enumerate()
            .map(|(position, _)| if position == 0 { 260.0 } else { 150.0 })
            .sum::<f32>();
        let mut pending_move = None;

        egui::ScrollArea::horizontal()
            .id_salt((id_salt, "horizontal"))
            .show(ui, |ui| {
                ui.set_min_width(minimum_width.max(ui.available_width()));
                let available_height = ui.available_height();
                let mut table = TableBuilder::new(ui)
                    .id_salt(id_salt)
                    .striped(true)
                    .resizable(true)
                    .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
                    .min_scrolled_height(0.0)
                    .max_scroll_height(available_height);
                for (position, _) in order.iter().enumerate() {
                    let width = if position == 0 { 260.0 } else { 150.0 };
                    table = table.column(
                        Column::initial(width)
                            .at_least(72.0)
                            .at_most(640.0)
                            .clip(true),
                    );
                }

                table
                    .header(28.0, |mut header| {
                        for &column_index in &order {
                            header.col(|ui| {
                                let (_, dropped) =
                                    ui.dnd_drop_zone::<usize, _>(egui::Frame::NONE, |ui| {
                                        ui.dnd_drag_source(
                                            ui.id().with(("result-column", column_index)),
                                            column_index,
                                            |ui| {
                                                ui.strong(
                                                    self.model.columns[column_index].as_str(),
                                                );
                                            },
                                        );
                                    });
                                if let Some(source) = dropped {
                                    pending_move = Some((*source, column_index));
                                }
                            });
                        }
                    })
                    .body(|body| {
                        body.rows(24.0, self.model.rows.len(), |mut row| {
                            let row_index = row.index();
                            for &column_index in &order {
                                row.col(|ui| {
                                    let value = self.model.rows[row_index]
                                        .get(column_index)
                                        .map_or("", String::as_str);
                                    ui.add(egui::Label::new(value).selectable(true).truncate())
                                        .on_hover_text(value);
                                });
                            }
                        });
                    });
            });

        if let Some((source, target)) = pending_move {
            self.move_column(source, target);
        }
    }

    fn move_column(&mut self, source: usize, target: usize) {
        if source == target {
            return;
        }
        let Some(source_position) = self
            .column_order
            .iter()
            .position(|column| *column == source)
        else {
            return;
        };
        let Some(target_position) = self
            .column_order
            .iter()
            .position(|column| *column == target)
        else {
            return;
        };
        let column = self.column_order.remove(source_position);
        self.column_order.insert(target_position, column);
    }
}

pub(crate) fn tabular_cells(
    schema: Option<&DataSchema>,
    batches: &[DataBatch],
) -> Option<(Vec<String>, Vec<Vec<CellValue>>)> {
    let DataSchema::Tabular(schema) = schema? else {
        return None;
    };
    let columns = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    for batch in batches {
        let DataBatch::Tabular(batch) = batch else {
            return None;
        };
        for row_index in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|array| array_cell_value(array.as_ref(), row_index))
                    .collect(),
            );
        }
    }
    Some((columns, rows))
}

fn append_batch(model: &mut ResultModel, batch: &DataBatch) {
    match batch {
        DataBatch::Tabular(batch) => append_record_batch(model, batch),
        DataBatch::Documents(documents) => {
            ensure_columns(model, &["文档"]);
            model.rows.extend(documents.iter().map(|document| {
                vec![truncate_display(
                    serde_json::to_string_pretty(document)
                        .unwrap_or_else(|_| document.to_string()),
                )]
            }));
        }
        DataBatch::KeyValues(entries) => {
            ensure_columns(model, &["键", "值", "类型", "TTL(ms)"]);
            model.rows.extend(entries.iter().map(|entry| {
                vec![
                    truncate_display(String::from_utf8_lossy(&entry.key).into_owned()),
                    truncate_display(String::from_utf8_lossy(&entry.value).into_owned()),
                    truncate_display(entry.value_type.clone()),
                    entry
                        .ttl_millis
                        .map(|ttl| ttl.to_string())
                        .unwrap_or_default(),
                ]
            }));
        }
    }
}

fn append_record_batch(model: &mut ResultModel, batch: &RecordBatch) {
    if model.columns.is_empty() {
        model.columns = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
    }
    for row_index in 0..batch.num_rows() {
        model.rows.push(
            batch
                .columns()
                .iter()
                .map(|array| truncate_display(array_value(array.as_ref(), row_index)))
                .collect(),
        );
    }
}

fn array_value(array: &dyn Array, row_index: usize) -> String {
    if array.is_null(row_index) {
        return "NULL".to_owned();
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return array.value(row_index).to_owned();
    }
    if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        return array.value(row_index).to_string();
    }
    if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        return array.value(row_index).to_string();
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        return array.value(row_index).to_string();
    }
    if let Some(array) = array.as_any().downcast_ref::<BooleanArray>() {
        return array.value(row_index).to_string();
    }
    format!("<{}>", array.data_type())
}

fn array_cell_value(array: &dyn Array, row_index: usize) -> CellValue {
    if array.is_null(row_index) {
        return CellValue::Null;
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return CellValue::Text(array.value(row_index).to_owned());
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return CellValue::Text(array.value(row_index).to_owned());
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        return CellValue::Binary(array.value(row_index).to_vec());
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return CellValue::Binary(array.value(row_index).to_vec());
    }
    CellValue::Text(array_value(array, row_index))
}

fn append_message(model: &mut ResultModel, message: String) {
    if model.columns.is_empty() {
        model.columns.push("消息".to_owned());
    }
    if model.columns.len() == 1 {
        model.rows.push(vec![message]);
    }
}

fn ensure_columns(model: &mut ResultModel, columns: &[&str]) {
    if model.columns.is_empty() {
        model.columns = columns.iter().map(|column| (*column).to_owned()).collect();
    }
}

fn schema_columns(schema: &DataSchema) -> Vec<String> {
    match schema {
        DataSchema::Tabular(schema) => schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect(),
        DataSchema::Documents => vec!["文档".to_owned()],
        DataSchema::KeyValues => vec![
            "键".to_owned(),
            "值".to_owned(),
            "类型".to_owned(),
            "TTL(ms)".to_owned(),
        ],
    }
}

fn truncate_display(mut value: String) -> String {
    if value.len() <= DISPLAY_CELL_MAX_BYTES {
        return value;
    }
    let mut boundary = DISPLAY_CELL_MAX_BYTES;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    value.truncate(boundary);
    value.push('…');
    value
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use dbc_data::{CellValue, DataBatch, DataSchema};

    use super::{
        DISPLAY_CELL_MAX_BYTES, ResultGridState, ResultModel, tabular_cells, truncate_display,
    };

    #[test]
    fn display_truncation_preserves_utf8_boundaries() {
        let value = "界".repeat(DISPLAY_CELL_MAX_BYTES);

        let truncated = truncate_display(value);

        assert!(truncated.ends_with('…'));
        assert!(truncated.is_char_boundary(truncated.len()));
        assert!(truncated.len() <= DISPLAY_CELL_MAX_BYTES + '…'.len_utf8());
    }

    #[test]
    fn column_order_can_move_and_resets_for_a_new_schema() {
        let mut grid = ResultGridState::new(ResultModel {
            columns: vec!["id".to_owned(), "name".to_owned(), "status".to_owned()],
            rows: Vec::new(),
        });

        grid.move_column(2, 0);
        assert_eq!(grid.column_order, vec![2, 0, 1]);

        grid.replace(ResultModel {
            columns: vec!["key".to_owned(), "value".to_owned()],
            rows: Vec::new(),
        });
        assert_eq!(grid.column_order, vec![0, 1]);
    }

    #[test]
    fn editable_cells_preserve_null_text_and_binary_distinctions() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("NULL"), None])) as ArrayRef,
                Arc::new(BinaryArray::from(vec![
                    Some(&[0_u8, 255_u8][..]),
                    None,
                ])) as ArrayRef,
            ],
        )
        .expect("record batch should build");
        let data = DataBatch::Tabular(batch);
        let schema = DataSchema::from_batch(&data);

        let (_, rows) = tabular_cells(Some(&schema), &[data])
            .expect("tabular cells should be available");
        assert_eq!(rows[0][0], CellValue::Text("NULL".to_owned()));
        assert_eq!(rows[1][0], CellValue::Null);
        assert_eq!(rows[0][1], CellValue::Binary(vec![0, 255]));
        assert_eq!(rows[1][1], CellValue::Null);
    }
}
