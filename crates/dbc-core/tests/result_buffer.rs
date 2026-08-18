use std::sync::Arc;

use dbc_core::result::{
    AppendOutcome, BufferLimits, ColumnSchema, DataBatch, ResultBuffer, RowBatchBuilder,
};

fn batch(ids: &[i64], names: &[&str]) -> DataBatch {
    let schema: Arc<[ColumnSchema]> = vec![
        ColumnSchema::new("id", "bigint"),
        ColumnSchema::new("name", "text"),
    ]
    .into();
    let mut builder = RowBatchBuilder::new(schema, ids.len());
    for (id, name) in ids.iter().zip(names) {
        let row = [id.to_string(), (*name).to_owned()];
        builder
            .push_row(|index| Ok::<_, ()>(Some(row[index].clone())))
            .expect("fixture rows should decode");
    }
    DataBatch::Tabular(builder.take_batch())
}

#[test]
fn buffer_slices_the_last_batch_at_the_row_limit() {
    let mut buffer = ResultBuffer::new(BufferLimits {
        max_rows: 3,
        max_bytes: usize::MAX,
    });

    assert_eq!(
        buffer.append(batch(&[1, 2], &["a", "b"])),
        AppendOutcome::Accepted {
            rows: 2,
            limit_reached: false,
        }
    );
    assert_eq!(
        buffer.append(batch(&[3, 4], &["c", "d"])),
        AppendOutcome::Accepted {
            rows: 1,
            limit_reached: true,
        }
    );
    assert_eq!(buffer.row_count(), 3);
    assert_eq!(buffer.batches().len(), 2);
    assert_eq!(buffer.batches()[1].row_count(), 1);
}

#[test]
fn buffer_rejects_new_batches_once_a_limit_is_reached() {
    let first = batch(&[1], &["large value"]);
    let exact_bytes = first.estimated_bytes();
    let mut buffer = ResultBuffer::new(BufferLimits {
        max_rows: 100,
        max_bytes: exact_bytes,
    });

    assert!(matches!(
        buffer.append(first),
        AppendOutcome::Accepted {
            rows: 1,
            limit_reached: true
        }
    ));
    assert_eq!(
        buffer.append(batch(&[2], &["another"])),
        AppendOutcome::LimitAlreadyReached
    );
    assert_eq!(buffer.row_count(), 1);
}

#[test]
fn document_batches_preserve_json_and_support_slicing() {
    let documents = DataBatch::Documents(vec![
        serde_json::json!({"_id": 1, "name": "one"}),
        serde_json::json!({"_id": 2, "name": "two"}),
    ]);

    let sliced = documents.slice(1, 1);

    assert_eq!(sliced.row_count(), 1);
    assert_eq!(
        sliced.documents(),
        Some(&[serde_json::json!({"_id": 2, "name": "two"})][..])
    );
}

#[test]
fn buffer_slice_spans_batch_boundaries() {
    let mut buffer = ResultBuffer::new(BufferLimits {
        max_rows: 10,
        max_bytes: usize::MAX,
    });
    assert!(matches!(
        buffer.append(batch(&[1, 2], &["a", "b"])),
        AppendOutcome::Accepted { rows: 2, .. }
    ));
    assert!(matches!(
        buffer.append(batch(&[3, 4], &["c", "d"])),
        AppendOutcome::Accepted { rows: 2, .. }
    ));

    let page = buffer.slice(1, 2);

    assert_eq!(page.len(), 2);
    assert_eq!(page[0].row_count(), 1);
    assert_eq!(page[1].row_count(), 1);
}
