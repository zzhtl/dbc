//! Display strings for database values, object kinds and driver metadata.
//!
//! Kept apart from the widgets so the wording of a filter operator or a
//! read-only reason can be reviewed in one place instead of being scattered
//! through the layout code.

use dbc_core::{
    metadata::DatabaseObjectKind,
    query_editability::QueryEditabilityReason,
    result::CellValue,
    table_data::FilterOperator,
};

use crate::drivers::DRIVER_CHOICES;

pub(crate) const FILTER_OPERATORS: &[FilterOperator] = &[
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

pub(crate) const fn filter_operator_label(operator: FilterOperator) -> &'static str {
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

pub(crate) const fn filter_operator_value_count(operator: FilterOperator) -> usize {
    match operator {
        FilterOperator::IsNull | FilterOperator::IsNotNull => 0,
        FilterOperator::Between | FilterOperator::NotBetween => 2,
        _ => 1,
    }
}

pub(crate) fn filter_values_label(values: &[CellValue]) -> String {
    values
        .iter()
        .map(compact_cell_value_label)
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn parameter_value_label(value: &CellValue) -> String {
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

pub(crate) fn compact_cell_value_label(value: &CellValue) -> String {
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

pub(crate) const fn query_editability_reason_label(reason: QueryEditabilityReason) -> &'static str {
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

pub(crate) fn object_kind_icon(kind: &DatabaseObjectKind) -> &'static str {
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

pub(crate) fn object_kind_label(kind: &DatabaseObjectKind) -> &'static str {
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

pub(crate) fn compact_count(value: usize) -> String {
    if value >= 1_000 && value.is_multiple_of(1_000) {
        format!("{}k", value / 1_000)
    } else {
        value.to_string()
    }
}

/// Human-readable driver name for a stored `driver_id`.
/// One-line preview of a stored query for the history menu.
pub(crate) fn history_label(text: &str) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut characters = collapsed.chars();
    let head: String = characters.by_ref().take(64).collect();
    if characters.next().is_some() {
        format!("{head}…")
    } else {
        head
    }
}

pub(crate) fn driver_display_name(driver_id: &str) -> &str {
    DRIVER_CHOICES
        .iter()
        .find(|choice| choice.id == driver_id)
        .map_or(driver_id, |choice| choice.name)
}
