use std::{collections::BTreeMap, time::Duration};

use dbc_data::CellValue;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const MAX_TABLE_PAGE_SIZE: u32 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TableRef {
    pub qualifiers: Vec<String>,
    pub name: String,
}

impl TableRef {
    #[must_use]
    pub fn new<I, S>(qualifiers: I, name: impl Into<String>) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            qualifiers: qualifiers.into_iter().map(Into::into).collect(),
            name: name.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableKind {
    Table,
    View,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableColumn {
    pub name: String,
    pub database_type: String,
    pub nullable: bool,
    pub ordinal: u32,
    pub default_expression: Option<String>,
    pub generated: bool,
    pub auto_increment: bool,
}

impl TableColumn {
    #[must_use]
    pub fn writable(&self) -> bool {
        !self.generated
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniqueKey {
    pub name: String,
    pub columns: Vec<String>,
    pub primary: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableMetadata {
    pub table: TableRef,
    pub kind: TableKind,
    pub columns: Vec<TableColumn>,
    pub unique_keys: Vec<UniqueKey>,
}

impl TableMetadata {
    #[must_use]
    pub fn column(&self, name: &str) -> Option<&TableColumn> {
        self.columns.iter().find(|column| column.name == name)
    }

    #[must_use]
    pub fn stable_key(&self) -> Option<&UniqueKey> {
        if self.kind != TableKind::Table {
            return None;
        }

        self.unique_keys
            .iter()
            .filter(|key| self.key_is_stable(key))
            .min_by_key(|key| (!key.primary, key.columns.len(), key.name.as_str()))
    }

    fn key_is_stable(&self, key: &UniqueKey) -> bool {
        !key.columns.is_empty()
            && key
                .columns
                .iter()
                .all(|name| self.column(name).is_some_and(|column| !column.nullable))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOperator {
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Like,
    NotLike,
    In,
    NotIn,
    Between,
    NotBetween,
    IsNull,
    IsNotNull,
}

impl FilterOperator {
    fn accepts_value_count(self, count: usize) -> bool {
        match self {
            Self::IsNull | Self::IsNotNull => count == 0,
            Self::In | Self::NotIn => count > 0,
            Self::Between | Self::NotBetween => count == 2,
            Self::Equals
            | Self::NotEquals
            | Self::GreaterThan
            | Self::GreaterThanOrEqual
            | Self::LessThan
            | Self::LessThanOrEqual
            | Self::Like
            | Self::NotLike => count == 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableFilter {
    pub column: String,
    pub operator: FilterOperator,
    pub values: Vec<CellValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSort {
    pub column: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableBrowseRequest {
    pub id: Uuid,
    pub table: TableRef,
    pub filters: Vec<TableFilter>,
    pub sort: Option<Vec<TableSort>>,
    pub raw_where: Option<String>,
    pub raw_order_by: Option<String>,
    pub page_index: u64,
    pub page_size: u32,
    pub timeout: Duration,
}

impl TableBrowseRequest {
    /// Validate the request against driver-provided metadata.
    ///
    /// Raw SQL fragments are parsed by the concrete driver using its dialect.
    pub fn validate(&self, metadata: &TableMetadata) -> Result<(), TableDataError> {
        if self.table != metadata.table {
            return Err(TableDataError::TableMismatch);
        }
        if self.page_size == 0 || self.page_size > MAX_TABLE_PAGE_SIZE {
            return Err(TableDataError::InvalidPageSize {
                maximum: MAX_TABLE_PAGE_SIZE,
            });
        }
        if self.timeout.is_zero() {
            return Err(TableDataError::InvalidTimeout);
        }

        for filter in &self.filters {
            if metadata.column(&filter.column).is_none() {
                return Err(TableDataError::UnknownColumn(filter.column.clone()));
            }
            if !filter.operator.accepts_value_count(filter.values.len()) {
                return Err(TableDataError::InvalidFilterArity {
                    column: filter.column.clone(),
                    operator: filter.operator,
                });
            }
            if filter
                .values
                .iter()
                .any(|value| matches!(value, CellValue::Default))
            {
                return Err(TableDataError::DefaultOutsideInsert);
            }
        }

        if let Some(sort) = &self.sort {
            for item in sort {
                if metadata.column(&item.column).is_none() {
                    return Err(TableDataError::UnknownColumn(item.column.clone()));
                }
            }
        }

        validate_raw_fragment(self.raw_where.as_deref(), "WHERE")?;
        validate_raw_fragment(self.raw_order_by.as_deref(), "ORDER BY")?;
        Ok(())
    }
}

fn validate_raw_fragment(fragment: Option<&str>, name: &'static str) -> Result<(), TableDataError> {
    if fragment.is_some_and(|fragment| fragment.trim().is_empty()) {
        return Err(TableDataError::EmptyRawFragment(name));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TablePage {
    pub request_id: Uuid,
    pub table: TableRef,
    pub columns: Vec<TableColumn>,
    pub rows: Vec<Vec<CellValue>>,
    pub total_rows: u64,
    pub page_index: u64,
    pub page_size: u32,
}

pub type RowValues = BTreeMap<String, CellValue>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableInsert {
    pub values: RowValues,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableUpdate {
    pub identity: RowValues,
    pub original_values: RowValues,
    pub values: RowValues,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDelete {
    pub identity: RowValues,
    pub original_values: RowValues,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableChangeRequest {
    pub id: Uuid,
    pub table: TableRef,
    pub inserts: Vec<TableInsert>,
    pub updates: Vec<TableUpdate>,
    pub deletes: Vec<TableDelete>,
    pub timeout: Duration,
}

impl TableChangeRequest {
    pub fn validate(&self, metadata: &TableMetadata) -> Result<(), TableDataError> {
        if self.table != metadata.table {
            return Err(TableDataError::TableMismatch);
        }
        if self.timeout.is_zero() {
            return Err(TableDataError::InvalidTimeout);
        }
        let key = metadata
            .stable_key()
            .ok_or(TableDataError::NoStableRowIdentity)?;

        for insert in &self.inserts {
            validate_values(metadata, &insert.values, true, true)?;
        }
        for update in &self.updates {
            validate_identity(metadata, key, &update.identity)?;
            validate_values(metadata, &update.original_values, false, false)?;
            validate_values(metadata, &update.values, false, true)?;
            if update.values.is_empty() {
                return Err(TableDataError::EmptyUpdate);
            }
            if let Some(column) = update
                .values
                .keys()
                .find(|column| !update.original_values.contains_key(*column))
            {
                return Err(TableDataError::MissingOriginalValue(column.clone()));
            }
        }
        for delete in &self.deletes {
            validate_identity(metadata, key, &delete.identity)?;
            validate_values(metadata, &delete.original_values, false, false)?;
        }
        Ok(())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inserts.is_empty() && self.updates.is_empty() && self.deletes.is_empty()
    }
}

fn validate_identity(
    metadata: &TableMetadata,
    key: &UniqueKey,
    identity: &RowValues,
) -> Result<(), TableDataError> {
    if identity.len() != key.columns.len()
        || key
            .columns
            .iter()
            .any(|column| !identity.contains_key(column))
    {
        return Err(TableDataError::InvalidRowIdentity);
    }
    validate_values(metadata, identity, false, false)?;
    if identity
        .values()
        .any(|value| matches!(value, CellValue::Null | CellValue::Default))
    {
        return Err(TableDataError::InvalidRowIdentity);
    }
    Ok(())
}

fn validate_values(
    metadata: &TableMetadata,
    values: &RowValues,
    allow_default: bool,
    require_writable: bool,
) -> Result<(), TableDataError> {
    for (name, value) in values {
        let column = metadata
            .column(name)
            .ok_or_else(|| TableDataError::UnknownColumn(name.clone()))?;
        if require_writable && !column.writable() {
            return Err(TableDataError::ReadOnlyColumn(name.clone()));
        }
        if !allow_default && matches!(value, CellValue::Default) {
            return Err(TableDataError::DefaultOutsideInsert);
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableStatementKind {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedStatement {
    pub kind: TableStatementKind,
    pub sql: String,
    pub parameters: Vec<StatementParameter>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatementParameter {
    pub database_type: String,
    pub value: CellValue,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableChangeSummary {
    pub inserted: u64,
    pub updated: u64,
    pub deleted: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableChangePlan {
    pub request_id: Uuid,
    pub table: TableRef,
    pub statements: Vec<PlannedStatement>,
    pub summary: TableChangeSummary,
    pub timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableApplyResult {
    pub request_id: Uuid,
    pub summary: TableChangeSummary,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum TableDataError {
    #[error("request table does not match the supplied metadata")]
    TableMismatch,
    #[error("page size must be between 1 and {maximum}")]
    InvalidPageSize { maximum: u32 },
    #[error("operation timeout must be greater than zero")]
    InvalidTimeout,
    #[error("unknown table column: {0}")]
    UnknownColumn(String),
    #[error("column is not writable: {0}")]
    ReadOnlyColumn(String),
    #[error("filter for {column} has the wrong number of values for {operator:?}")]
    InvalidFilterArity {
        column: String,
        operator: FilterOperator,
    },
    #[error("DEFAULT is only valid for inserted values")]
    DefaultOutsideInsert,
    #[error("raw {0} fragment cannot be empty")]
    EmptyRawFragment(&'static str),
    #[error("table has no stable non-null unique row identity")]
    NoStableRowIdentity,
    #[error("row identity does not match the table's stable key")]
    InvalidRowIdentity,
    #[error("an update must contain at least one changed value")]
    EmptyUpdate,
    #[error("an update is missing the original value for column: {0}")]
    MissingOriginalValue(String),
}
