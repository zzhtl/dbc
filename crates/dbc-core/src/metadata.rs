use std::{collections::BTreeMap, time::Duration};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const MAX_OBJECT_PAGE_SIZE: usize = 1_000;
const MAX_CURSOR_LENGTH: usize = 256;

/// A database-agnostic object kind used by the lazy navigation tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseObjectKind {
    Schema,
    Table,
    PartitionedTable,
    View,
    MaterializedView,
    ForeignTable,
    Sequence,
    Column,
    Index,
    Constraint,
    Trigger,
    Routine,
    Collection,
    Keyspace,
    Key,
    Other(String),
}

/// Human-readable path segments identifying a node in an object tree.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectPath {
    pub segments: Vec<String>,
}

impl ObjectPath {
    #[must_use]
    pub fn new<I, S>(segments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            segments: segments.into_iter().map(Into::into).collect(),
        }
    }

    #[must_use]
    pub fn child(&self, segment: impl Into<String>) -> Self {
        let mut segments = self.segments.clone();
        segments.push(segment.into());
        Self { segments }
    }
}

/// A single node returned by a database driver's object discovery operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseObject {
    pub id: String,
    pub name: String,
    pub path: ObjectPath,
    pub kind: DatabaseObjectKind,
    pub has_children: bool,
    pub properties: BTreeMap<String, serde_json::Value>,
}

/// A bounded lazy-loading request for one level of a database object tree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectListRequest {
    pub id: Uuid,
    pub parent: Option<ObjectPath>,
    pub include_system: bool,
    pub limit: usize,
    pub cursor: Option<String>,
    pub timeout: Duration,
}

impl ObjectListRequest {
    /// Validate resource bounds before the request crosses a driver boundary.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero or oversized limit, a zero timeout, or an
    /// unreasonably large opaque cursor.
    pub fn validate(&self) -> Result<(), ObjectListValidationError> {
        if !(1..=MAX_OBJECT_PAGE_SIZE).contains(&self.limit) {
            return Err(ObjectListValidationError::InvalidLimit {
                maximum: MAX_OBJECT_PAGE_SIZE,
            });
        }
        if self.timeout.is_zero() {
            return Err(ObjectListValidationError::ZeroTimeout);
        }
        if self
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.len() > MAX_CURSOR_LENGTH)
        {
            return Err(ObjectListValidationError::CursorTooLong);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectPage {
    pub items: Vec<DatabaseObject>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum ObjectListValidationError {
    #[error("object page limit must be between 1 and {maximum}")]
    InvalidLimit { maximum: usize },
    #[error("object discovery timeout must be greater than zero")]
    ZeroTimeout,
    #[error("object discovery cursor is too long")]
    CursorTooLong,
}
