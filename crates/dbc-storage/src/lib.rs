//! Local metadata and operating-system secret storage.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::Mutex,
};

use dbc_core::{driver::ConnectionProfile, policy::SecurityPolicy};
use rusqlite::{Connection, OptionalExtension, params};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredConnection {
    pub profile: ConnectionProfile,
    pub policy: SecurityPolicy,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("metadata serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("metadata lock was poisoned")]
    LockPoisoned,
    #[error("keychain error: {0}")]
    Keychain(String),
}

/// SQLite-backed, non-secret application metadata.
pub struct MetadataStore {
    connection: Mutex<Connection>,
}

impl MetadataStore {
    /// Open the metadata database and apply embedded migrations.
    ///
    /// # Errors
    ///
    /// Returns database or migration errors.
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let connection = Connection::open(path)?;
        Self::from_connection(connection)
    }

    /// Open an isolated in-memory metadata database.
    ///
    /// # Errors
    ///
    /// Returns migration errors.
    pub fn open_in_memory() -> Result<Self, StorageError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, StorageError> {
        connection.execute_batch(
            "
            PRAGMA foreign_keys = ON;
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS connections (
                id TEXT PRIMARY KEY NOT NULL,
                display_name TEXT NOT NULL,
                profile_json TEXT NOT NULL,
                policy_json TEXT NOT NULL
            );
            PRAGMA user_version = 1;
            ",
        )?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    /// Insert or replace one connection profile.
    ///
    /// # Errors
    ///
    /// Returns serialization, lock, or SQLite errors.
    pub fn upsert_connection(&self, connection: &StoredConnection) -> Result<(), StorageError> {
        let profile_json = serde_json::to_string(&connection.profile)?;
        let policy_json = serde_json::to_string(&connection.policy)?;
        let database = self
            .connection
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?;
        database.execute(
            "
            INSERT INTO connections (id, display_name, profile_json, policy_json)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(id) DO UPDATE SET
                display_name = excluded.display_name,
                profile_json = excluded.profile_json,
                policy_json = excluded.policy_json
            ",
            params![
                connection.profile.id,
                connection.profile.display_name,
                profile_json,
                policy_json
            ],
        )?;
        Ok(())
    }

    /// Load all connection profiles in deterministic display order.
    ///
    /// # Errors
    ///
    /// Returns lock, SQLite, or deserialization errors.
    pub fn list_connections(&self) -> Result<Vec<StoredConnection>, StorageError> {
        let database = self
            .connection
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?;
        let mut statement = database.prepare(
            "
            SELECT profile_json, policy_json
            FROM connections
            ORDER BY display_name COLLATE NOCASE, id
            ",
        )?;
        let mut rows = statement.query([])?;
        let mut connections = Vec::new();
        while let Some(row) = rows.next()? {
            let profile_json: String = row.get(0)?;
            let policy_json: String = row.get(1)?;
            connections.push(StoredConnection {
                profile: serde_json::from_str(&profile_json)?,
                policy: serde_json::from_str(&policy_json)?,
            });
        }
        Ok(connections)
    }

    /// Delete a connection profile. Repeated deletion is harmless.
    ///
    /// # Errors
    ///
    /// Returns lock or SQLite errors.
    pub fn delete_connection(&self, id: &str) -> Result<bool, StorageError> {
        let database = self
            .connection
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?;
        Ok(database.execute("DELETE FROM connections WHERE id = ?1", [id])? > 0)
    }

    /// Return the persisted JSON for diagnostics and secret-leak tests.
    ///
    /// # Errors
    ///
    /// Returns lock or SQLite errors.
    pub fn raw_connection_payload(&self, id: &str) -> Result<Option<String>, StorageError> {
        let database = self
            .connection
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?;
        database
            .query_row(
                "
                SELECT profile_json || char(10) || policy_json
                FROM connections
                WHERE id = ?1
                ",
                [id],
                |row| row.get(0),
            )
            .optional()
            .map_err(StorageError::from)
    }
}

pub trait SecretStore: Send + Sync {
    /// Store or replace a secret.
    ///
    /// # Errors
    ///
    /// Returns a backend-specific storage error.
    fn set(&self, id: &str, secret: &str) -> Result<(), StorageError>;

    /// Retrieve a secret.
    ///
    /// # Errors
    ///
    /// Returns a backend-specific storage error.
    fn get(&self, id: &str) -> Result<Option<String>, StorageError>;

    /// Delete a secret and report whether it existed.
    ///
    /// # Errors
    ///
    /// Returns a backend-specific storage error.
    fn delete(&self, id: &str) -> Result<bool, StorageError>;
}

#[derive(Debug, Default)]
pub struct MemorySecretStore {
    secrets: Mutex<BTreeMap<String, String>>,
}

impl MemorySecretStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl SecretStore for MemorySecretStore {
    fn set(&self, id: &str, secret: &str) -> Result<(), StorageError> {
        self.secrets
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?
            .insert(id.to_owned(), secret.to_owned());
        Ok(())
    }

    fn get(&self, id: &str) -> Result<Option<String>, StorageError> {
        Ok(self
            .secrets
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?
            .get(id)
            .cloned())
    }

    fn delete(&self, id: &str) -> Result<bool, StorageError> {
        Ok(self
            .secrets
            .lock()
            .map_err(|_| StorageError::LockPoisoned)?
            .remove(id)
            .is_some())
    }
}

/// Secret storage backed by the platform keychain.
#[derive(Debug, Clone)]
pub struct SystemSecretStore {
    service: String,
}

impl SystemSecretStore {
    #[must_use]
    pub fn new(service: impl Into<String>) -> Self {
        Self {
            service: service.into(),
        }
    }

    fn entry(&self, id: &str) -> Result<keyring::Entry, StorageError> {
        keyring::Entry::new(&self.service, id)
            .map_err(|error| StorageError::Keychain(error.to_string()))
    }
}

impl SecretStore for SystemSecretStore {
    fn set(&self, id: &str, secret: &str) -> Result<(), StorageError> {
        self.entry(id)?
            .set_password(secret)
            .map_err(|error| StorageError::Keychain(error.to_string()))
    }

    fn get(&self, id: &str) -> Result<Option<String>, StorageError> {
        match self.entry(id)?.get_password() {
            Ok(secret) => Ok(Some(secret)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(error) => Err(StorageError::Keychain(error.to_string())),
        }
    }

    fn delete(&self, id: &str) -> Result<bool, StorageError> {
        match self.entry(id)?.delete_credential() {
            Ok(()) => Ok(true),
            Err(keyring::Error::NoEntry) => Ok(false),
            Err(error) => Err(StorageError::Keychain(error.to_string())),
        }
    }
}
