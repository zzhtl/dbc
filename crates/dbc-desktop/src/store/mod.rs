//! Local configuration and credential storage.
//!
//! Settings live in a plain JSON file so they can be read, diffed, backed up
//! and hand-edited. Passwords never go in there: they are held in a separate
//! encrypted vault (see [`secrets`]) that the user unlocks with one master
//! password.

pub mod secrets;

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::atomic_file::write_atomic;
pub use secrets::VaultError;

const SETTINGS_FILE: &str = "settings.json";
const VAULT_FILE: &str = "secrets.bin";
/// Query history is capped so the settings file cannot grow without bound.
const MAX_HISTORY: usize = 200;

/// A saved connection. Deliberately holds no password.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedConnection {
    pub id: String,
    pub name: String,
    pub driver_id: String,
    pub endpoint: String,
    #[serde(default)]
    pub database: String,
    #[serde(default)]
    pub user: String,
    /// Optional grouping label shown in the sidebar.
    #[serde(default)]
    pub group: String,
    /// Whether a password for this connection is expected in the vault.
    #[serde(default)]
    pub save_password: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThemePreference {
    #[default]
    System,
    Light,
    Dark,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct UiPreferences {
    pub sidebar_width: f32,
    pub results_height: f32,
    pub page_size: usize,
    pub max_buffered_rows: usize,
    pub max_buffered_bytes: usize,
    pub theme: ThemePreference,
    pub export_directory: Option<PathBuf>,
}

impl Default for UiPreferences {
    fn default() -> Self {
        Self {
            sidebar_width: 286.0,
            results_height: 340.0,
            page_size: 200,
            max_buffered_rows: 10_000,
            max_buffered_bytes: 64 * 1024 * 1024,
            theme: ThemePreference::System,
            export_directory: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub connections: Vec<SavedConnection>,
    pub ui: UiPreferences,
    /// Most recent queries first.
    pub history: Vec<HistoryEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub driver_id: String,
    pub text: String,
}

/// Reads and writes the per-user configuration directory.
#[derive(Debug)]
pub struct Store {
    directory: PathBuf,
    settings: Settings,
    /// Set to false when the settings file could not be read, so a later save
    /// does not silently discard a file we failed to parse.
    loaded: bool,
}

impl Store {
    /// Open the configuration directory, falling back to defaults when the
    /// settings file is missing or unreadable.
    #[must_use]
    pub fn open() -> Self {
        Self::open_in(config_directory())
    }

    #[must_use]
    pub fn open_in(directory: PathBuf) -> Self {
        let path = directory.join(SETTINGS_FILE);
        let (settings, loaded) = match fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice(&bytes) {
                Ok(settings) => (settings, true),
                Err(_) => (Settings::default(), false),
            },
            // A missing file is the normal first-run case, not a failure.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                (Settings::default(), true)
            }
            Err(_) => (Settings::default(), false),
        };
        Self {
            directory,
            settings,
            loaded,
        }
    }

    #[must_use]
    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    pub fn settings_mut(&mut self) -> &mut Settings {
        &mut self.settings
    }

    #[must_use]
    pub fn vault_path(&self) -> PathBuf {
        self.directory.join(VAULT_FILE)
    }

    #[must_use]
    pub fn vault_exists(&self) -> bool {
        self.vault_path().is_file()
    }

    /// Read the vault file so the caller can unlock it.
    ///
    /// # Errors
    ///
    /// Returns [`VaultError::Io`] when the file exists but cannot be read.
    pub fn read_vault(&self) -> Result<Option<Vec<u8>>, VaultError> {
        match fs::read(self.vault_path()) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(VaultError::Io(error.to_string())),
        }
    }

    /// Record a query in the history, moving an existing copy to the front.
    pub fn remember_query(&mut self, driver_id: &str, text: &str) {
        let text = text.trim();
        if text.is_empty() {
            return;
        }
        let entry = HistoryEntry {
            driver_id: driver_id.to_owned(),
            text: text.to_owned(),
        };
        self.settings.history.retain(|existing| existing != &entry);
        self.settings.history.insert(0, entry);
        self.settings.history.truncate(MAX_HISTORY);
    }

    /// Persist the settings file.
    ///
    /// # Errors
    ///
    /// Returns a human-readable message when the file cannot be replaced, or
    /// when the previous settings file failed to parse — overwriting it then
    /// would discard connections the user can still recover by hand.
    pub fn save(&self) -> Result<(), String> {
        if !self.loaded {
            return Err(format!(
                "{} 无法解析，为避免覆盖请先手动修复或删除该文件",
                settings_path(&self.directory).display()
            ));
        }
        let encoded = serde_json::to_vec_pretty(&self.settings)
            .map_err(|error| error.to_string())?;
        write_atomic(&settings_path(&self.directory), &encoded, true)
            .map_err(|error| error.to_string())
    }
}

/// Per-user configuration directory, resolved without a path-helper crate.
fn config_directory() -> PathBuf {
    let base = if cfg!(target_os = "windows") {
        std::env::var_os("APPDATA").map(PathBuf::from)
    } else if cfg!(target_os = "macos") {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| home.join("Library").join("Application Support"))
    } else {
        std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .or_else(|| {
                std::env::var_os("HOME")
                    .map(PathBuf::from)
                    .map(|home| home.join(".config"))
            })
    };
    base.unwrap_or_else(|| PathBuf::from(".")).join("dbc")
}

/// Directory an export picker should open in.
#[must_use]
pub fn default_export_directory(preferences: &UiPreferences) -> PathBuf {
    preferences
        .export_directory
        .clone()
        .filter(|path| path.is_dir())
        .or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .filter(|home| home.is_dir())
        })
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."))
}

fn settings_path(directory: &Path) -> PathBuf {
    directory.join(SETTINGS_FILE)
}

#[cfg(test)]
mod tests {
    use super::{MAX_HISTORY, SavedConnection, Store};

    fn temp_store(name: &str) -> (std::path::PathBuf, Store) {
        let directory = std::env::temp_dir().join(name);
        let _ignored = std::fs::remove_dir_all(&directory);
        let store = Store::open_in(directory.clone());
        (directory, store)
    }

    #[test]
    fn settings_round_trip_through_the_json_file() {
        let (directory, mut store) = temp_store("dbc-store-roundtrip");
        store.settings_mut().connections.push(SavedConnection {
            id: "c1".to_owned(),
            name: "本地 PG".to_owned(),
            driver_id: "postgresql".to_owned(),
            endpoint: "postgres://127.0.0.1:5432".to_owned(),
            database: "app".to_owned(),
            user: "postgres".to_owned(),
            group: "开发".to_owned(),
            save_password: true,
        });
        store.save().expect("settings should be written");

        let reopened = Store::open_in(directory.clone());

        assert_eq!(reopened.settings().connections.len(), 1);
        assert_eq!(reopened.settings().connections[0].name, "本地 PG");
        let _ignored = std::fs::remove_dir_all(&directory);
    }

    #[test]
    fn the_settings_file_never_holds_a_password_field() {
        let (directory, mut store) = temp_store("dbc-store-no-password");
        store.settings_mut().connections.push(SavedConnection {
            id: "c1".to_owned(),
            name: "n".to_owned(),
            driver_id: "sqlite".to_owned(),
            endpoint: "sqlite::memory:".to_owned(),
            database: String::new(),
            user: String::new(),
            group: String::new(),
            save_password: false,
        });
        store.save().expect("settings should be written");

        let raw = std::fs::read_to_string(super::settings_path(&directory))
            .expect("settings file should exist");

        // `save_password` is a flag, not a secret; assert no key is literally
        // named `password`.
        assert!(
            !raw.contains("\"password\""),
            "settings must not carry a password field, got: {raw}"
        );
        let _ignored = std::fs::remove_dir_all(&directory);
    }

    #[test]
    fn history_deduplicates_and_stays_bounded() {
        let (directory, mut store) = temp_store("dbc-store-history");

        for index in 0..MAX_HISTORY + 20 {
            store.remember_query("sqlite", &format!("SELECT {index}"));
        }
        store.remember_query("sqlite", "SELECT 0");

        let history = &store.settings().history;
        assert_eq!(history.len(), MAX_HISTORY);
        assert_eq!(history[0].text, "SELECT 0");
        assert_eq!(
            history.iter().filter(|entry| entry.text == "SELECT 0").count(),
            1
        );
        let _ignored = std::fs::remove_dir_all(&directory);
    }

    #[test]
    fn an_unparsable_settings_file_is_never_overwritten() {
        let (directory, _) = temp_store("dbc-store-corrupt");
        std::fs::create_dir_all(&directory).expect("directory should be creatable");
        std::fs::write(super::settings_path(&directory), b"{ not json")
            .expect("seed file should be writable");

        let store = Store::open_in(directory.clone());

        assert!(store.save().is_err(), "a corrupt file must be preserved");
        assert_eq!(
            std::fs::read_to_string(super::settings_path(&directory))
                .expect("file should still exist"),
            "{ not json"
        );
        let _ignored = std::fs::remove_dir_all(&directory);
    }
}
