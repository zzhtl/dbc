//! Saved connections, preferences and the credential vault.
//!
//! Settings are written on every change rather than at exit: the process
//! can be closed at any moment, and a few kilobytes of JSON is cheaper than
//! losing a connection the user just set up.

use super::*;

impl DbcApp {
    /// Persist settings, surfacing failures instead of losing them silently.
    pub(super) fn save_settings(&mut self) {
        if let Err(error) = self.store.save() {
            self.status = Status::error(format!("配置保存失败：{error}"));
        }
    }

    /// Mirror the live UI preferences into the settings before writing them.
    pub(super) fn capture_preferences(&mut self) {
        let preferences = &mut self.store.settings_mut().ui;
        preferences.page_size = self.query_settings.page_size;
        preferences.max_buffered_rows = self.query_settings.row_limit;
        preferences.max_buffered_bytes =
            self.query_settings.memory_limit_mib.saturating_mul(1024 * 1024);
        preferences.export_directory = Some(self.export_directory.clone());
    }

    fn driver_index_for(id: &str) -> Option<usize> {
        DRIVER_CHOICES.iter().position(|choice| choice.id == id)
    }

    /// Load a saved connection into the form. The password only comes along
    /// when the vault is unlocked.
    pub(super) fn apply_saved_connection(&mut self, index: usize) {
        let Some(saved) = self.store.settings().connections.get(index).cloned() else {
            return;
        };
        if !self.allow_pending_navigation("切换连接") {
            return;
        }
        if let Some(driver) = Self::driver_index_for(&saved.driver_id)
            && driver != self.selected_driver
        {
            self.close_current_session();
            self.reset_session_view();
            self.selected_driver = driver;
            self.tab_mut().query_text = DRIVER_CHOICES[driver].query.to_owned();
        }
        self.endpoint = saved.endpoint.clone();
        self.database = saved.database.clone();
        self.user = saved.user.clone();
        self.connection_name = saved.name.clone();
        self.save_password = saved.save_password;
        self.selected_saved = Some(index);
        self.password.clear();

        if saved.save_password {
            match self.vault.as_ref().and_then(|vault| vault.get(&saved.id)) {
                Some(secret) => {
                    self.password = secret.to_owned();
                    self.status = Status::info(format!("已载入连接 {}", saved.name));
                }
                None => {
                    self.status = Status::info(format!(
                        "已载入连接 {}；解锁凭据库后可自动填充密码",
                        saved.name
                    ));
                }
            }
        } else {
            self.status = Status::info(format!("已载入连接 {}", saved.name));
        }
    }

    /// Save the current form. An existing entry with the same name is updated
    /// so repeated saves do not pile up duplicates.
    pub(super) fn save_current_connection(&mut self) {
        let name = self.connection_name.trim().to_owned();
        if name.is_empty() {
            self.status = Status::error("请先填写连接名称");
            return;
        }
        if self.endpoint.trim().is_empty() {
            self.status = Status::error("连接地址不能为空");
            return;
        }
        if self.save_password && self.vault.is_none() {
            self.status = Status::error("保存密码需要先解锁凭据库");
            return;
        }
        let driver_id = self.choice().id.to_owned();
        let existing = self
            .store
            .settings()
            .connections
            .iter()
            .position(|saved| saved.name == name);
        let id = existing
            .and_then(|index| self.store.settings().connections.get(index))
            .map_or_else(|| Uuid::new_v4().to_string(), |saved| saved.id.clone());
        let entry = SavedConnection {
            id: id.clone(),
            name: name.clone(),
            driver_id,
            endpoint: self.endpoint.trim().to_owned(),
            database: self.database.trim().to_owned(),
            user: self.user.trim().to_owned(),
            group: String::new(),
            save_password: self.save_password,
        };

        if let Some(vault) = self.vault.as_mut() {
            if self.save_password && !self.password.is_empty() {
                vault.set(id.clone(), self.password.clone());
            } else {
                vault.remove(&id);
            }
            let path = self.store.vault_path();
            if let Err(error) = vault.save(&path) {
                self.status = Status::error(format!("凭据保存失败：{error}"));
                return;
            }
        }

        let connections = &mut self.store.settings_mut().connections;
        match existing {
            Some(index) => connections[index] = entry,
            None => connections.push(entry),
        }
        self.selected_saved = existing.or(Some(connections.len() - 1));
        self.capture_preferences();
        self.save_settings();
        if !self.status.is_error() {
            self.status = Status::success(format!("已保存连接 {name}"));
        }
    }

    pub(super) fn delete_saved_connection(&mut self, index: usize) {
        let Some(saved) = self.store.settings().connections.get(index).cloned() else {
            return;
        };
        if let Some(vault) = self.vault.as_mut() {
            vault.remove(&saved.id);
            let path = self.store.vault_path();
            let _ignored = vault.save(&path);
        }
        self.store.settings_mut().connections.remove(index);
        if self.selected_saved == Some(index) {
            self.selected_saved = None;
        }
        self.save_settings();
        if !self.status.is_error() {
            self.status = Status::info(format!("已删除连接 {}", saved.name));
        }
    }

    /// Open a throwaway connection to validate the form without disturbing the
    /// active session.
    pub(super) fn test_connection(&mut self, ctx: &egui::Context) {
        if self.testing_connection || self.connection_busy {
            return;
        }
        let choice = self.choice();
        let Some(factory) = self.registry.get(choice.id) else {
            self.status = Status::error(format!("驱动 {} 未注册", choice.id));
            return;
        };
        if self.endpoint.trim().is_empty() {
            self.status = Status::error("连接地址不能为空");
            return;
        }
        let password = optional_input(&self.password);
        let profile = ConnectionProfile {
            id: Uuid::new_v4().to_string(),
            driver_id: choice.id.to_owned(),
            display_name: choice.name.to_owned(),
            endpoint: self.endpoint.trim().to_owned(),
            database: optional_input(&self.database),
            user: optional_input(&self.user),
            secret_id: password.as_ref().map(|_| "desktop:test".to_owned()),
        };
        self.testing_connection = true;
        self.status = Status::progress("正在测试连接…");
        let event_sender = self.event_sender.clone();
        let repaint = ctx.clone();
        let _cancellation = self.runtime.spawn_reported(
            move |_| async move {
                let secret = password.map(SecretValue::new);
                factory.connect(&profile, secret.as_ref()).await
            },
            move |result| {
                let _sent = event_sender.send(AppEvent::ConnectionTested { result });
                repaint.request_repaint();
            },
        );
    }

    pub(super) fn open_vault_prompt(&mut self) {
        let purpose = if self.store.vault_exists() {
            VaultPurpose::Unlock
        } else {
            VaultPurpose::Create
        };
        self.vault_prompt = Some(VaultPrompt::new(purpose));
    }

    pub(super) fn submit_vault_password(&mut self, master: &str) {
        let purpose = self
            .vault_prompt
            .as_ref()
            .map_or(VaultPurpose::Create, VaultPrompt::purpose);
        let opened = match purpose {
            VaultPurpose::Create => Vault::create(master),
            VaultPurpose::Unlock => match self.store.read_vault() {
                Ok(Some(bytes)) => Vault::unlock(&bytes, master),
                Ok(None) => Vault::create(master),
                Err(error) => Err(error),
            },
        };
        match opened {
            Ok(vault) => {
                let entries = vault.len();
                self.vault = Some(vault);
                self.vault_prompt = None;
                self.status = Status::success(format!("凭据库已解锁 · {entries} 条"));
            }
            Err(error) => {
                if let Some(prompt) = self.vault_prompt.as_mut() {
                    prompt.set_error(error.to_string());
                }
            }
        }
    }
}
