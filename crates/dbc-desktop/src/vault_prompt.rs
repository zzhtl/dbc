//! Master-password prompt for the credential vault.

use eframe::egui::{self, Key, Modal, RichText, TextEdit};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VaultPurpose {
    /// No vault file exists yet; the user is choosing a master password.
    Create,
    /// A vault file exists and must be decrypted.
    Unlock,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromptOutcome {
    Pending,
    Cancelled,
    Submitted(String),
}

#[derive(Debug)]
pub struct VaultPrompt {
    purpose: VaultPurpose,
    password: String,
    confirmation: String,
    error: Option<String>,
}

impl Drop for VaultPrompt {
    fn drop(&mut self) {
        use zeroize::Zeroize as _;

        self.password.zeroize();
        self.confirmation.zeroize();
    }
}

impl VaultPrompt {
    #[must_use]
    pub fn new(purpose: VaultPurpose) -> Self {
        Self {
            purpose,
            password: String::new(),
            confirmation: String::new(),
            error: None,
        }
    }

    #[must_use]
    pub fn purpose(&self) -> VaultPurpose {
        self.purpose
    }

    /// Report a failed attempt without closing the prompt.
    pub fn set_error(&mut self, message: impl Into<String>) {
        self.error = Some(message.into());
        self.password.clear();
        self.confirmation.clear();
    }

    /// Whether the entered values can be submitted.
    fn is_submittable(&self) -> bool {
        if self.password.is_empty() {
            return false;
        }
        self.purpose == VaultPurpose::Unlock || self.password == self.confirmation
    }

    pub fn show(&mut self, ctx: &egui::Context) -> PromptOutcome {
        let modal = Modal::new(egui::Id::new("dbc-vault-prompt")).show(ctx, |ui| {
            ui.set_width(420.0);
            ui.heading(match self.purpose {
                VaultPurpose::Create => "设置主密码",
                VaultPurpose::Unlock => "解锁凭据库",
            });
            ui.add_space(4.0);
            ui.label(
                RichText::new(match self.purpose {
                    VaultPurpose::Create => {
                        "主密码用于加密本地保存的数据库密码。忘记后无法找回，只能删除凭据库重来。"
                    }
                    VaultPurpose::Unlock => "输入主密码以读取已保存的数据库密码。",
                })
                .weak(),
            );
            ui.add_space(8.0);

            let mut submit = false;
            let response = ui.add(
                TextEdit::singleline(&mut self.password)
                    .password(true)
                    .hint_text("主密码")
                    .desired_width(f32::INFINITY),
            );
            if self.purpose == VaultPurpose::Create {
                ui.add_space(4.0);
                ui.add(
                    TextEdit::singleline(&mut self.confirmation)
                        .password(true)
                        .hint_text("再次输入")
                        .desired_width(f32::INFINITY),
                );
                if !self.confirmation.is_empty() && self.password != self.confirmation {
                    ui.colored_label(ui.visuals().error_fg_color, "两次输入不一致");
                }
            } else if response.lost_focus() && ui.input(|input| input.key_pressed(Key::Enter)) {
                submit = true;
            }
            if let Some(error) = self.error.as_ref() {
                ui.add_space(4.0);
                ui.colored_label(ui.visuals().error_fg_color, error);
            }

            ui.add_space(8.0);
            let mut cancel = false;
            ui.horizontal(|ui| {
                if ui.button("取消").clicked() {
                    cancel = true;
                }
                if ui
                    .add_enabled(self.is_submittable(), egui::Button::new("确定"))
                    .clicked()
                {
                    submit = true;
                }
            });

            if cancel {
                PromptOutcome::Cancelled
            } else if submit && self.is_submittable() {
                PromptOutcome::Submitted(self.password.clone())
            } else {
                PromptOutcome::Pending
            }
        });

        // Read `should_close` before taking `inner`, which moves out of `modal`.
        let dismissed = modal.should_close();
        match modal.inner {
            PromptOutcome::Pending if dismissed => PromptOutcome::Cancelled,
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{VaultPrompt, VaultPurpose};

    #[test]
    fn creating_a_vault_requires_matching_confirmations() {
        let mut prompt = VaultPrompt::new(VaultPurpose::Create);
        prompt.password = "secret".to_owned();
        assert!(!prompt.is_submittable(), "confirmation is still empty");

        prompt.confirmation = "typo".to_owned();
        assert!(!prompt.is_submittable());

        prompt.confirmation = "secret".to_owned();
        assert!(prompt.is_submittable());
    }

    #[test]
    fn unlocking_needs_only_a_non_empty_password() {
        let mut prompt = VaultPrompt::new(VaultPurpose::Unlock);
        assert!(!prompt.is_submittable());

        prompt.password = "secret".to_owned();
        assert!(prompt.is_submittable());
    }

    #[test]
    fn reporting_an_error_clears_the_entered_password() {
        let mut prompt = VaultPrompt::new(VaultPurpose::Unlock);
        prompt.password = "wrong".to_owned();

        prompt.set_error("主密码不正确");

        assert!(prompt.password.is_empty());
        assert!(!prompt.is_submittable());
    }
}
