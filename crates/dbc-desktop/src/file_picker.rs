//! In-application file picker.
//!
//! The native dialog crate this replaces needs an `xdg-desktop-portal` service
//! on Linux; when that service is missing the dialog silently returns nothing
//! and the export looks cancelled. Drawing the picker with egui keeps file
//! selection working on headless, container and minimal desktop setups, and it
//! reports its own errors instead of failing invisibly.

use std::{
    fs,
    path::{Path, PathBuf},
};

use eframe::egui::{self, Key, Modal, RichText, ScrollArea, TextEdit};

/// What the caller should do after one frame of the picker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PickerOutcome {
    /// Still open.
    Pending,
    Cancelled,
    Selected(PathBuf),
}

#[derive(Debug, Clone)]
struct Entry {
    name: String,
    path: PathBuf,
    is_directory: bool,
}

/// A save-file picker rendered as an egui modal.
#[derive(Debug)]
pub struct FilePicker {
    title: String,
    directory: PathBuf,
    /// Editable location bar; accepts a pasted absolute path.
    location: String,
    file_name: String,
    extension: String,
    entries: Vec<Entry>,
    error: Option<String>,
    show_hidden: bool,
    /// Set once the chosen target already exists, so overwriting takes a
    /// deliberate second confirmation.
    pending_overwrite: Option<PathBuf>,
}

impl FilePicker {
    /// Open a save picker for `file_name` under `directory`.
    #[must_use]
    pub fn save(
        title: impl Into<String>,
        directory: PathBuf,
        file_name: impl Into<String>,
        extension: impl Into<String>,
    ) -> Self {
        let directory = usable_directory(directory);
        let mut picker = Self {
            title: title.into(),
            location: directory.display().to_string(),
            directory,
            file_name: file_name.into(),
            extension: extension.into(),
            entries: Vec::new(),
            error: None,
            show_hidden: false,
            pending_overwrite: None,
        };
        picker.reload();
        picker
    }

    /// Directory the picker currently shows, so the caller can restore it next time.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    fn reload(&mut self) {
        self.entries.clear();
        self.error = None;
        let read = match fs::read_dir(&self.directory) {
            Ok(read) => read,
            Err(error) => {
                self.error = Some(format!("无法读取目录：{error}"));
                return;
            }
        };
        for entry in read.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !self.show_hidden && name.starts_with('.') {
                continue;
            }
            let is_directory = entry.file_type().is_ok_and(|kind| kind.is_dir());
            if !is_directory && !self.matches_filter(name) {
                continue;
            }
            self.entries.push(Entry {
                name: name.to_owned(),
                path,
                is_directory,
            });
        }
        // Directories first, then a case-insensitive name order.
        self.entries.sort_by(|left, right| {
            right
                .is_directory
                .cmp(&left.is_directory)
                .then_with(|| left.name.to_lowercase().cmp(&right.name.to_lowercase()))
        });
    }

    fn matches_filter(&self, name: &str) -> bool {
        if self.extension.is_empty() {
            return true;
        }
        Path::new(name)
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case(&self.extension))
    }

    fn navigate_to(&mut self, directory: PathBuf) {
        self.directory = directory;
        self.location = self.directory.display().to_string();
        self.pending_overwrite = None;
        self.reload();
    }

    /// Resolve the location bar, which may name either a directory or a full
    /// target path.
    fn apply_location(&mut self) {
        let candidate = PathBuf::from(self.location.trim());
        if candidate.is_dir() {
            self.navigate_to(candidate);
        } else if let Some(parent) = candidate.parent().filter(|parent| parent.is_dir()) {
            if let Some(name) = candidate.file_name().and_then(|name| name.to_str()) {
                self.file_name = name.to_owned();
            }
            self.navigate_to(parent.to_path_buf());
        } else {
            self.error = Some("路径不存在".to_owned());
        }
    }

    /// Full target path, with the format extension appended when missing.
    fn target(&self) -> PathBuf {
        let name = self.file_name.trim();
        // Leave the name alone when there is no filter, when it is empty, or
        // when the user already typed the right extension.
        let already_suffixed = Path::new(name)
            .extension()
            .is_some_and(|existing| existing.eq_ignore_ascii_case(&self.extension));
        let name = if self.extension.is_empty() || name.is_empty() || already_suffixed {
            name.to_owned()
        } else {
            format!("{name}.{}", self.extension)
        };
        self.directory.join(name)
    }

    pub fn show(&mut self, ctx: &egui::Context) -> PickerOutcome {
        let modal = Modal::new(egui::Id::new("dbc-file-picker")).show(ctx, |ui| {
            ui.set_width(560.0);
            ui.heading(&self.title);
            ui.add_space(6.0);

            if let Some(target) = self.pending_overwrite.clone() {
                return self.show_overwrite_confirmation(ui, &target);
            }

            self.show_location_bar(ui);
            ui.add_space(4.0);
            self.show_listing(ui);
            ui.add_space(6.0);
            self.show_footer(ui)
        });

        // Read `should_close` before taking `inner`, which moves out of `modal`.
        let dismissed = modal.should_close();
        match modal.inner {
            PickerOutcome::Pending if dismissed => PickerOutcome::Cancelled,
            outcome => outcome,
        }
    }

    fn show_overwrite_confirmation(
        &mut self,
        ui: &mut egui::Ui,
        target: &Path,
    ) -> PickerOutcome {
        ui.label(RichText::new("目标文件已存在").strong());
        ui.label(RichText::new(target.display().to_string()).monospace());
        ui.add_space(8.0);
        let mut outcome = PickerOutcome::Pending;
        ui.horizontal(|ui| {
            if ui.button("取消").clicked() {
                self.pending_overwrite = None;
            }
            if ui.button("覆盖").clicked() {
                outcome = PickerOutcome::Selected(target.to_path_buf());
            }
        });
        outcome
    }

    fn show_location_bar(&mut self, ui: &mut egui::Ui) {
        let mut go_up = false;
        let mut apply = false;
        ui.horizontal(|ui| {
            if ui
                .add_enabled(self.directory.parent().is_some(), egui::Button::new("↑ 上级"))
                .clicked()
            {
                go_up = true;
            }
            let response = ui.add(
                TextEdit::singleline(&mut self.location)
                    .desired_width(ui.available_width() - 70.0)
                    .hint_text("目录，或粘贴完整路径"),
            );
            if response.lost_focus() && ui.input(|input| input.key_pressed(Key::Enter)) {
                apply = true;
            }
            if ui.button("转到").clicked() {
                apply = true;
            }
        });
        if go_up
            && let Some(parent) = self.directory.parent().map(Path::to_path_buf)
        {
            self.navigate_to(parent);
        }
        if apply {
            self.apply_location();
        }
    }

    fn show_listing(&mut self, ui: &mut egui::Ui) {
        let mut enter_directory = None;
        let mut choose_file = None;

        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.set_height(260.0);
            ScrollArea::vertical()
                .id_salt("dbc-file-picker-listing")
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    if let Some(error) = self.error.as_ref() {
                        ui.colored_label(ui.visuals().error_fg_color, error);
                        return;
                    }
                    if self.entries.is_empty() {
                        ui.weak("这个目录里没有可选文件");
                        return;
                    }
                    for entry in &self.entries {
                        let label = if entry.is_directory {
                            format!("📁 {}", entry.name)
                        } else {
                            format!("📄 {}", entry.name)
                        };
                        let selected =
                            !entry.is_directory && entry.name == self.file_name;
                        let response = ui.selectable_label(selected, label);
                        if entry.is_directory {
                            if response.double_clicked() {
                                enter_directory = Some(entry.path.clone());
                            }
                        } else if response.clicked() {
                            choose_file = Some(entry.name.clone());
                        }
                    }
                });
        });

        if let Some(directory) = enter_directory {
            self.navigate_to(directory);
        }
        if let Some(name) = choose_file {
            self.file_name = name;
        }
    }

    fn show_footer(&mut self, ui: &mut egui::Ui) -> PickerOutcome {
        let mut outcome = PickerOutcome::Pending;
        let mut confirm = false;
        let hidden_before = self.show_hidden;

        ui.horizontal(|ui| {
            ui.label("文件名");
            let response = ui.add(
                TextEdit::singleline(&mut self.file_name)
                    .desired_width(ui.available_width() - 180.0),
            );
            if response.lost_focus() && ui.input(|input| input.key_pressed(Key::Enter)) {
                confirm = true;
            }
            ui.checkbox(&mut self.show_hidden, "显示隐藏项");
        });
        ui.add_space(6.0);
        ui.horizontal(|ui| {
            if ui.button("取消").clicked() {
                outcome = PickerOutcome::Cancelled;
            }
            if ui
                .add_enabled(
                    !self.file_name.trim().is_empty(),
                    egui::Button::new("保存"),
                )
                .clicked()
            {
                confirm = true;
            }
        });

        if self.show_hidden != hidden_before {
            self.reload();
        }

        if confirm && !self.file_name.trim().is_empty() {
            let target = self.target();
            if target.exists() {
                self.pending_overwrite = Some(target);
            } else {
                outcome = PickerOutcome::Selected(target);
            }
        }
        outcome
    }
}

/// Fall back through the home directory to the filesystem root so the picker
/// always opens somewhere readable.
fn usable_directory(directory: PathBuf) -> PathBuf {
    if directory.is_dir() {
        return directory;
    }
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from)
        && home.is_dir()
    {
        return home;
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::FilePicker;

    fn picker(file_name: &str, extension: &str) -> FilePicker {
        FilePicker::save(
            "导出",
            std::env::temp_dir(),
            file_name,
            extension,
        )
    }

    #[test]
    fn target_appends_the_missing_extension() {
        let picker = picker("result", "csv");

        assert_eq!(
            picker.target().file_name().and_then(|name| name.to_str()),
            Some("result.csv")
        );
    }

    #[test]
    fn target_keeps_an_extension_the_user_already_typed() {
        let picker = picker("result.CSV", "csv");

        assert_eq!(
            picker.target().file_name().and_then(|name| name.to_str()),
            Some("result.CSV")
        );
    }

    #[test]
    fn a_missing_directory_falls_back_to_a_readable_one() {
        let picker = FilePicker::save(
            "导出",
            PathBuf::from("/definitely/not/a/real/directory"),
            "result",
            "csv",
        );

        assert!(picker.directory().is_dir());
    }
}
