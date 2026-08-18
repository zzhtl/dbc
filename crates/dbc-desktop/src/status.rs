//! Status-bar messages.
//!
//! Every outcome used to be a plain `String` rendered in one weak-grey line, so
//! a PostgreSQL error with its detail and hint was truncated by the layout,
//! could not be copied, and vanished as soon as the next message arrived.
//! Classifying the message lets errors stand out, keeps the full text
//! reachable, and drives the progress indicator.

use eframe::egui::{self, RichText};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusKind {
    Info,
    Progress,
    Success,
    Error,
}

/// One status-bar message plus the full text behind it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    kind: StatusKind,
    /// Single line shown in the bar.
    summary: String,
    /// Full text, present when it carries more than the summary.
    detail: Option<String>,
}

/// Summaries longer than this get a detail view instead of being clipped.
const MAX_SUMMARY_CHARS: usize = 110;

impl Status {
    #[must_use]
    pub fn info(message: impl Into<String>) -> Self {
        Self::new(StatusKind::Info, message)
    }

    #[must_use]
    pub fn progress(message: impl Into<String>) -> Self {
        Self::new(StatusKind::Progress, message)
    }

    #[must_use]
    pub fn success(message: impl Into<String>) -> Self {
        Self::new(StatusKind::Success, message)
    }

    /// Build an error status, keeping the untruncated text as the detail.
    #[must_use]
    pub fn error(message: impl Into<String>) -> Self {
        Self::new(StatusKind::Error, message)
    }

    fn new(kind: StatusKind, message: impl Into<String>) -> Self {
        let message = message.into();
        let first_line = message.lines().next().unwrap_or_default();
        let summary = truncate_chars(first_line, MAX_SUMMARY_CHARS);
        // Keep the full text whenever the bar cannot show all of it.
        let detail = (summary != message).then(|| message.clone());
        Self {
            kind,
            summary,
            detail,
        }
    }

    #[must_use]
    pub fn is_error(&self) -> bool {
        self.kind == StatusKind::Error
    }

    #[must_use]
    pub fn kind_is_not_error(&self) -> bool {
        !self.is_error()
    }

    /// Full text for the detail view and for copying.
    #[must_use]
    pub fn full_text(&self) -> &str {
        self.detail.as_deref().unwrap_or(&self.summary)
    }

    #[must_use]
    pub fn has_detail(&self) -> bool {
        self.detail.is_some()
    }

    /// Render the status bar. Returns `true` when the user asked for details.
    pub fn show(&self, ui: &mut egui::Ui) -> bool {
        let mut show_detail = false;
        ui.horizontal(|ui| {
            if self.kind == StatusKind::Progress {
                ui.add(egui::Spinner::new().size(12.0));
            }
            let text = RichText::new(&self.summary);
            let text = match self.kind {
                StatusKind::Error => text.color(ui.visuals().error_fg_color).strong(),
                StatusKind::Success => text.color(success_color(ui)),
                StatusKind::Progress => text,
                StatusKind::Info => text.weak(),
            };
            ui.label(text);

            if self.has_detail() && ui.small_button("详情").clicked() {
                show_detail = true;
            }
            if self.kind == StatusKind::Error && ui.small_button("复制").clicked() {
                ui.ctx().copy_text(self.full_text().to_owned());
            }
        });
        show_detail
    }
}

impl Default for Status {
    fn default() -> Self {
        Self::info(String::new())
    }
}

/// Green that stays legible against both the light and the dark theme.
fn success_color(ui: &egui::Ui) -> egui::Color32 {
    if ui.visuals().dark_mode {
        egui::Color32::from_rgb(0x6d, 0xd4, 0x00)
    } else {
        egui::Color32::from_rgb(0x1a, 0x7f, 0x37)
    }
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut characters = value.chars();
    let truncated: String = characters.by_ref().take(max_chars).collect();
    if characters.next().is_some() {
        format!("{truncated}…")
    } else {
        truncated
    }
}

#[cfg(test)]
mod tests {
    use super::{Status, StatusKind};

    #[test]
    fn a_short_single_line_message_needs_no_detail_view() {
        let status = Status::info("已连接");

        assert_eq!(status.summary, "已连接");
        assert!(!status.has_detail());
        assert_eq!(status.full_text(), "已连接");
    }

    #[test]
    fn a_multi_line_error_keeps_every_line_reachable() {
        let status = Status::error("执行失败：syntax error\nDETAIL: near \"slect\"\nHINT: 检查拼写");

        assert_eq!(status.kind, StatusKind::Error);
        assert_eq!(status.summary, "执行失败：syntax error");
        assert!(status.has_detail());
        assert!(status.full_text().contains("HINT: 检查拼写"));
    }

    #[test]
    fn a_long_message_is_summarised_without_splitting_a_character() {
        let status = Status::error("错".repeat(400));

        assert!(status.summary.ends_with('…'));
        assert!(status.summary.chars().count() <= 111);
        assert_eq!(status.full_text().chars().count(), 400);
    }
}
