//! Command palette.
//!
//! One keyboard entry point for everything the application can do, so the
//! toolbar only has to carry the few actions that are worth a permanent button.
//! The palette owns matching and selection; the caller owns the commands.

use eframe::egui::{self, Key, Modal, RichText, ScrollArea, TextEdit};

/// One row offered by the palette.
pub struct PaletteEntry {
    pub label: String,
    /// Right-aligned secondary text: a shortcut, a category, or a value.
    pub hint: String,
}

impl PaletteEntry {
    #[must_use]
    pub fn new(label: impl Into<String>, hint: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            hint: hint.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaletteOutcome {
    Pending,
    Cancelled,
    /// Index into the entry slice that was passed in.
    Chosen(usize),
}

#[derive(Debug, Default)]
pub struct Palette {
    query: String,
    /// Index into the filtered list, not into the entry slice.
    highlighted: usize,
}

/// How many rows the list shows before scrolling.
const VISIBLE_ROWS: usize = 12;

impl Palette {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn show(&mut self, ctx: &egui::Context, entries: &[PaletteEntry]) -> PaletteOutcome {
        let matches = filter(&self.query, entries);
        self.highlighted = self.highlighted.min(matches.len().saturating_sub(1));

        let (up, down, accept) = ctx.input_mut(|input| {
            (
                input.consume_key(egui::Modifiers::NONE, Key::ArrowUp),
                input.consume_key(egui::Modifiers::NONE, Key::ArrowDown),
                input.consume_key(egui::Modifiers::NONE, Key::Enter),
            )
        });
        if down && !matches.is_empty() {
            self.highlighted = (self.highlighted + 1) % matches.len();
        }
        if up && !matches.is_empty() {
            self.highlighted = self
                .highlighted
                .checked_sub(1)
                .unwrap_or(matches.len() - 1);
        }

        let mut clicked = None;
        let modal = Modal::new(egui::Id::new("dbc-command-palette")).show(ctx, |ui| {
            ui.set_width(560.0);
            let response = ui.add(
                TextEdit::singleline(&mut self.query)
                    .hint_text("输入命令、表名或历史查询")
                    .desired_width(f32::INFINITY),
            );
            // Typing should always land in the box, never in the editor behind.
            response.request_focus();
            ui.add_space(6.0);

            if matches.is_empty() {
                ui.weak("没有匹配的命令");
                return;
            }
            ScrollArea::vertical()
                .id_salt("dbc-command-palette-list")
                .max_height(VISIBLE_ROWS as f32 * 24.0)
                .show(ui, |ui| {
                    for (position, &index) in matches.iter().enumerate() {
                        let entry = &entries[index];
                        let selected = position == self.highlighted;
                        let row = ui.selectable_label(
                            selected,
                            format!("{:<48}", entry.label),
                        );
                        if !entry.hint.is_empty() {
                            ui.horizontal(|ui| {
                                ui.add_space(12.0);
                                ui.label(RichText::new(&entry.hint).weak().small());
                            });
                        }
                        if selected {
                            row.scroll_to_me(None);
                        }
                        if row.clicked() {
                            clicked = Some(index);
                        }
                    }
                });
        });

        let dismissed = modal.should_close();
        if let Some(index) = clicked {
            return PaletteOutcome::Chosen(index);
        }
        if accept {
            return matches
                .get(self.highlighted)
                .map_or(PaletteOutcome::Pending, |&index| {
                    PaletteOutcome::Chosen(index)
                });
        }
        if dismissed {
            return PaletteOutcome::Cancelled;
        }
        PaletteOutcome::Pending
    }
}

/// Rank entries against `query`, best first.
///
/// Matching is a case-insensitive subsequence so `slq` finds `慢查询 SLOWLOG`
/// without the user having to remember exact wording.
fn filter(query: &str, entries: &[PaletteEntry]) -> Vec<usize> {
    if query.trim().is_empty() {
        return (0..entries.len()).collect();
    }
    let needle = query.to_lowercase();
    let mut scored = entries
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| {
            let haystack = format!("{} {}", entry.label, entry.hint).to_lowercase();
            score(&needle, &haystack).map(|score| (score, index))
        })
        .collect::<Vec<_>>();
    // Higher score first; ties keep the caller's order so the list stays stable.
    scored.sort_by(|left, right| right.0.cmp(&left.0).then(left.1.cmp(&right.1)));
    scored.into_iter().map(|(_, index)| index).collect()
}

/// Subsequence score, or `None` when `needle` does not appear in order.
fn score(needle: &str, haystack: &str) -> Option<i32> {
    let mut total = 0;
    let mut consecutive = 0;
    let mut haystack_chars = haystack.char_indices().peekable();

    for wanted in needle.chars() {
        if wanted.is_whitespace() {
            continue;
        }
        loop {
            let (offset, candidate) = haystack_chars.next()?;
            if candidate == wanted {
                consecutive += 1;
                // Reward runs, and matches at the very start of the label.
                total += 1 + consecutive;
                if offset == 0 {
                    total += 4;
                }
                break;
            }
            consecutive = 0;
        }
    }
    Some(total)
}

#[cfg(test)]
mod tests {
    use super::{PaletteEntry, filter, score};

    fn entries() -> Vec<PaletteEntry> {
        vec![
            PaletteEntry::new("执行查询", "Ctrl+Enter"),
            PaletteEntry::new("新建标签页", "Ctrl+T"),
            PaletteEntry::new("刷新对象树", "F5"),
            PaletteEntry::new("打开表 items", "表"),
        ]
    }

    #[test]
    fn an_empty_query_keeps_every_entry_in_order() {
        assert_eq!(filter("  ", &entries()), vec![0, 1, 2, 3]);
    }

    #[test]
    fn matching_is_a_case_insensitive_subsequence() {
        let entries = entries();

        // `Ctrl+Enter` also contains the letters of `ctrl+t`, so a subsequence
        // match is expected to return both — ranking is what has to be right.
        assert_eq!(filter("ctrl+t", &entries)[0], 1);
        assert_eq!(filter("CTRL+T", &entries)[0], 1);
        assert_eq!(filter("ctrl+t", &entries), filter("CTRL+T", &entries));
        // Not a subsequence of anything.
        assert!(filter("zzz", &entries).is_empty());
    }

    #[test]
    fn a_prefix_match_outranks_a_scattered_one() {
        let entries = vec![
            PaletteEntry::new("scattered s q l here", ""),
            PaletteEntry::new("sql 编辑器", ""),
        ];

        assert_eq!(filter("sql", &entries)[0], 1);
    }

    #[test]
    fn consecutive_characters_score_higher_than_gaps() {
        let tight = score("abc", "abc").expect("exact match");
        let loose = score("abc", "axbxc").expect("subsequence match");

        assert!(tight > loose);
    }

    #[test]
    fn chinese_labels_match_by_substring() {
        let entries = entries();

        assert_eq!(filter("刷新", &entries), vec![2]);
        assert_eq!(filter("items", &entries), vec![3]);
    }
}
