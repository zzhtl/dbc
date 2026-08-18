//! Keyboard shortcuts.
//!
//! The application previously had none: executing a query, cancelling it and
//! refreshing the object tree all required travelling to a toolbar button,
//! which is the wrong cost for the three most repeated actions in a database
//! client.

use eframe::egui::{self, Key, KeyboardShortcut, Modifiers};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shortcut {
    Execute,
    Cancel,
    RefreshObjects,
    ApplyTableChanges,
    CommandPalette,
    NewTab,
    CloseTab,
    NextTab,
}

/// `COMMAND` maps to Ctrl on Windows and Linux, and to Cmd on macOS.
const BINDINGS: &[(Shortcut, Modifiers, Key)] = &[
    (Shortcut::Execute, Modifiers::COMMAND, Key::Enter),
    (Shortcut::ApplyTableChanges, Modifiers::COMMAND, Key::S),
    (Shortcut::RefreshObjects, Modifiers::NONE, Key::F5),
    (Shortcut::Cancel, Modifiers::NONE, Key::Escape),
    (Shortcut::CommandPalette, Modifiers::COMMAND, Key::K),
    (Shortcut::NewTab, Modifiers::COMMAND, Key::T),
    (Shortcut::CloseTab, Modifiers::COMMAND, Key::W),
    (Shortcut::NextTab, Modifiers::CTRL, Key::Tab),
];

/// Take at most one shortcut per frame.
///
/// `escape_available` is false while a modal is open, so `Esc` keeps closing
/// the modal instead of cancelling the query behind it.
pub fn consume(ctx: &egui::Context, escape_available: bool) -> Option<Shortcut> {
    for (shortcut, modifiers, key) in BINDINGS {
        if *key == Key::Escape && !escape_available {
            continue;
        }
        let binding = KeyboardShortcut::new(*modifiers, *key);
        if ctx.input_mut(|input| input.consume_shortcut(&binding)) {
            return Some(*shortcut);
        }
    }
    None
}

/// Text shown next to the matching button, so the shortcut is discoverable.
#[must_use]
pub const fn hint(shortcut: Shortcut) -> &'static str {
    match shortcut {
        Shortcut::Execute => "Ctrl+Enter",
        Shortcut::ApplyTableChanges => "Ctrl+S",
        Shortcut::RefreshObjects => "F5",
        Shortcut::Cancel => "Esc",
        Shortcut::CommandPalette => "Ctrl+K",
        Shortcut::NewTab => "Ctrl+T",
        Shortcut::CloseTab => "Ctrl+W",
        Shortcut::NextTab => "Ctrl+Tab",
    }
}

#[cfg(test)]
mod tests {
    use super::{BINDINGS, Shortcut, hint};

    #[test]
    fn every_shortcut_has_exactly_one_binding_and_one_hint() {
        for shortcut in [
            Shortcut::Execute,
            Shortcut::Cancel,
            Shortcut::RefreshObjects,
            Shortcut::ApplyTableChanges,
            Shortcut::CommandPalette,
            Shortcut::NewTab,
            Shortcut::CloseTab,
            Shortcut::NextTab,
        ] {
            let bound = BINDINGS
                .iter()
                .filter(|(candidate, _, _)| *candidate == shortcut)
                .count();
            assert_eq!(bound, 1, "{shortcut:?} must have exactly one binding");
            assert!(!hint(shortcut).is_empty());
        }
    }

    #[test]
    fn no_two_shortcuts_share_a_binding() {
        for (index, (_, modifiers, key)) in BINDINGS.iter().enumerate() {
            for (other_modifiers, other_key) in BINDINGS
                .iter()
                .skip(index + 1)
                .map(|(_, modifiers, key)| (modifiers, key))
            {
                assert!(
                    !(modifiers == other_modifiers && key == other_key),
                    "duplicate binding for {key:?}"
                );
            }
        }
    }
}
