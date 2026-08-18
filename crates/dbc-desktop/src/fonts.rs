//! System font discovery.
//!
//! egui ships Latin-only default fonts, so a Chinese interface renders as tofu
//! boxes unless a CJK face is loaded from the host. Walking the platform font
//! directories with `std::fs` keeps this self-contained: the font crate this
//! replaces pulled an HTTP client into a local database tool.

use std::{
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
};

use eframe::egui::{FontData, FontDefinitions, FontFamily};

/// Font file stems known to carry CJK coverage, most preferred first.
///
/// Matching on the file name avoids parsing every face on the system just to
/// ask whether it has Han glyphs. Anything unrecognised is ignored rather than
/// guessed at, so a wrong pick cannot silently replace the UI font.
const PREFERRED_STEMS: &[&str] = &[
    // Pan-CJK families, shipped by most Linux distributions.
    "notosanscjk",
    "notoserifcjk",
    "sourcehansans",
    "sourcehanserif",
    "notosanssc",
    "notosanstc",
    // macOS.
    "pingfang",
    "hiraginosans",
    "stheiti",
    "songti",
    // Windows.
    "msyh",
    "msyhbd",
    "simhei",
    "simsun",
    "msjh",
    "malgun",
    // Older Linux fallbacks.
    "wenquanyi",
    "wqy",
    "droidsansfallback",
    "uming",
    "ukai",
];

/// Directories are walked no deeper than this, and no more files than
/// [`MAX_SCANNED_FILES`] are inspected, so a pathological font tree cannot
/// stall start-up.
const MAX_DEPTH: usize = 6;
const MAX_SCANNED_FILES: usize = 20_000;

/// Install a CJK-capable system font, returning the file that was used.
///
/// Returns `None` when no recognised font is present; the caller surfaces that
/// so the user gets an actionable hint instead of unreadable glyphs.
pub fn install_cjk_font(ctx: &eframe::egui::Context) -> Option<PathBuf> {
    let candidate = find_cjk_font()?;
    let bytes = fs::read(&candidate).ok()?;

    let mut definitions = FontDefinitions::default();
    definitions.font_data.insert(
        FONT_NAME.to_owned(),
        // Collections (`.ttc`) expose several faces; face 0 of every family in
        // `PREFERRED_STEMS` carries Han coverage.
        std::sync::Arc::new(FontData::from_owned(bytes)),
    );
    for family in [FontFamily::Proportional, FontFamily::Monospace] {
        definitions
            .families
            .entry(family)
            .or_default()
            .push(FONT_NAME.to_owned());
    }
    ctx.set_fonts(definitions);
    Some(candidate)
}

const FONT_NAME: &str = "dbc-system-cjk";

fn find_cjk_font() -> Option<PathBuf> {
    let mut best: Option<(usize, PathBuf)> = None;
    let mut scanned = 0_usize;

    for root in font_directories() {
        collect(&root, 0, &mut scanned, &mut best);
        // Rank 0 is the most preferred family; nothing later can beat it.
        if matches!(best, Some((0, _))) {
            break;
        }
    }
    best.map(|(_, path)| path)
}

fn collect(
    directory: &Path,
    depth: usize,
    scanned: &mut usize,
    best: &mut Option<(usize, PathBuf)>,
) {
    if depth > MAX_DEPTH || *scanned >= MAX_SCANNED_FILES {
        return;
    }
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        if *scanned >= MAX_SCANNED_FILES {
            return;
        }
        let path = entry.path();
        // `file_type` avoids following symlinked directory cycles.
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_dir() {
            collect(&path, depth + 1, scanned, best);
            if matches!(best, Some((0, _))) {
                return;
            }
            continue;
        }
        *scanned += 1;
        let Some(rank) = rank_font_file(&path) else {
            continue;
        };
        if best.as_ref().is_none_or(|(current, _)| rank < *current) {
            *best = Some((rank, path));
        }
    }
}

/// Return the preference rank of a font file, or `None` when it is not a
/// recognised CJK face.
fn rank_font_file(path: &Path) -> Option<usize> {
    let extension = path
        .extension()
        .and_then(OsStr::to_str)?
        .to_ascii_lowercase();
    if !matches!(extension.as_str(), "ttf" | "otf" | "ttc" | "otc") {
        return None;
    }
    let stem = path
        .file_stem()
        .and_then(OsStr::to_str)?
        .to_ascii_lowercase()
        .replace([' ', '-', '_'], "");
    PREFERRED_STEMS
        .iter()
        .position(|preferred| stem.starts_with(preferred))
}

fn font_directories() -> Vec<PathBuf> {
    let mut directories = Vec::new();
    let home = std::env::var_os("HOME").map(PathBuf::from);

    if cfg!(target_os = "windows") {
        if let Some(windows) = std::env::var_os("WINDIR") {
            directories.push(PathBuf::from(windows).join("Fonts"));
        }
        if let Some(local) = std::env::var_os("LOCALAPPDATA") {
            directories.push(
                PathBuf::from(local)
                    .join("Microsoft")
                    .join("Windows")
                    .join("Fonts"),
            );
        }
    } else if cfg!(target_os = "macos") {
        if let Some(home) = home.as_ref() {
            directories.push(home.join("Library/Fonts"));
        }
        directories.push(PathBuf::from("/Library/Fonts"));
        directories.push(PathBuf::from("/System/Library/Fonts"));
        directories.push(PathBuf::from("/System/Library/Fonts/Supplemental"));
    } else {
        if let Some(home) = home.as_ref() {
            directories.push(home.join(".local/share/fonts"));
            directories.push(home.join(".fonts"));
        }
        directories.push(PathBuf::from("/usr/share/fonts"));
        directories.push(PathBuf::from("/usr/local/share/fonts"));
        directories.push(PathBuf::from("/run/host/fonts"));
    }
    directories
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::rank_font_file;

    #[test]
    fn recognises_cjk_families_across_platforms() {
        let noto = rank_font_file(Path::new("/usr/share/fonts/NotoSansCJK-Regular.ttc"))
            .expect("Noto CJK should be recognised");
        let fallback =
            rank_font_file(Path::new("/usr/share/fonts/DroidSansFallback.ttf"))
                .expect("Droid fallback should be recognised");

        assert!(noto < fallback, "pan-CJK families must outrank fallbacks");
    }

    #[test]
    fn separators_and_case_do_not_change_the_match() {
        assert_eq!(
            rank_font_file(Path::new("/Library/Fonts/PingFang.ttc")),
            rank_font_file(Path::new("/Library/Fonts/ping fang.TTC")),
        );
    }

    #[test]
    fn latin_only_and_non_font_files_are_ignored() {
        assert_eq!(rank_font_file(Path::new("/usr/share/fonts/Ubuntu-R.ttf")), None);
        assert_eq!(rank_font_file(Path::new("/usr/share/fonts/fonts.dir")), None);
        assert_eq!(rank_font_file(Path::new("/usr/share/fonts/NotoSansCJK")), None);
    }
}
