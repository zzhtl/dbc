//! Atomic file replacement.
//!
//! Every file this application writes — exports, connection settings, the
//! credential vault — lands in a sibling temporary file first and only replaces
//! the target once the bytes are durable. A cancelled export or a crash mid
//! write therefore never truncates the file the user already had.

use std::{
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

/// Distinguishes concurrent writers inside one process; the pid separates
/// writers across processes.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A file that becomes visible at its target path only on [`AtomicFile::commit`].
#[derive(Debug)]
pub struct AtomicFile {
    target: PathBuf,
    temporary: PathBuf,
    file: Option<File>,
}

impl AtomicFile {
    /// Create the temporary file next to `target`.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error when the parent directory is missing or
    /// not writable.
    pub fn create(target: &Path) -> io::Result<Self> {
        let parent = target.parent().unwrap_or(Path::new("."));
        fs::create_dir_all(parent)?;
        let unique = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let name = target
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("dbc-output");
        let temporary = parent.join(format!(
            ".{name}.{}.{unique}.tmp",
            std::process::id()
        ));
        let file = File::create(&temporary)?;
        Ok(Self {
            target: target.to_path_buf(),
            temporary,
            file: Some(file),
        })
    }

    pub fn writer(&mut self) -> &mut File {
        self.file
            .as_mut()
            .expect("the file is present until commit consumes it")
    }

    /// Restrict the temporary file to the current user before secrets reach it.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error when the mode cannot be applied.
    #[cfg(unix)]
    pub fn restrict_to_owner(&self) -> io::Result<()> {
        use std::os::unix::fs::PermissionsExt as _;

        fs::set_permissions(&self.temporary, fs::Permissions::from_mode(0o600))
    }

    #[cfg(not(unix))]
    pub fn restrict_to_owner(&self) -> io::Result<()> {
        // Windows and other targets inherit the parent directory's ACL, which is
        // already user-scoped for the per-user configuration directory.
        Ok(())
    }

    /// Flush, sync and move the temporary file onto the target path.
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error; the target keeps its previous contents
    /// when any step fails.
    pub fn commit(mut self) -> io::Result<()> {
        let mut file = self
            .file
            .take()
            .expect("the file is present until commit consumes it");
        file.flush()?;
        file.sync_all()?;
        drop(file);
        fs::rename(&self.temporary, &self.target)
    }
}

impl Drop for AtomicFile {
    fn drop(&mut self) {
        // Only reached when `commit` was not called: cancelled, failed, or a panic.
        if self.file.take().is_some() {
            let _ignored = fs::remove_file(&self.temporary);
        }
    }
}

/// Write `bytes` to `target` atomically.
///
/// # Errors
///
/// Returns the underlying I/O error without touching the existing target.
pub fn write_atomic(target: &Path, bytes: &[u8], owner_only: bool) -> io::Result<()> {
    let mut file = AtomicFile::create(target)?;
    if owner_only {
        file.restrict_to_owner()?;
    }
    file.writer().write_all(bytes)?;
    file.commit()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{AtomicFile, write_atomic};

    #[test]
    fn a_dropped_write_leaves_the_previous_contents_intact() {
        let directory = std::env::temp_dir().join("dbc-atomic-drop-test");
        let _ignored = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).expect("test directory should be creatable");
        let target = directory.join("data.txt");
        fs::write(&target, b"original").expect("seed file should be writable");

        {
            let mut file = AtomicFile::create(&target).expect("temp file should open");
            std::io::Write::write_all(file.writer(), b"replacement")
                .expect("temp write should succeed");
            // Dropped without commit.
        }

        assert_eq!(
            fs::read_to_string(&target).expect("target should still exist"),
            "original"
        );
        // The temporary file must not linger next to the target.
        let leftovers = fs::read_dir(&directory)
            .expect("directory should be readable")
            .flatten()
            .count();
        assert_eq!(leftovers, 1);

        let _ignored = fs::remove_dir_all(&directory);
    }

    #[test]
    fn commit_replaces_the_target() {
        let directory = std::env::temp_dir().join("dbc-atomic-commit-test");
        let _ignored = fs::remove_dir_all(&directory);
        let target = directory.join("nested").join("data.txt");

        write_atomic(&target, b"first", false).expect("first write should succeed");
        write_atomic(&target, b"second", false).expect("second write should succeed");

        assert_eq!(
            fs::read_to_string(&target).expect("target should exist"),
            "second"
        );

        let _ignored = fs::remove_dir_all(&directory);
    }
}
