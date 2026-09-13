//! The output ledger: every path one generator run writes, and the sweep that
//! deletes the generated files it did not.
//!
//! The generator overwrites its outputs in place. That leaves a hole. When a
//! schema leaves the schema set, or a message renames a common struct, the
//! file from the previous run stays on disk. It is still tracked and still
//! unchanged, so `git status` reports nothing and the `codegen drift` CI job
//! passes over a `generated` tree that no longer matches the schemas.
//!
//! [`Written`] closes the hole. Each write records its path. [`Written::prune`]
//! then walks the output directories and removes every generated file that the
//! run did not write.

use std::{
    collections::BTreeSet,
    io::BufRead,
    path::{Path, PathBuf},
};

use crate::{emit::common::BANNER_PREFIX, fmt};

/// An error from writing or pruning a generated file.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Fmt(#[from] fmt::FmtError),
}

/// The set of paths one generator run wrote.
#[derive(Debug, Default)]
pub struct Written(BTreeSet<PathBuf>);

impl Written {
    /// Write a file verbatim and record the path.
    ///
    /// # Errors
    /// Returns an error when the file cannot be written.
    pub fn write(&mut self, path: PathBuf, body: &str) -> std::io::Result<()> {
        std::fs::write(&path, body)?;
        self.0.insert(path);
        Ok(())
    }

    /// Format generated Rust source through rustfmt, write it, and record the
    /// path.
    ///
    /// The quote-based emitters return unformatted token text. rustfmt is the
    /// secondary processing step that turns it into the canonical committed
    /// form.
    ///
    /// # Errors
    /// Returns an error when rustfmt rejects the source or the file cannot be
    /// written.
    pub fn write_rs(&mut self, path: PathBuf, body: &str) -> Result<(), WriteError> {
        let body = fmt::rustfmt(body)?;
        self.write(path, &body)?;
        Ok(())
    }

    /// True when this run wrote `path`.
    #[must_use]
    pub fn contains(&self, path: &Path) -> bool {
        self.0.contains(path)
    }

    /// Delete every generated file under `dir` that this run did not write.
    ///
    /// A file counts as generated when it opens with [`BANNER_PREFIX`]. The
    /// sweep leaves every other file alone, so a hand-written file in an
    /// output directory, or a wrong output path on the command line, does not
    /// lose data.
    ///
    /// `recursive` descends into subdirectories and removes the ones it leaves
    /// empty. A flat sweep stops at the first level, which is what
    /// `crates/protocol/generated` needs: it holds a namespace subdirectory
    /// that a separate run owns.
    ///
    /// # Errors
    /// Returns an error when a directory cannot be read or a file cannot be
    /// removed.
    pub fn prune(&self, dir: &Path, recursive: bool) -> std::io::Result<()> {
        if !dir.is_dir() {
            return Ok(());
        }
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                if recursive {
                    self.prune(&path, true)?;
                    if std::fs::read_dir(&path)?.next().is_none() {
                        std::fs::remove_dir(&path)?;
                    }
                }
            } else if !self.0.contains(&path) && is_generated(&path)? {
                std::fs::remove_file(&path)?;
            }
        }
        Ok(())
    }
}

/// True when `path` opens with the generated-file banner.
fn is_generated(path: &Path) -> std::io::Result<bool> {
    let file = std::fs::File::open(path)?;
    let mut first = String::new();
    std::io::BufReader::new(file).read_line(&mut first)?;
    Ok(first.starts_with(BANNER_PREFIX))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::emit::common::banner;

    /// A private scratch directory that the test removes when it drops.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir()
                .join(format!("krabka-codegen-out-{}-{name}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn generated(&self, rel: &str) -> PathBuf {
            self.at(rel, &format!("{}pub const X: i16 = 0;\n", banner("test")))
        }

        fn at(&self, rel: &str, body: &str) -> PathBuf {
            let path = self.0.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, body).unwrap();
            path
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn prune_removes_a_generated_file_the_run_did_not_write() {
        let scratch = Scratch::new("stale");
        let stale = scratch.generated("Removed.owned.rs");
        let fresh = scratch.0.join("Kept.owned.rs");

        let mut written = Written::default();
        written
            .write(fresh.clone(), &format!("{}\n", banner("test")))
            .unwrap();
        written.prune(&scratch.0, false).unwrap();

        assert!(!stale.exists());
        assert!(fresh.exists());
        assert!(written.contains(&fresh));
    }

    #[test]
    fn prune_keeps_a_hand_written_neighbour() {
        let scratch = Scratch::new("handwritten");
        let hand = scratch.at("hand_written.rs", "//! Hand-written.\n");

        let written = Written::default();
        written.prune(&scratch.0, false).unwrap();

        assert!(hand.exists());
    }

    #[test]
    fn a_flat_prune_leaves_subdirectories_alone() {
        let scratch = Scratch::new("flat");
        let nested = scratch.generated("kafka_3_6_2/Removed.owned.rs");

        let written = Written::default();
        written.prune(&scratch.0, false).unwrap();

        assert!(nested.exists());
    }

    #[test]
    fn a_recursive_prune_drops_the_directory_it_empties() {
        let scratch = Scratch::new("recursive");
        let nested = scratch.generated("common/owned/gone/key_value.owned.rs");

        let written = Written::default();
        written.prune(&scratch.0, true).unwrap();

        assert!(!nested.exists());
        assert!(!scratch.0.join("common/owned/gone").exists());
    }
}
