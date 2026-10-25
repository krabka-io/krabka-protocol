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
    fs::{self, File},
    io::{self, BufRead, BufReader},
    path::{Path, PathBuf},
};

use crate::{emit::common::BANNER_PREFIX, fmt};

/// An error from writing or pruning a generated file.
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error(transparent)]
    Io(#[from] io::Error),
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
    pub fn write(&mut self, path: PathBuf, body: &str) -> io::Result<()> {
        fs::write(&path, body)?;
        self.0.insert(path);
        Ok(())
    }

    /// Format generated Rust source with `format`, write it, and record the
    /// path.
    ///
    /// The quote-based emitters return unformatted token text. The generator
    /// passes [`fmt::rustfmt`], which turns it into the canonical committed
    /// form.
    ///
    /// # Errors
    /// Returns an error when `format` rejects the source or the file cannot be
    /// written.
    pub fn write_rs(
        &mut self,
        path: PathBuf,
        body: &str,
        format: fmt::Formatter,
    ) -> Result<(), WriteError> {
        let body = format(body)?;
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
    pub fn prune(&self, dir: &Path, recursive: bool) -> io::Result<()> {
        if !dir.is_dir() {
            return Ok(());
        }
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                if recursive {
                    self.prune(&path, true)?;
                    if fs::read_dir(&path)?.next().is_none() {
                        fs::remove_dir(&path)?;
                    }
                }
            } else if !self.0.contains(&path) && is_generated(&path)? {
                fs::remove_file(&path)?;
            }
        }
        Ok(())
    }
}

/// True when `path` opens with the generated-file banner.
fn is_generated(path: &Path) -> io::Result<bool> {
    let file = File::open(path)?;
    let mut first = String::new();
    BufReader::new(file).read_line(&mut first)?;
    Ok(first.starts_with(BANNER_PREFIX))
}
