//! A private scratch directory for tests that write files.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
};

/// A directory under the system temporary directory, removed on drop.
///
/// Bazel points `TMPDIR` at the test's own temporary directory, so the
/// directory is private to the test under Bazel as well as under Cargo.
pub struct Scratch(PathBuf);

impl Scratch {
    /// An empty directory named after the process and `name`.
    pub fn new(name: &str) -> Self {
        let dir = env::temp_dir().join(format!("krabka-codegen-{}-{name}", process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    /// The directory itself.
    pub fn path(&self) -> &Path {
        &self.0
    }

    /// Write `body` to `rel` below the directory, creating its parents.
    pub fn at(&self, rel: &str, body: &str) -> PathBuf {
        let path = self.0.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, body).unwrap();
        path
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
