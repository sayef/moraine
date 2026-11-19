//! A read-only tantivy [`Directory`] over the sections of a bundle.
//!
//! The original design unpacks a full-text bundle into a cache directory and opens it with tantivy's
//! `MmapDirectory`. Serving the sections in place does the same without a second copy on disk,
//! and works the same way in the memory cache mode: the bundle bytes are already mapped and
//! 4,096-aligned, and every file tantivy opens is a slice of them.
//!
//! A full-text bundle holds one tantivy segment. Its sections are `meta.json` (tantivy's index
//! metadata, with the segment id replaced by [`SEGMENT_ID`] so that equal rows give equal
//! bytes) and `seg.<ext>`, one per segment component. tantivy asks for `<segment id>.<ext>`;
//! the directory serves `seg.<ext>` for it.

use std::collections::HashMap;
use std::io;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tantivy::Directory;
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    DirectoryLock, FileHandle, Lock, OwnedBytes, WatchCallback, WatchHandle, WritePtr,
};

use crate::index::bundle::{BundleBytes, BundleView};

/// The segment id every full-text bundle names (its simple form is 32 zeros).
pub const SEGMENT_ID: &str = "00000000000000000000000000000000";
/// Section holding tantivy's `meta.json`.
pub const META: &str = "meta.json";
/// Prefix of the sections holding segment components.
pub const SEG: &str = "seg.";

/// The bundle bytes, as tantivy's owned bytes require: a stable address for as long as they
/// live.
struct Held(BundleBytes);

impl Deref for Held {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

// SAFETY: `BundleBytes` is an `Arc` of a read-only mapping; moving `Held` moves the `Arc`, never
// the mapped bytes, so the slice it derefs to keeps its address for its whole life.
unsafe impl stable_deref_trait::StableDeref for Held {}

/// See the module documentation.
#[derive(Clone)]
pub struct BundleDirectory {
    files: Arc<HashMap<PathBuf, OwnedBytes>>,
}

impl std::fmt::Debug for BundleDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BundleDirectory")
            .field("files", &self.files.len())
            .finish()
    }
}

impl BundleDirectory {
    /// The directory a bundle's sections describe. Sections other than `meta.json` and
    /// `seg.*` are ignored.
    pub fn new(view: &BundleView) -> BundleDirectory {
        let all = OwnedBytes::new(Held(view.bytes().clone()));
        let mut files = HashMap::new();
        for s in view.sections() {
            let path = if s.name == META {
                PathBuf::from(META)
            } else if let Some(ext) = s.name.strip_prefix(SEG) {
                PathBuf::from(format!("{SEGMENT_ID}.{ext}"))
            } else {
                continue;
            };
            let (start, len) = (s.offset as usize, s.length as usize);
            files.insert(path, all.slice(start..start + len));
        }
        BundleDirectory {
            files: Arc::new(files),
        }
    }

    fn file(&self, path: &Path) -> Result<&OwnedBytes, OpenReadError> {
        self.files
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }
}

fn read_only() -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, "a bundle is read-only")
}

impl Directory for BundleDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        Ok(Arc::new(self.file(path)?.clone()))
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: Arc::new(read_only()),
            filepath: path.to_path_buf(),
        })
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        Ok(self.files.contains_key(path))
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::wrap_io_error(
            read_only(),
            path.to_path_buf(),
        ))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        Ok(self.file(path)?.as_slice().to_vec())
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        Err(read_only())
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    /// Nothing ever writes a bundle, so readers need no lock.
    fn acquire_lock(&self, _lock: &Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(())))
    }

    fn watch(&self, _cb: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }
}
