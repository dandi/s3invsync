use std::fmt;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

/// An error type containing a collection of one or more errors that occurred
/// concurrently during syncing
#[derive(Debug)]
pub(crate) struct MultiError(pub(crate) Vec<anyhow::Error>);

impl fmt::Display for MultiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.len() > 1 {
            writeln!(f, "{} ERRORS:\n---", self.0.len())?;
        }
        let mut first = true;
        for e in &self.0 {
            if !std::mem::replace(&mut first, false) {
                writeln!(f, "\n---")?;
            }
            write!(f, "{e:?}")?;
        }
        Ok(())
    }
}

impl std::error::Error for MultiError {}

/// If `r` is an `Err` with the given `ErrorKind`, convert it to `Ok(())`.
fn suppress_error_kind(r: std::io::Result<()>, kind: ErrorKind) -> std::io::Result<()> {
    if matches!(r, Err(ref e) if e.kind() == kind) {
        Ok(())
    } else {
        r
    }
}

/// Returns `true` if `p` is an empty directory
pub(crate) fn is_empty_dir(p: &Path) -> std::io::Result<bool> {
    let mut iter = fs_err::read_dir(p)?;
    match iter.next() {
        None => Ok(true),
        Some(Ok(_)) => Ok(false),
        Some(Err(e)) if e.kind() == ErrorKind::NotFound => Ok(false),
        Some(Err(e)) => Err(e),
    }
}

/// Deletes the directory `topdir` and all of its parent directories up to —
/// but not including — `rootdir`, so long as each is empty.
pub(crate) fn rmdir_to_root(topdir: &Path, rootdir: &Path) -> std::io::Result<()> {
    for p in topdir.ancestors() {
        if p == rootdir || !is_empty_dir(p)? {
            break;
        }
        suppress_error_kind(fs_err::remove_dir(p), ErrorKind::NotFound)?;
    }
    Ok(())
}

/// If `p` is a directory or a symlink, delete it.  Returns `true` if `p`
/// exists afterwards.
pub(crate) async fn ensure_file(p: &Path) -> anyhow::Result<bool> {
    match fs_err::symlink_metadata(p) {
        Ok(md) if md.is_dir() => {
            tracing::debug!(path = %p.display(), "Download path is an unexpected directory; deleting");
            fs_err::tokio::remove_dir_all(p).await?;
            Ok(false)
        }
        Ok(md) if md.is_symlink() => {
            tracing::debug!(path = %p.display(), "Download path is an unexpected symlink; deleting");
            fs_err::tokio::remove_file(p).await?;
            Ok(false)
        }
        Ok(md) if md.is_file() => Ok(true),
        Ok(md) => anyhow::bail!(
            "Path {} has unexpected file type {:?}",
            p.display(),
            md.file_type()
        ),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Ensure that the path formed by concatenating `root` with `dirs` exists and
/// is a directory.  If `root` concatenated with any leading sequence of `dirs`
/// already exists but is not a directory, delete it.
pub(crate) fn force_create_dir_all<I: IntoIterator<Item: AsRef<Path>>>(
    root: &Path,
    dirs: I,
) -> std::io::Result<()> {
    let mut p = PathBuf::from(root);
    for d in dirs {
        p.push(d);
        match fs_err::symlink_metadata(&p) {
            Ok(md) => {
                if !md.is_dir() {
                    tracing::debug!(path = %p.display(), "Intermediate path in directory structure is an unexpected file; deleting");
                    // Work around races when multiple tasks create the same
                    // directory path at once:
                    suppress_error_kind(fs_err::remove_file(&p), ErrorKind::NotFound)?;
                    suppress_error_kind(fs_err::create_dir(&p), ErrorKind::AlreadyExists)?;
                }
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                // Work around races when multiple tasks create the same
                // directory path at once:
                suppress_error_kind(fs_err::create_dir(&p), ErrorKind::AlreadyExists)?;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

pub(crate) fn make_old_filename(basename: &str, version_id: &str, etag: &str) -> String {
    format!("{basename}.old.{version_id}.{etag}")
}

#[cfg(test)]
mod tests {
    use super::*;

    mod rmdir_to_root {
        use super::*;
        use fs_err::PathExt;
        use tempfile::tempdir;

        #[test]
        fn empty_tree() {
            let root = tempdir().unwrap();
            let topdir = root.path().join("apple").join("banana").join("coconut");
            fs_err::create_dir_all(&topdir).unwrap();
            rmdir_to_root(&topdir, root.path()).unwrap();
            assert!(is_empty_dir(root.path()).unwrap());
        }

        #[test]
        fn file_in_topdir() {
            let root = tempdir().unwrap();
            let topdir = root.path().join("apple").join("banana").join("coconut");
            let filepath = topdir.join("file.txt");
            fs_err::create_dir_all(&topdir).unwrap();
            fs_err::write(&filepath, b"This is test text.\n").unwrap();
            rmdir_to_root(&topdir, root.path()).unwrap();
            assert!(filepath.fs_err_try_exists().unwrap());
        }

        #[test]
        fn file_above_topdir() {
            let root = tempdir().unwrap();
            let topdir = root.path().join("apple").join("banana").join("coconut");
            let filepath = root.path().join("apple").join("banana").join("file.txt");
            fs_err::create_dir_all(&topdir).unwrap();
            fs_err::write(&filepath, b"This is test text.\n").unwrap();
            rmdir_to_root(&topdir, root.path()).unwrap();
            assert!(filepath.fs_err_try_exists().unwrap());
            assert!(!topdir.fs_err_try_exists().unwrap());
        }
    }
}
