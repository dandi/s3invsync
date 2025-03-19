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

impl From<anyhow::Error> for MultiError {
    fn from(e: anyhow::Error) -> MultiError {
        MultiError(vec![e])
    }
}

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

/// Construct the base filename for backing up an object that is not the latest
/// version of its key, where `basename` is the filename portion of the key,
/// `version_id` is the object's version ID, and `etag` is its etag.
pub(crate) fn make_old_filename(basename: &str, version_id: Option<&str>, etag: &str) -> String {
    format!(
        "{basename}.old.{v}.{etag}",
        v = version_id.unwrap_or("null")
    )
}
