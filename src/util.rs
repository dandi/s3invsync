use crate::consts::METADATA_FILENAME;
use std::fmt;
use std::path::Path;
use thiserror::Error;

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

pub(crate) fn is_empty_dir(p: &Path) -> std::io::Result<bool> {
    let mut iter = fs_err::read_dir(p)?;
    match iter.next() {
        None => Ok(true),
        Some(Ok(_)) => Ok(false),
        Some(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Some(Err(e)) => Err(e),
    }
}

pub(crate) fn check_normed_posix_path(key: &str) -> Result<(), PurePathError> {
    if key.is_empty() {
        Err(PurePathError::Empty {
            key: key.to_owned(),
        })
    } else if key.starts_with('/') {
        Err(PurePathError::StartsWithSlash {
            key: key.to_owned(),
        })
    } else if key.ends_with('/') {
        Err(PurePathError::EndsWithSlash {
            key: key.to_owned(),
        })
    } else if key.contains('\0') {
        Err(PurePathError::Nul {
            key: key.to_owned(),
        })
    } else if key
        .split('/')
        .any(|p| p.is_empty() || p == "." || p == "..")
    {
        Err(PurePathError::NotNormalized {
            key: key.to_owned(),
        })
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum PurePathError {
    #[error("key {key:?} is not a valid filepath: empty")]
    Empty { key: String },
    #[error("key {key:?} is not a valid filepath: starts with a forward slash")]
    StartsWithSlash { key: String },
    #[error("key {key:?} is not a valid filepath: ends with a forward slash")]
    EndsWithSlash { key: String },
    #[error("key {key:?} is not a valid filepath: contains NUL")]
    Nul { key: String },
    #[error("key {key:?} is not a valid filepath: not normalized")]
    NotNormalized { key: String },
}

// Reject filenames that equal `METADATA_FILENAME` or look like
// `{filename}.old.{version_id}.{etag}` (specifically, that are of the form
// `{nonempty}.old.{nonempty}.{nonempty}`)
pub(crate) fn check_special_filename(filename: &str) -> Result<(), SpecialFilenameError> {
    if filename == METADATA_FILENAME {
        return Err(SpecialFilenameError {
            filename: filename.to_owned(),
        });
    }
    if let Some(i) = filename.find(".old.").filter(|&i| i > 0) {
        let post_old = &filename[(i + 5)..];
        if post_old
            .find('.')
            .is_some_and(|j| (1..(post_old.len() - 1)).contains(&j))
        {
            return Err(SpecialFilenameError {
                filename: filename.to_owned(),
            });
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("cannot back up object with special filename {filename:?}")]
pub(crate) struct SpecialFilenameError {
    filename: String,
}

/// Deletes the directory `topdir` and all of its parent directories up to —
/// but not including — `rootdir`, so long as each is empty.
pub(crate) fn rmdir_to_root(topdir: &Path, rootdir: &Path) -> std::io::Result<()> {
    let mut p = Some(topdir);
    while let Some(pp) = p {
        if pp == rootdir || !is_empty_dir(pp)? {
            break;
        }
        match fs_err::remove_dir(pp) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        }
        p = pp.parent();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("foo", true)]
    #[case("foo.old", true)]
    #[case("foo.old.bar", true)]
    #[case("foo.old.bar.baz", false)]
    #[case("foo.old.bar.baz.quux.glarch", false)]
    #[case("foo.old.bar.", true)]
    #[case(".old.bar.baz", true)]
    #[case("foo.old..baz", true)]
    #[case("foo.old..", true)]
    fn test_check_special_filename(#[case] s: &str, #[case] ok: bool) {
        assert_eq!(check_special_filename(s).is_ok(), ok);
    }

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
