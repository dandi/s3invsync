use std::fmt;
use std::path::Path;

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
