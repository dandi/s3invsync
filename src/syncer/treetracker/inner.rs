use crate::keypath::KeyPath;
use either::Either;
use std::cmp::Ordering;
use std::collections::HashMap;

/// An "open" directory within a [`TreeTracker`][super::TreeTracker], i.e., one
/// to which keys are currently being added (either directly or to a descendant
/// directory)
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PartialDirectory<T> {
    /// All files & directories in this directory that have been seen so far,
    /// excluding `current_subdir`
    pub(super) entries: Vec<Entry<T>>,

    /// The name of the subdirectory of this directory that is currently
    /// "open", if any
    pub(super) current_subdir: Option<String>,
}

impl<T> PartialDirectory<T> {
    /// Create a new, empty `PartialDirectory`
    pub(super) fn new() -> Self {
        PartialDirectory {
            entries: Vec::new(),
            current_subdir: None,
        }
    }

    /// Returns true if the directory is empty, i.e., if no entries have been
    /// registered in it
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.current_subdir.is_none()
    }

    /// Mark the current "open" subdirectory as closed, adding to `entries`
    ///
    /// # Panics
    ///
    /// Panics if there is no current open subdirectory.
    pub(super) fn close_current(&mut self) {
        let Some(name) = self.current_subdir.take() else {
            panic!("PartialDirectory::close_current() called without a current directory");
        };
        self.entries.push(Entry::dir(name));
    }

    /// Returns true if the last entry added to this directory is a subdirectory
    pub(super) fn last_entry_is_dir(&self) -> bool {
        self.current_subdir.is_some()
    }

    /// Compare `cname` against the name of the last entry added to this
    /// directory
    pub(super) fn cmp_vs_last_entry(&self, cname: CmpName<'_>) -> Option<Ordering> {
        self.current_subdir
            .as_deref()
            .map(|cd| cname.cmp(&CmpName::Dir(cd)))
            .or_else(|| self.entries.last().map(|en| cname.cmp(&en.cmp_name())))
    }
}

/// A file or directory entry in an "open" directory tracked by
/// [`TreeTracker`][super::TreeTracker]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Entry<T> {
    File {
        /// The filename
        name: String,

        /// If the latest version of the corresponding key has been added, this
        /// is its payload.
        value: Option<T>,

        /// Mapping from "old filenames" registered for the key to their
        /// payloads
        old_filenames: HashMap<String, T>,
    },
    Dir {
        /// The name of the directory
        name: String,
    },
}

impl<T> Entry<T> {
    /// Create a new `Entry::File` with the given `name`.  If `old_filename` is
    /// `None`, `value` is used as the payload for the latest version of the
    /// key; otherwise, the given old filename is registered with `value` as
    /// its payload.
    pub(super) fn file<S: Into<String>>(
        name: S,
        value: T,
        old_filename: Option<String>,
    ) -> Entry<T> {
        if let Some(of) = old_filename {
            Entry::File {
                name: name.into(),
                value: None,
                old_filenames: HashMap::from([(of, value)]),
            }
        } else {
            Entry::File {
                name: name.into(),
                value: Some(value),
                old_filenames: HashMap::new(),
            }
        }
    }

    /// Create a new `Entry::Dir` with the given `name`
    pub(super) fn dir<S: Into<String>>(name: S) -> Entry<T> {
        Entry::Dir { name: name.into() }
    }

    /// Returns the name of the entry
    pub(super) fn name(&self) -> &str {
        match self {
            Entry::File { name, .. } => name,
            Entry::Dir { name } => name,
        }
    }

    /// Returns the name of the entry as a [`CmpName`]
    pub(super) fn cmp_name(&self) -> CmpName<'_> {
        match self {
            Entry::File { name, .. } => CmpName::File(name.as_ref()),
            Entry::Dir { name } => CmpName::Dir(name.as_ref()),
        }
    }
}

/// A wrapper around an individual path name component that compares it to
/// other components as though they were part of longer paths, i.e., directory
/// names have an implicit trailing '/' added.  As an exception, if a file name
/// and a directory name are equal aside from the trailing '/', this type
/// compares them as equal.
#[derive(Clone, Copy, Debug)]
pub(super) enum CmpName<'a> {
    File(&'a str),
    Dir(&'a str),
}

impl CmpName<'_> {
    /// Returns the inner name, without any trailing slashes
    pub(super) fn name(&self) -> &str {
        match self {
            CmpName::File(s) => s,
            CmpName::Dir(s) => s,
        }
    }

    /// Returns an iterator over all characters in the name.  If the name is
    /// for a directory, a `'/'` is emitted at the end of the iterator.
    pub(super) fn chars(&self) -> impl Iterator<Item = char> + '_ {
        match self {
            CmpName::File(s) => Either::Left(s.chars()),
            CmpName::Dir(s) => Either::Right(s.chars().chain(std::iter::once('/'))),
        }
    }
}

impl PartialEq for CmpName<'_> {
    fn eq(&self, other: &CmpName<'_>) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for CmpName<'_> {}

impl PartialOrd for CmpName<'_> {
    fn partial_cmp(&self, other: &CmpName<'_>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CmpName<'_> {
    fn cmp(&self, other: &CmpName<'_>) -> Ordering {
        if self.name() == other.name() {
            Ordering::Equal
        } else {
            self.chars().cmp(other.chars())
        }
    }
}

/// An iterator over the path components of a key path
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct KeyComponents<'a, T> {
    i: usize,
    path: &'a str,
    value: Option<T>,
    old_filename: Option<Option<String>>,
}

impl<'a, T> KeyComponents<'a, T> {
    pub(super) fn new(key: &'a KeyPath, value: T, old_filename: Option<String>) -> Self {
        KeyComponents {
            i: 0,
            path: key.as_ref(),
            value: Some(value),
            old_filename: Some(old_filename),
        }
    }
}

impl<'a, T> Iterator for KeyComponents<'a, T> {
    type Item = (usize, Component<'a, T>);

    fn next(&mut self) -> Option<Self::Item> {
        let c = match self.path.find('/') {
            Some(i) => {
                let name = &self.path[..i];
                self.path = &self.path[(i + 1)..];
                Component::Dir(name)
            }
            None => Component::File(self.path, self.value.take()?, self.old_filename.take()?),
        };
        let i = self.i;
        self.i += 1;
        Some((i, c))
    }
}

/// A path component of a key path
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Component<'a, T> {
    /// `name` (no trailing slash)
    Dir(&'a str),

    /// `name`, `value`, `old_filename`
    File(&'a str, T, Option<String>),
}

impl<'a, T> Component<'a, T> {
    pub(super) fn cmp_name(&self) -> CmpName<'a> {
        match self {
            Component::Dir(name) => CmpName::Dir(name),
            Component::File(name, _, _) => CmpName::File(name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod cmp_name {
        use super::*;

        #[test]
        fn dir_eq_file() {
            assert!(CmpName::File("foo") == CmpName::Dir("foo"));
        }

        #[test]
        fn pre_slash_dir_before_dir() {
            assert!(CmpName::Dir("apple!banana") < CmpName::Dir("apple"));
        }

        #[test]
        fn pre_slash_file_before_dir() {
            assert!(CmpName::File("apple!banana") < CmpName::Dir("apple"));
        }

        #[test]
        fn pre_slash_dir_after_file() {
            assert!(CmpName::Dir("apple!banana") > CmpName::File("apple"));
        }

        #[test]
        fn pre_slash_file_after_file() {
            assert!(CmpName::File("apple!banana") > CmpName::File("apple"));
        }
    }

    mod key_components {
        use super::*;

        #[test]
        fn plain() {
            let key = "foo/bar/quux.txt".parse::<KeyPath>().unwrap();
            let mut iter = KeyComponents::new(&key, 1, None);
            assert_eq!(iter.next(), Some((0, Component::Dir("foo"))));
            assert_eq!(iter.next(), Some((1, Component::Dir("bar"))));
            assert_eq!(iter.next(), Some((2, Component::File("quux.txt", 1, None))));
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        #[test]
        fn filename_only() {
            let key = "quux.txt".parse::<KeyPath>().unwrap();
            let mut iter = KeyComponents::new(&key, 1, None);
            assert_eq!(iter.next(), Some((0, Component::File("quux.txt", 1, None))));
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        #[test]
        fn with_old_filename() {
            let key = "foo/bar/quux.txt".parse::<KeyPath>().unwrap();
            let mut iter = KeyComponents::new(&key, 1, Some("quux.old.1.2".into()));
            assert_eq!(iter.next(), Some((0, Component::Dir("foo"))));
            assert_eq!(iter.next(), Some((1, Component::Dir("bar"))));
            assert_eq!(
                iter.next(),
                Some((
                    2,
                    Component::File("quux.txt", 1, Some("quux.old.1.2".into()))
                ))
            );
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }
    }
}
