use crate::keypath::KeyPath;
use either::Either;
use std::cmp::Ordering;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PartialDirectory<T> {
    pub(super) entries: Vec<Entry<T>>,
    pub(super) current_subdir: Option<String>,
}

impl<T> PartialDirectory<T> {
    pub(super) fn new() -> Self {
        PartialDirectory {
            entries: Vec::new(),
            current_subdir: None,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.current_subdir.is_none()
    }

    pub(super) fn close_current(&mut self) {
        let Some(name) = self.current_subdir.take() else {
            panic!("PartialDirectory::close_current() called without a current directory");
        };
        self.entries.push(Entry::dir(name));
    }

    pub(super) fn last_entry_is_dir(&self) -> bool {
        self.current_subdir.is_some()
    }

    pub(super) fn cmp_vs_last_entry(&self, cname: CmpName<'_>) -> Option<Ordering> {
        self.current_subdir
            .as_deref()
            .map(|cd| cname.cmp(&CmpName::Dir(cd)))
            .or_else(|| self.entries.last().map(|en| cname.cmp(&en.cmp_name())))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Entry<T> {
    File {
        name: String,
        //old_filenames: Vec<String>, // TODO
        value: T,
    },
    Dir {
        name: String,
    },
}

impl<T> Entry<T> {
    pub(super) fn file<S: Into<String>>(name: S, value: T) -> Entry<T> {
        Entry::File {
            name: name.into(),
            //old_filenames: Vec::new(), // TODO
            value,
        }
    }

    pub(super) fn dir<S: Into<String>>(name: S) -> Entry<T> {
        Entry::Dir { name: name.into() }
    }

    pub(super) fn name(&self) -> &str {
        match self {
            Entry::File { name, .. } => name,
            Entry::Dir { name } => name,
        }
    }

    pub(super) fn is_file(&self) -> bool {
        matches!(self, Entry::File { .. })
    }

    pub(super) fn is_dir(&self) -> bool {
        matches!(self, Entry::Dir { .. })
    }

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
    pub(super) fn name(&self) -> &str {
        match self {
            CmpName::File(s) => s,
            CmpName::Dir(s) => s,
        }
    }

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct KeyComponents<'a, T> {
    i: usize,
    path: &'a str,
    value: Option<T>,
}

impl<'a, T> KeyComponents<'a, T> {
    pub(super) fn new(key: &'a KeyPath, value: T) -> Self {
        KeyComponents {
            i: 0,
            path: key.as_ref(),
            value: Some(value),
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
            None => Component::File(self.path, self.value.take()?),
        };
        let i = self.i;
        self.i += 1;
        Some((i, c))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Component<'a, T> {
    Dir(&'a str),
    File(&'a str, T),
}

impl<'a, T> Component<'a, T> {
    pub(super) fn cmp_name(&self) -> CmpName<'a> {
        match self {
            Component::Dir(name) => CmpName::Dir(name),
            Component::File(name, _) => CmpName::File(name),
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
            let mut iter = KeyComponents::new(&key, 1);
            assert_eq!(iter.next(), Some((0, Component::Dir("foo"))));
            assert_eq!(iter.next(), Some((1, Component::Dir("bar"))));
            assert_eq!(iter.next(), Some((2, Component::File("quux.txt", 1))));
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }

        #[test]
        fn filename_only() {
            let key = "quux.txt".parse::<KeyPath>().unwrap();
            let mut iter = KeyComponents::new(&key, 1);
            assert_eq!(iter.next(), Some((0, Component::File("quux.txt", 1))));
            assert_eq!(iter.next(), None);
            assert_eq!(iter.next(), None);
        }
    }
}
