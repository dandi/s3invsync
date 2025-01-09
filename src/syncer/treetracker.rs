use crate::keypath::KeyPath;
use either::Either;
use std::cmp::Ordering;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct TreeTracker<T>(Vec<PartialDirectory<T>>);

impl<T> TreeTracker<T> {
    pub(super) fn new() -> Self {
        TreeTracker(vec![PartialDirectory::new()])
    }

    pub(super) fn add(
        &mut self,
        key: &KeyPath,
        //old_filename: Option<String>, // TODO
        value: T,
    ) -> Result<Vec<Directory<T>>, TreeTrackerError> {
        let (dirpath, filename) = key.split();
        let mut popped_dirs = Vec::new();
        let mut parts = dirpath.unwrap_or_default().split('/').enumerate();
        for (i, dirname) in parts.by_ref() {
            let cmp_dirname = CmpName::Dir(dirname);
            let Some(pd) = self.0.get_mut(i) else {
                unreachable!(
                    "TreeTracker::add() iteration should not go past the end of the stack"
                );
            };
            if let Some(cd) = pd.current_subdir.as_ref() {
                match cmp_dirname.cmp(&CmpName::Dir(cd)) {
                    Ordering::Equal => continue,
                    Ordering::Greater => {
                        // Close current dirs & push
                        for _ in (i + 1)..(self.0.len()) {
                            popped_dirs.push(self.pop());
                        }
                        self.push_dir(dirname);
                        break; // GOTO push
                    }
                    Ordering::Less => {
                        return Err(TreeTrackerError::Unsorted {
                            before: self.last_key(),
                            after: key.into(),
                        });
                    }
                }
            } else if let Some(en) = pd.entries.last() {
                match cmp_dirname.cmp(&en.cmp_name()) {
                    Ordering::Equal => {
                        assert!(en.is_file(), "last element of PartialDirectory::entries should be a file when current_subdir is None");
                        return Err(TreeTrackerError::Conflict(self.last_key()));
                    }
                    Ordering::Greater => {
                        self.push_dir(dirname);
                        break; // GOTO push
                    }
                    Ordering::Less => {
                        return Err(TreeTrackerError::Unsorted {
                            before: self.last_key(),
                            after: key.into(),
                        });
                    }
                }
            } else {
                assert!(
                    self.is_empty(),
                    "top dir of TreeTracker should be root when empty"
                );
                self.push_dir(dirname);
                break; // GOTO push
            }
        }
        for (_, dirname) in parts {
            self.push_dir(dirname);
        }
        if self.push_file(filename, value) {
            Ok(popped_dirs)
        } else {
            Err(TreeTrackerError::Unsorted {
                before: self.last_key(),
                after: key.into(),
            })
        }
    }

    pub(super) fn finish(mut self) -> Vec<Directory<T>> {
        let mut dirs = Vec::new();
        while !self.0.is_empty() {
            dirs.push(self.pop());
        }
        dirs
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty() || (self.0.len() == 1 && self.0[0].is_empty())
    }

    fn push_dir(&mut self, name: &str) {
        let Some(pd) = self.0.last_mut() else {
            panic!("TreeTracker::push_dir() called on void tracker");
        };
        assert!(
            pd.current_subdir.is_none(),
            "TreeTracker::push_dir() called when top dir has subdir"
        );
        pd.current_subdir = Some(name.to_owned());
        self.0.push(PartialDirectory::new());
    }

    fn push_file(&mut self, name: &str, value: T) -> bool {
        let Some(pd) = self.0.last_mut() else {
            panic!("TreeTracker::push_file() called on void tracker");
        };
        assert!(
            pd.current_subdir.is_none(),
            "TreeTracker::push_file() called when top dir has subdir"
        );
        if let Some(en) = pd.entries.last() {
            if en.cmp_name() >= CmpName::File(name) {
                return false;
            }
        }
        pd.entries.push(Entry::file(name, value));
        true
    }

    fn pop(&mut self) -> Directory<T> {
        let Some(pd) = self.0.pop() else {
            panic!("TreeTracker::pop() called on void tracker");
        };
        assert!(
            pd.current_subdir.is_none(),
            "TreeTracker::pop() called when top dir has subdir"
        );
        let entries = pd.entries;
        let path = (!self.0.is_empty()).then(|| self.last_key());
        if let Some(ppd) = self.0.last_mut() {
            ppd.close_current();
        }
        Directory { path, entries }
    }

    fn last_key(&self) -> String {
        let mut s = String::new();
        for pd in &self.0 {
            if let Some(name) = pd
                .current_subdir
                .as_deref()
                .or_else(|| pd.entries.last().map(Entry::name))
            {
                if !s.is_empty() {
                    s.push('/');
                }
                s.push_str(name);
            } else {
                assert!(
                    self.is_empty(),
                    "TreeTracker dir should be empty root when empty"
                );
                panic!("TreeTracker::last_key() called on empty tracker");
            }
        }
        s
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PartialDirectory<T> {
    entries: Vec<Entry<T>>,
    current_subdir: Option<String>,
}

impl<T> PartialDirectory<T> {
    fn new() -> Self {
        PartialDirectory {
            entries: Vec::new(),
            current_subdir: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.current_subdir.is_none()
    }

    fn close_current(&mut self) {
        let Some(name) = self.current_subdir.take() else {
            panic!("PartialDirectory::close_current() called without a current directory");
        };
        self.entries.push(Entry::dir(name));
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Entry<T> {
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
    fn file<S: Into<String>>(name: S, value: T) -> Entry<T> {
        Entry::File {
            name: name.into(),
            //old_filenames: Vec::new(), // TODO
            value,
        }
    }

    fn dir<S: Into<String>>(name: S) -> Entry<T> {
        Entry::Dir { name: name.into() }
    }

    fn name(&self) -> &str {
        match self {
            Entry::File { name, .. } => name,
            Entry::Dir { name } => name,
        }
    }

    fn is_file(&self) -> bool {
        matches!(self, Entry::File { .. })
    }

    fn is_dir(&self) -> bool {
        matches!(self, Entry::Dir { .. })
    }

    fn cmp_name(&self) -> CmpName<'_> {
        match self {
            Entry::File { name, .. } => CmpName::File(name.as_ref()),
            Entry::Dir { name } => CmpName::Dir(name.as_ref()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Directory<T> {
    path: Option<String>,   // `None` for the root
    entries: Vec<Entry<T>>, // TODO: Flatten out the old_filenames
}

impl<T> Directory<T> {
    pub(super) fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    fn find(&self, name: &str) -> Option<&Entry<T>> {
        self.entries
            .binary_search_by(|en| en.name().cmp(name))
            .ok()
            .map(|i| &self.entries[i])
    }

    pub(super) fn contains_file(&self, name: &str) -> bool {
        self.find(name).is_some_and(Entry::is_file)
    }

    pub(super) fn contains_dir(&self, name: &str) -> bool {
        self.find(name).is_some_and(Entry::is_dir)
    }

    #[allow(dead_code)]
    pub(super) fn map<U, F: FnMut(T) -> U>(self, mut f: F) -> Directory<U> {
        Directory {
            path: self.path,
            entries: self
                .entries
                .into_iter()
                .map(|en| match en {
                    Entry::File { name, value } => Entry::File {
                        name,
                        value: f(value),
                    },
                    Entry::Dir { name } => Entry::Dir { name },
                })
                .collect(),
        }
    }
}

/// A wrapper around an individual path name component that compares it to
/// other components as though they were part of longer paths, i.e., directory
/// names have an implicit trailing '/' added.  As an exception, if a file name
/// and a directory name are equal aside from the trailing '/', this type
/// compares them as equal.
#[derive(Clone, Copy, Debug)]
enum CmpName<'a> {
    File(&'a str),
    Dir(&'a str),
}

impl CmpName<'_> {
    fn name(&self) -> &str {
        match self {
            CmpName::File(s) => s,
            CmpName::Dir(s) => s,
        }
    }

    fn chars(&self) -> impl Iterator<Item = char> + '_ {
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

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(super) enum TreeTrackerError {
    #[error("received keys in unsorted order: {before:?} came before {after:?}")]
    Unsorted { before: String, after: String },
    #[error("path {0:?} is used as both a file and a directory")]
    Conflict(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_dir() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/bar.txt".parse::<KeyPath>().unwrap(), 1),
            Ok(Vec::new())
        );
        assert_eq!(
            tracker.add(&"foo/quux.txt".parse::<KeyPath>().unwrap(), 2),
            Ok(Vec::new())
        );
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 2);
        assert_eq!(dirs[0].path(), Some("foo"));
        assert_eq!(
            dirs[0].entries,
            vec![Entry::file("bar.txt", 1), Entry::file("quux.txt", 2)]
        );
        assert_eq!(dirs[1].path(), None);
        assert_eq!(dirs[1].entries, vec![Entry::dir("foo")]);
    }

    #[test]
    fn different_dir() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/bar.txt".parse::<KeyPath>().unwrap(), 1),
            Ok(Vec::new())
        );
        let dirs = tracker
            .add(&"glarch/quux.txt".parse::<KeyPath>().unwrap(), 2)
            .unwrap();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path(), Some("foo"));
        assert_eq!(dirs[0].entries, vec![Entry::file("bar.txt", 1)]);
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 2);
        assert_eq!(dirs[0].path(), Some("glarch"));
        assert_eq!(dirs[0].entries, vec![Entry::file("quux.txt", 2)]);
        assert_eq!(dirs[1].path(), None);
        assert_eq!(
            dirs[1].entries,
            vec![Entry::dir("foo"), Entry::dir("glarch")]
        );
    }

    #[test]
    fn different_subdir() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/bar/apple.txt".parse::<KeyPath>().unwrap(), 1),
            Ok(Vec::new())
        );
        let dirs = tracker
            .add(&"foo/quux/banana.txt".parse::<KeyPath>().unwrap(), 2)
            .unwrap();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path(), Some("foo/bar"));
        assert_eq!(dirs[0].entries, vec![Entry::file("apple.txt", 1)]);
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 3);
        assert_eq!(dirs[0].path(), Some("foo/quux"));
        assert_eq!(dirs[0].entries, vec![Entry::file("banana.txt", 2)]);
        assert_eq!(dirs[1].path(), Some("foo"));
        assert_eq!(dirs[1].entries, vec![Entry::dir("bar"), Entry::dir("quux")]);
        assert_eq!(dirs[2].path(), None);
        assert_eq!(dirs[2].entries, vec![Entry::dir("foo")]);
    }

    #[test]
    fn preslash_dir_then_toslash_dir() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(
                &"foo/apple!banana/gnusto.txt".parse::<KeyPath>().unwrap(),
                1,
            ),
            Ok(Vec::new())
        );
        let dirs = tracker
            .add(&"foo/apple/cleesh.txt".parse::<KeyPath>().unwrap(), 2)
            .unwrap();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path(), Some("foo/apple!banana"));
        assert_eq!(dirs[0].entries, vec![Entry::file("gnusto.txt", 1)]);
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 3);
        assert_eq!(dirs[0].path(), Some("foo/apple"));
        assert_eq!(dirs[0].entries, vec![Entry::file("cleesh.txt", 2)]);
        assert_eq!(dirs[1].path(), Some("foo"));
        assert_eq!(
            dirs[1].entries,
            vec![Entry::dir("apple!banana"), Entry::dir("apple")]
        );
        assert_eq!(dirs[2].path(), None);
        assert_eq!(dirs[2].entries, vec![Entry::dir("foo")]);
    }

    #[test]
    fn preslash_file_then_toslash_file() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/bar/apple!banana.txt".parse::<KeyPath>().unwrap(), 1),
            Ok(Vec::new())
        );
        let e = tracker
            .add(&"foo/bar/apple".parse::<KeyPath>().unwrap(), 2)
            .unwrap_err();
        assert_eq!(
            e,
            TreeTrackerError::Unsorted {
                before: "foo/bar/apple!banana.txt".into(),
                after: "foo/bar/apple".into()
            }
        );
    }

    #[test]
    fn tostash_file_then_preslash_file() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/bar/apple".parse::<KeyPath>().unwrap(), 1),
            Ok(Vec::new())
        );
        assert_eq!(
            tracker.add(&"foo/bar/apple!banana.txt".parse::<KeyPath>().unwrap(), 2),
            Ok(Vec::new())
        );
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 3);
        assert_eq!(dirs[0].path(), Some("foo/bar"));
        assert_eq!(
            dirs[0].entries,
            vec![Entry::file("apple", 1), Entry::file("apple!banana.txt", 2)]
        );
        assert_eq!(dirs[1].path(), Some("foo"));
        assert_eq!(dirs[1].entries, vec![Entry::dir("bar")]);
        assert_eq!(dirs[2].path(), None);
        assert_eq!(dirs[2].entries, vec![Entry::dir("foo")]);
    }

    #[test]
    fn preslash_dir_then_toslash_file() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(
                &"foo/apple!banana/gnusto.txt".parse::<KeyPath>().unwrap(),
                1,
            ),
            Ok(Vec::new())
        );
        let e = tracker
            .add(&"foo/apple".parse::<KeyPath>().unwrap(), 2)
            .unwrap_err();
        assert_eq!(
            e,
            TreeTrackerError::Unsorted {
                before: "foo/apple!banana/gnusto.txt".into(),
                after: "foo/apple".into()
            }
        );
    }

    #[test]
    fn preslash_file_then_toslash_dir() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/bar/apple!banana.txt".parse::<KeyPath>().unwrap(), 1),
            Ok(Vec::new())
        );
        assert_eq!(
            tracker.add(&"foo/bar/apple/apricot.txt".parse::<KeyPath>().unwrap(), 2),
            Ok(Vec::new())
        );
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 4);
        assert_eq!(dirs[0].path(), Some("foo/bar/apple"));
        assert_eq!(dirs[0].entries, vec![Entry::file("apricot.txt", 2)]);
        assert_eq!(dirs[1].path(), Some("foo/bar"));
        assert_eq!(
            dirs[1].entries,
            vec![Entry::file("apple!banana.txt", 1), Entry::dir("apple")]
        );
        assert_eq!(dirs[2].path(), Some("foo"));
        assert_eq!(dirs[2].entries, vec![Entry::dir("bar")]);
        assert_eq!(dirs[3].path(), None);
        assert_eq!(dirs[3].entries, vec![Entry::dir("foo")]);
    }

    #[test]
    fn path_conflict_file_then_dir() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/bar".parse::<KeyPath>().unwrap(), 1),
            Ok(Vec::new())
        );
        let e = tracker
            .add(&"foo/bar/apple.txt".parse::<KeyPath>().unwrap(), 2)
            .unwrap_err();
        assert_eq!(e, TreeTrackerError::Conflict("foo/bar".into()));
    }

    #[test]
    fn just_finish() {
        let tracker = TreeTracker::<()>::new();
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path(), None);
        assert!(dirs[0].entries.is_empty());
    }

    #[test]
    fn multidir_finish() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(
                &"apple/banana/coconut/date.txt".parse::<KeyPath>().unwrap(),
                1
            ),
            Ok(Vec::new())
        );
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 4);
        assert_eq!(dirs[0].path(), Some("apple/banana/coconut"));
        assert_eq!(dirs[0].entries, vec![Entry::file("date.txt", 1)]);
        assert_eq!(dirs[1].path(), Some("apple/banana"));
        assert_eq!(dirs[1].entries, vec![Entry::dir("coconut")]);
        assert_eq!(dirs[2].path(), Some("apple"));
        assert_eq!(dirs[2].entries, vec![Entry::dir("banana")]);
        assert_eq!(dirs[3].path(), None);
        assert_eq!(dirs[3].entries, vec![Entry::dir("apple")]);
    }

    #[test]
    fn closedir_then_files_in_parent() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"apple/banana/coconut.txt".parse::<KeyPath>().unwrap(), 1),
            Ok(Vec::new())
        );
        let dirs = tracker
            .add(&"apple/kumquat.txt".parse::<KeyPath>().unwrap(), 2)
            .unwrap();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path(), Some("apple/banana"));
        assert_eq!(dirs[0].entries, vec![Entry::file("coconut.txt", 1)]);
        assert_eq!(
            tracker.add(&"apple/mango.txt".parse::<KeyPath>().unwrap(), 3),
            Ok(Vec::new())
        );
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 2);
        assert_eq!(dirs[0].path(), Some("apple"));
        assert_eq!(
            dirs[0].entries,
            vec![
                Entry::dir("banana"),
                Entry::file("kumquat.txt", 2),
                Entry::file("mango.txt", 3),
            ]
        );
        assert_eq!(dirs[1].path(), None);
        assert_eq!(dirs[1].entries, vec![Entry::dir("apple")]);
    }

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
}

// TESTS TO ADD:
// - second path closes multiple directories
// - close multiple directories down to root
// - close a subdirectory, then start a new directory in its parent
// - close a directory in the root, continue on
// - mix of files & directories in a directory
