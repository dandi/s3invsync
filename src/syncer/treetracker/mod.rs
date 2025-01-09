mod inner;
use self::inner::*;
use crate::keypath::KeyPath;
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
        fn after_error(key: &KeyPath, mut e: TreeTrackerError) -> TreeTrackerError {
            if let TreeTrackerError::Unsorted { ref mut after, .. } = e {
                *after = key.into();
            }
            e
        }
        let mut popped_dirs = Vec::new();
        let mut partiter = KeyComponents::new(key, value);
        while let Some((i, part)) = partiter.next() {
            let Some(pd) = self.0.get_mut(i) else {
                unreachable!(
                    "TreeTracker::add() iteration should not go past the end of the stack"
                );
            };
            let cmp_name = part.cmp_name();
            match part {
                Component::File(name, value) => {
                    match (pd.last_entry_is_dir(), pd.cmp_vs_last_entry(cmp_name)) {
                        (in_dir, Some(Ordering::Greater)) => {
                            if in_dir {
                                // Close current dirs
                                for _ in (i + 1)..(self.0.len()) {
                                    popped_dirs.push(self.pop());
                                }
                            }
                            self.push_file(name, value)
                                .map_err(|e| after_error(key, e))?;
                            break;
                        }
                        (true, Some(Ordering::Equal)) => {
                            return Err(TreeTrackerError::Conflict(self.last_key()));
                        }
                        (false, Some(Ordering::Equal)) => {
                            // XXX: Change this when support for old filenames is
                            //      added:
                            return Err(TreeTrackerError::DuplicateFile(key.into()));
                        }
                        (_, Some(Ordering::Less)) => {
                            return Err(TreeTrackerError::Unsorted {
                                before: self.last_key(),
                                after: key.into(),
                            });
                        }
                        (_, None) => {
                            assert!(
                                self.is_empty(),
                                "top dir of TreeTracker should be root when empty"
                            );
                            self.push_file(name, value)
                                .map_err(|e| after_error(key, e))?;
                            break;
                        }
                    }
                }
                Component::Dir(name) => {
                    match (pd.last_entry_is_dir(), pd.cmp_vs_last_entry(cmp_name)) {
                        (in_dir, Some(Ordering::Greater)) => {
                            if in_dir {
                                // Close current dirs
                                for _ in (i + 1)..(self.0.len()) {
                                    popped_dirs.push(self.pop());
                                }
                            }
                            self.push_parts(name, partiter)
                                .map_err(|e| after_error(key, e))?;
                            break;
                        }
                        (true, Some(Ordering::Equal)) => continue,
                        (false, Some(Ordering::Equal)) => {
                            return Err(TreeTrackerError::Conflict(self.last_key()));
                        }
                        (_, Some(Ordering::Less)) => {
                            return Err(TreeTrackerError::Unsorted {
                                before: self.last_key(),
                                after: key.into(),
                            });
                        }
                        (_, None) => {
                            assert!(
                                self.is_empty(),
                                "top dir of TreeTracker should be root when empty"
                            );
                            self.push_parts(name, partiter)
                                .map_err(|e| after_error(key, e))?;
                            break;
                        }
                    }
                }
            }
        }
        Ok(popped_dirs)
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

    fn push_parts(
        &mut self,
        first_dirname: &str,
        rest: KeyComponents<'_, T>,
    ) -> Result<(), TreeTrackerError> {
        self.push_dir(first_dirname);
        for (_, part) in rest {
            match part {
                Component::Dir(name) => self.push_dir(name),
                Component::File(name, value) => self.push_file(name, value)?,
            }
        }
        Ok(())
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

    fn push_file(&mut self, name: &str, value: T) -> Result<(), TreeTrackerError> {
        let Some(pd) = self.0.last_mut() else {
            panic!("TreeTracker::push_file() called on void tracker");
        };
        assert!(
            pd.current_subdir.is_none(),
            "TreeTracker::push_file() called when top dir has subdir"
        );
        if let Some(en) = pd.entries.last() {
            match CmpName::File(name).cmp(&en.cmp_name()) {
                Ordering::Equal => return Err(TreeTrackerError::DuplicateFile(self.last_key())),
                // IMPORTANT: The `after` needs to be replaced with the full path in the
                // calling context:
                Ordering::Less => {
                    return Err(TreeTrackerError::Unsorted {
                        before: self.last_key(),
                        after: name.into(),
                    })
                }
                Ordering::Greater => (),
            }
        }
        pd.entries.push(Entry::file(name, value));
        Ok(())
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

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(super) enum TreeTrackerError {
    #[error("received keys in unsorted order: {before:?} came before {after:?}")]
    Unsorted { before: String, after: String },
    #[error("path {0:?} is used as both a file and a directory")]
    Conflict(String),
    #[error("file key {0:?} encountered more than once")]
    DuplicateFile(String),
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
}

// TESTS TO ADD:
// - second path closes multiple directories
// - close multiple directories down to root
// - close a subdirectory, then start a new directory in its parent
// - close a directory in the root, continue on
// - mix of files & directories in a directory
// - file in root dir (with & without preceding entries)
// - KeyComponents
