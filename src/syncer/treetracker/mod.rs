mod inner;
use self::inner::*;
use crate::keypath::KeyPath;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
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
        value: T,
        old_filename: Option<String>,
    ) -> Result<Vec<Directory<T>>, TreeTrackerError> {
        let mut popped_dirs = Vec::new();
        let mut partiter = KeyComponents::new(key, value, old_filename);
        while let Some((i, part)) = partiter.next() {
            let Some(pd) = self.0.get_mut(i) else {
                unreachable!(
                    "TreeTracker::add() iteration should not go past the end of the stack"
                );
            };
            let cmp_name = part.cmp_name();
            match part {
                Component::File(name, value, old_filename) => {
                    match (pd.last_entry_is_dir(), pd.cmp_vs_last_entry(cmp_name)) {
                        (in_dir, Some(Ordering::Greater)) => {
                            if in_dir {
                                // Close current dirs
                                for _ in (i + 1)..(self.0.len()) {
                                    popped_dirs.push(self.pop());
                                }
                            }
                            self.push_file(name, value, old_filename)?;
                            break;
                        }
                        (true, Some(Ordering::Equal)) => {
                            return Err(TreeTrackerError::Conflict(key.into()));
                        }
                        (false, Some(Ordering::Equal)) => {
                            self.push_file(name, value, old_filename)?;
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
                            self.push_file(name, value, old_filename)?;
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
                            self.push_parts(name, partiter)?;
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
                            self.push_parts(name, partiter)?;
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
                Component::File(name, value, old_filename) => {
                    self.push_file(name, value, old_filename)?;
                }
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

    fn push_file(
        &mut self,
        name: &str,
        value: T,
        old_filename: Option<String>,
    ) -> Result<(), TreeTrackerError> {
        let Some(pd) = self.0.last_mut() else {
            panic!("TreeTracker::push_file() called on void tracker");
        };
        assert!(
            pd.current_subdir.is_none(),
            "TreeTracker::push_file() called when top dir has subdir"
        );
        if let Some(en) = pd.entries.last_mut() {
            match CmpName::File(name).cmp(&en.cmp_name()) {
                Ordering::Less => {
                    panic!("TreeTracker::push_file() called with filename less than previous name")
                }
                Ordering::Equal => {
                    let Entry::File {
                        old_filenames,
                        value: envalue,
                        ..
                    } = en
                    else {
                        panic!("TreeTracker::push_file() called with filename equal to previous name and previous is not a file");
                    };
                    if let Some(of) = old_filename {
                        if old_filenames.insert(of.clone(), value).is_some() {
                            return Err(TreeTrackerError::DuplicateOldFile {
                                key: self.last_key(),
                                old_filename: of,
                            });
                        }
                    } else if envalue.is_none() {
                        *envalue = Some(value);
                    } else {
                        return Err(TreeTrackerError::DuplicateFile(self.last_key()));
                    }
                }
                Ordering::Greater => pd.entries.push(Entry::file(name, value, old_filename)),
            }
        } else {
            pd.entries.push(Entry::file(name, value, old_filename));
        }
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
        let mut files = HashMap::new();
        let mut directories = HashSet::new();
        for en in entries {
            match en {
                Entry::File {
                    name,
                    value,
                    old_filenames,
                } => {
                    if let Some(value) = value {
                        files.insert(name, value);
                    }
                    files.extend(old_filenames);
                }
                Entry::Dir { name } => {
                    directories.insert(name);
                }
            }
        }
        Directory {
            path,
            files,
            directories,
        }
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
    path: Option<String>, // `None` for the root
    files: HashMap<String, T>,
    directories: HashSet<String>,
}

impl<T> Directory<T> {
    pub(super) fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    pub(super) fn contains_file(&self, name: &str) -> bool {
        self.files.contains_key(name)
    }

    pub(super) fn contains_dir(&self, name: &str) -> bool {
        self.directories.contains(name)
    }

    #[allow(dead_code)]
    pub(super) fn map<U, F: FnMut(T) -> U>(self, mut f: F) -> Directory<U> {
        Directory {
            path: self.path,
            files: self
                .files
                .into_iter()
                .map(|(name, value)| (name, f(value)))
                .collect(),
            directories: self.directories,
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
    #[error("key {key:?} has multiple non-latest versions with filename {old_filename:?}")]
    DuplicateOldFile { key: String, old_filename: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_dir() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/bar.txt".parse::<KeyPath>().unwrap(), 1, None),
            Ok(Vec::new())
        );
        assert_eq!(
            tracker.add(&"foo/quux.txt".parse::<KeyPath>().unwrap(), 2, None),
            Ok(Vec::new())
        );
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 2);
        assert_eq!(dirs[0].path(), Some("foo"));
        assert_eq!(
            dirs[0].files,
            HashMap::from([("bar.txt".into(), 1), ("quux.txt".into(), 2),])
        );
        assert!(dirs[0].directories.is_empty());
        assert_eq!(dirs[1].path(), None);
        assert!(dirs[1].files.is_empty());
        assert_eq!(dirs[1].directories, HashSet::from(["foo".into()]));
    }

    #[test]
    fn different_dir() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/bar.txt".parse::<KeyPath>().unwrap(), 1, None),
            Ok(Vec::new())
        );
        let dirs = tracker
            .add(&"glarch/quux.txt".parse::<KeyPath>().unwrap(), 2, None)
            .unwrap();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path(), Some("foo"));
        assert_eq!(dirs[0].files, HashMap::from([("bar.txt".into(), 1)]));
        assert!(dirs[0].directories.is_empty());
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 2);
        assert_eq!(dirs[0].path(), Some("glarch"));
        assert_eq!(dirs[0].files, HashMap::from([("quux.txt".into(), 2)]));
        assert!(dirs[0].directories.is_empty());
        assert_eq!(dirs[1].path(), None);
        assert!(dirs[1].files.is_empty());
        assert_eq!(
            dirs[1].directories,
            HashSet::from(["foo".into(), "glarch".into()])
        );
    }

    #[test]
    fn different_subdir() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/bar/apple.txt".parse::<KeyPath>().unwrap(), 1, None),
            Ok(Vec::new())
        );
        let dirs = tracker
            .add(&"foo/quux/banana.txt".parse::<KeyPath>().unwrap(), 2, None)
            .unwrap();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path(), Some("foo/bar"));
        assert_eq!(dirs[0].files, HashMap::from([("apple.txt".into(), 1)]));
        assert!(dirs[0].directories.is_empty());
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 3);
        assert_eq!(dirs[0].path(), Some("foo/quux"));
        assert_eq!(dirs[0].files, HashMap::from([("banana.txt".into(), 2)]));
        assert!(dirs[0].directories.is_empty());
        assert_eq!(dirs[1].path(), Some("foo"));
        assert!(dirs[1].files.is_empty());
        assert_eq!(
            dirs[1].directories,
            HashSet::from(["bar".into(), "quux".into()])
        );
        assert_eq!(dirs[2].path(), None);
        assert!(dirs[2].files.is_empty());
        assert_eq!(dirs[2].directories, HashSet::from(["foo".into()]));
    }

    #[test]
    fn preslash_dir_then_toslash_dir() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(
                &"foo/apple!banana/gnusto.txt".parse::<KeyPath>().unwrap(),
                1,
                None
            ),
            Ok(Vec::new())
        );
        let dirs = tracker
            .add(&"foo/apple/cleesh.txt".parse::<KeyPath>().unwrap(), 2, None)
            .unwrap();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path(), Some("foo/apple!banana"));
        assert_eq!(dirs[0].files, HashMap::from([("gnusto.txt".into(), 1)]));
        assert!(dirs[0].directories.is_empty());
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 3);
        assert_eq!(dirs[0].path(), Some("foo/apple"));
        assert_eq!(dirs[0].files, HashMap::from([("cleesh.txt".into(), 2)]));
        assert!(dirs[0].directories.is_empty());
        assert_eq!(dirs[1].path(), Some("foo"));
        assert!(dirs[1].files.is_empty());
        assert_eq!(
            dirs[1].directories,
            HashSet::from(["apple!banana".into(), "apple".into()])
        );
        assert_eq!(dirs[2].path(), None);
        assert!(dirs[1].files.is_empty());
        assert_eq!(dirs[2].directories, HashSet::from([("foo".into())]));
    }

    #[test]
    fn preslash_file_then_toslash_file() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(
                &"foo/bar/apple!banana.txt".parse::<KeyPath>().unwrap(),
                1,
                None
            ),
            Ok(Vec::new())
        );
        let e = tracker
            .add(&"foo/bar/apple".parse::<KeyPath>().unwrap(), 2, None)
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
            tracker.add(&"foo/bar/apple".parse::<KeyPath>().unwrap(), 1, None),
            Ok(Vec::new())
        );
        assert_eq!(
            tracker.add(
                &"foo/bar/apple!banana.txt".parse::<KeyPath>().unwrap(),
                2,
                None
            ),
            Ok(Vec::new())
        );
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 3);
        assert_eq!(dirs[0].path(), Some("foo/bar"));
        assert_eq!(
            dirs[0].files,
            HashMap::from([("apple".into(), 1), ("apple!banana.txt".into(), 2)])
        );
        assert!(dirs[0].directories.is_empty());
        assert_eq!(dirs[1].path(), Some("foo"));
        assert!(dirs[1].files.is_empty());
        assert_eq!(dirs[1].directories, HashSet::from(["bar".into()]));
        assert_eq!(dirs[2].path(), None);
        assert!(dirs[2].files.is_empty());
        assert_eq!(dirs[2].directories, HashSet::from(["foo".into()]));
    }

    #[test]
    fn preslash_dir_then_toslash_file() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(
                &"foo/apple!banana/gnusto.txt".parse::<KeyPath>().unwrap(),
                1,
                None,
            ),
            Ok(Vec::new())
        );
        let e = tracker
            .add(&"foo/apple".parse::<KeyPath>().unwrap(), 2, None)
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
            tracker.add(
                &"foo/bar/apple!banana.txt".parse::<KeyPath>().unwrap(),
                1,
                None
            ),
            Ok(Vec::new())
        );
        assert_eq!(
            tracker.add(
                &"foo/bar/apple/apricot.txt".parse::<KeyPath>().unwrap(),
                2,
                None
            ),
            Ok(Vec::new())
        );
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 4);
        assert_eq!(dirs[0].path(), Some("foo/bar/apple"));
        assert_eq!(dirs[0].files, HashMap::from([("apricot.txt".into(), 2)]));
        assert!(dirs[0].directories.is_empty());
        assert_eq!(dirs[1].path(), Some("foo/bar"));
        assert_eq!(
            dirs[1].files,
            HashMap::from([("apple!banana.txt".into(), 1)])
        );
        assert_eq!(dirs[1].directories, HashSet::from(["apple".into()]));
        assert_eq!(dirs[2].path(), Some("foo"));
        assert!(dirs[2].files.is_empty());
        assert_eq!(dirs[2].directories, HashSet::from(["bar".into()]));
        assert_eq!(dirs[3].path(), None);
        assert!(dirs[3].files.is_empty());
        assert_eq!(dirs[3].directories, HashSet::from(["foo".into()]));
    }

    #[test]
    fn path_conflict_file_then_dir() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/bar".parse::<KeyPath>().unwrap(), 1, None),
            Ok(Vec::new())
        );
        let e = tracker
            .add(&"foo/bar/apple.txt".parse::<KeyPath>().unwrap(), 2, None)
            .unwrap_err();
        assert_eq!(e, TreeTrackerError::Conflict("foo/bar".into()));
    }

    #[test]
    fn path_conflict_dir_then_file() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/bar/quux.txt".parse::<KeyPath>().unwrap(), 1, None),
            Ok(Vec::new())
        );
        let e = tracker
            .add(&"foo/bar".parse::<KeyPath>().unwrap(), 2, None)
            .unwrap_err();
        assert_eq!(e, TreeTrackerError::Conflict("foo/bar".into()));
    }

    #[test]
    fn just_finish() {
        let tracker = TreeTracker::<()>::new();
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path(), None);
        assert!(dirs[0].files.is_empty());
        assert!(dirs[0].directories.is_empty());
    }

    #[test]
    fn multidir_finish() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(
                &"apple/banana/coconut/date.txt".parse::<KeyPath>().unwrap(),
                1,
                None
            ),
            Ok(Vec::new())
        );
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 4);
        assert_eq!(dirs[0].path(), Some("apple/banana/coconut"));
        assert_eq!(dirs[0].files, HashMap::from([("date.txt".into(), 1)]));
        assert!(dirs[0].directories.is_empty());
        assert_eq!(dirs[1].path(), Some("apple/banana"));
        assert!(dirs[1].files.is_empty());
        assert_eq!(dirs[1].directories, HashSet::from(["coconut".into()]));
        assert_eq!(dirs[2].path(), Some("apple"));
        assert!(dirs[2].files.is_empty());
        assert_eq!(dirs[2].directories, HashSet::from(["banana".into()]));
        assert_eq!(dirs[3].path(), None);
        assert!(dirs[3].files.is_empty());
        assert_eq!(dirs[3].directories, HashSet::from(["apple".into()]));
    }

    #[test]
    fn closedir_then_files_in_parent() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(
                &"apple/banana/coconut.txt".parse::<KeyPath>().unwrap(),
                1,
                None
            ),
            Ok(Vec::new())
        );
        let dirs = tracker
            .add(&"apple/kumquat.txt".parse::<KeyPath>().unwrap(), 2, None)
            .unwrap();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path(), Some("apple/banana"));
        assert_eq!(dirs[0].files, HashMap::from([("coconut.txt".into(), 1)]));
        assert!(dirs[0].directories.is_empty());

        assert_eq!(
            tracker.add(&"apple/mango.txt".parse::<KeyPath>().unwrap(), 3, None),
            Ok(Vec::new())
        );
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 2);
        assert_eq!(dirs[0].path(), Some("apple"));
        assert_eq!(
            dirs[0].files,
            HashMap::from([("kumquat.txt".into(), 2), ("mango.txt".into(), 3)])
        );
        assert_eq!(dirs[0].directories, HashSet::from(["banana".into()]));
        assert_eq!(dirs[1].path(), None);
        assert!(dirs[1].files.is_empty());
        assert_eq!(dirs[1].directories, HashSet::from(["apple".into()]));
    }

    #[test]
    fn closedir_then_dirs_in_parent() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(
                &"apple/banana/coconut.txt".parse::<KeyPath>().unwrap(),
                1,
                None
            ),
            Ok(Vec::new())
        );
        let dirs = tracker
            .add(
                &"apple/eggplant/kumquat.txt".parse::<KeyPath>().unwrap(),
                2,
                None,
            )
            .unwrap();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path(), Some("apple/banana"));
        assert_eq!(dirs[0].files, HashMap::from([("coconut.txt".into(), 1)]));
        assert!(dirs[0].directories.is_empty());
        let dirs = tracker
            .add(
                &"apple/mango/tangerine.txt".parse::<KeyPath>().unwrap(),
                3,
                None,
            )
            .unwrap();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path(), Some("apple/eggplant"));
        assert_eq!(dirs[0].files, HashMap::from([("kumquat.txt".into(), 2)]));
        assert!(dirs[0].directories.is_empty());
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 3);
        assert_eq!(dirs[0].path(), Some("apple/mango"));
        assert_eq!(dirs[0].files, HashMap::from([("tangerine.txt".into(), 3)]));
        assert!(dirs[0].directories.is_empty());
        assert_eq!(dirs[1].path(), Some("apple"));
        assert!(dirs[1].files.is_empty());
        assert_eq!(
            dirs[1].directories,
            HashSet::from(["banana".into(), "eggplant".into(), "mango".into()])
        );
        assert_eq!(dirs[2].path(), None);
        assert!(dirs[1].files.is_empty());
        assert_eq!(dirs[2].directories, HashSet::from(["apple".into()]));
    }

    #[test]
    fn close_multiple_dirs() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(
                &"apple/banana/coconut/date.txt".parse::<KeyPath>().unwrap(),
                1,
                None
            ),
            Ok(Vec::new())
        );
        let dirs = tracker
            .add(&"foo.txt".parse::<KeyPath>().unwrap(), 2, None)
            .unwrap();
        assert_eq!(dirs.len(), 3);
        assert_eq!(dirs[0].path(), Some("apple/banana/coconut"));
        assert_eq!(dirs[0].files, HashMap::from([("date.txt".into(), 1)]));
        assert!(dirs[0].directories.is_empty());
        assert_eq!(dirs[1].path(), Some("apple/banana"));
        assert!(dirs[1].files.is_empty());
        assert_eq!(dirs[1].directories, HashSet::from(["coconut".into()]));
        assert_eq!(dirs[2].path(), Some("apple"));
        assert!(dirs[2].files.is_empty());
        assert_eq!(dirs[2].directories, HashSet::from(["banana".into()]));
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path(), None);
        assert_eq!(dirs[0].files, HashMap::from([("foo.txt".into(), 2)]));
        assert_eq!(dirs[0].directories, HashSet::from(["apple".into()]));
    }

    #[test]
    fn same_file_twice() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/bar/quux.txt".parse::<KeyPath>().unwrap(), 1, None),
            Ok(Vec::new())
        );
        let e = tracker
            .add(&"foo/bar/quux.txt".parse::<KeyPath>().unwrap(), 2, None)
            .unwrap_err();
        assert_eq!(
            e,
            TreeTrackerError::DuplicateFile("foo/bar/quux.txt".into())
        );
    }

    #[test]
    fn unsorted_parent_dirs() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/gnusto/quux.txt".parse::<KeyPath>().unwrap(), 1, None),
            Ok(Vec::new())
        );
        let e = tracker
            .add(&"foo/bar/cleesh.txt".parse::<KeyPath>().unwrap(), 2, None)
            .unwrap_err();
        assert_eq!(
            e,
            TreeTrackerError::Unsorted {
                before: "foo/gnusto/quux.txt".into(),
                after: "foo/bar/cleesh.txt".into()
            }
        );
    }

    #[test]
    fn file_then_preceding_dir() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/gnusto.txt".parse::<KeyPath>().unwrap(), 1, None),
            Ok(Vec::new())
        );
        let e = tracker
            .add(&"foo/bar/cleesh.txt".parse::<KeyPath>().unwrap(), 2, None)
            .unwrap_err();
        assert_eq!(
            e,
            TreeTrackerError::Unsorted {
                before: "foo/gnusto.txt".into(),
                after: "foo/bar/cleesh.txt".into()
            }
        );
    }

    #[test]
    fn files_in_root() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo.txt".parse::<KeyPath>().unwrap(), 1, None),
            Ok(Vec::new())
        );
        assert_eq!(
            tracker.add(&"gnusto/cleesh.txt".parse::<KeyPath>().unwrap(), 2, None),
            Ok(Vec::new())
        );
        let dirs = tracker
            .add(&"quux.txt".parse::<KeyPath>().unwrap(), 3, None)
            .unwrap();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path(), Some("gnusto"));
        assert_eq!(dirs[0].files, HashMap::from([("cleesh.txt".into(), 2)]));
        assert!(dirs[0].directories.is_empty());
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].path(), None);
        assert_eq!(
            dirs[0].files,
            HashMap::from([("foo.txt".into(), 1), ("quux.txt".into(), 3)])
        );
        assert_eq!(dirs[0].directories, HashSet::from(["gnusto".into()]));
    }

    #[test]
    fn old_filename() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(
                &"foo/bar.txt".parse::<KeyPath>().unwrap(),
                1,
                Some("bar.txt.old.1.2".into())
            ),
            Ok(Vec::new())
        );
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 2);
        assert_eq!(
            dirs[0],
            Directory {
                path: Some("foo".into()),
                files: HashMap::from([("bar.txt.old.1.2".into(), 1)]),
                directories: HashSet::new(),
            }
        );
        assert_eq!(
            dirs[1],
            Directory {
                path: None,
                files: HashMap::new(),
                directories: HashSet::from(["foo".into()]),
            }
        );
    }

    #[test]
    fn multiple_old_filenames() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(
                &"foo/bar.txt".parse::<KeyPath>().unwrap(),
                1,
                Some("bar.txt.old.a.b".into())
            ),
            Ok(Vec::new())
        );
        assert_eq!(
            tracker.add(
                &"foo/bar.txt".parse::<KeyPath>().unwrap(),
                2,
                Some("bar.txt.old.1.2".into())
            ),
            Ok(Vec::new())
        );
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 2);
        assert_eq!(
            dirs[0],
            Directory {
                path: Some("foo".into()),
                files: HashMap::from([
                    ("bar.txt.old.a.b".into(), 1),
                    ("bar.txt.old.1.2".into(), 2),
                ]),
                directories: HashSet::new(),
            }
        );
        assert_eq!(
            dirs[1],
            Directory {
                path: None,
                files: HashMap::new(),
                directories: HashSet::from(["foo".into()]),
            }
        );
    }

    #[test]
    fn old_and_non_old() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(
                &"foo/bar.txt".parse::<KeyPath>().unwrap(),
                1,
                Some("bar.txt.old.a.b".into())
            ),
            Ok(Vec::new())
        );
        assert_eq!(
            tracker.add(&"foo/bar.txt".parse::<KeyPath>().unwrap(), 2, None),
            Ok(Vec::new())
        );
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 2);
        assert_eq!(
            dirs[0],
            Directory {
                path: Some("foo".into()),
                files: HashMap::from([("bar.txt.old.a.b".into(), 1), ("bar.txt".into(), 2),]),
                directories: HashSet::new(),
            }
        );
        assert_eq!(
            dirs[1],
            Directory {
                path: None,
                files: HashMap::new(),
                directories: HashSet::from(["foo".into()]),
            }
        );
    }

    #[test]
    fn non_old_and_old() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(&"foo/bar.txt".parse::<KeyPath>().unwrap(), 1, None,),
            Ok(Vec::new())
        );
        assert_eq!(
            tracker.add(
                &"foo/bar.txt".parse::<KeyPath>().unwrap(),
                2,
                Some("bar.txt.old.a.b".into())
            ),
            Ok(Vec::new())
        );
        let dirs = tracker.finish();
        assert_eq!(dirs.len(), 2);
        assert_eq!(
            dirs[0],
            Directory {
                path: Some("foo".into()),
                files: HashMap::from([("bar.txt".into(), 1), ("bar.txt.old.a.b".into(), 2),]),
                directories: HashSet::new(),
            }
        );
        assert_eq!(
            dirs[1],
            Directory {
                path: None,
                files: HashMap::new(),
                directories: HashSet::from(["foo".into()]),
            }
        );
    }

    #[test]
    fn duplicate_old_filenames() {
        let mut tracker = TreeTracker::new();
        assert_eq!(
            tracker.add(
                &"foo/bar.txt".parse::<KeyPath>().unwrap(),
                1,
                Some("bar.txt.old.1.2".into())
            ),
            Ok(Vec::new())
        );
        let e = tracker
            .add(
                &"foo/bar.txt".parse::<KeyPath>().unwrap(),
                2,
                Some("bar.txt.old.1.2".into()),
            )
            .unwrap_err();
        assert_eq!(
            e,
            TreeTrackerError::DuplicateOldFile {
                key: "foo/bar.txt".into(),
                old_filename: "bar.txt.old.1.2".into()
            }
        );
    }
}
