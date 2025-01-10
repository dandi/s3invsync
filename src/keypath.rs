use crate::consts::METADATA_FILENAME;
use thiserror::Error;

/// A nonempty, forward-slash-separated path that does not contain any of the
/// following:
///
/// - a `.` or `..` component
/// - a leading or trailing forward slash
/// - two or more consecutive forward slashes
/// - NUL
/// - a component that equals [`METADATA_FILENAME`] or that looks like
///   `{filename}.old.{version_id}.{etag}` (specifically, of the form
///   `{nonempty}.old.{nonempty}.{nonempty}`)
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct KeyPath(String);

impl KeyPath {
    /// Return the filename portion of the path
    pub(crate) fn name(&self) -> &str {
        self.0
            .split('/')
            .next_back()
            .expect("path should be nonempty")
    }

    /// Split the path into the directory component (if any) and filename
    pub(crate) fn split(&self) -> (Option<&str>, &str) {
        match self.0.rsplit_once('/') {
            Some((pre, post)) => (Some(pre), post),
            None => (None, &*self.0),
        }
    }
}

impl From<KeyPath> for String {
    fn from(value: KeyPath) -> String {
        value.0
    }
}

impl From<&KeyPath> for String {
    fn from(value: &KeyPath) -> String {
        value.0.clone()
    }
}

impl std::fmt::Debug for KeyPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::fmt::Display for KeyPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialEq<str> for KeyPath {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl<'a> PartialEq<&'a str> for KeyPath {
    fn eq(&self, other: &&'a str) -> bool {
        &self.0 == other
    }
}

impl AsRef<str> for KeyPath {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl std::ops::Deref for KeyPath {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl std::str::FromStr for KeyPath {
    type Err = ParseKeyPathError;

    fn from_str(s: &str) -> Result<KeyPath, ParseKeyPathError> {
        match validate(s) {
            Ok(()) => Ok(KeyPath(s.into())),
            Err(e) => Err(e),
        }
    }
}

impl TryFrom<String> for KeyPath {
    type Error = KeyPathFromStringError;

    fn try_from(s: String) -> Result<KeyPath, Self::Error> {
        match validate(&s) {
            Ok(()) => Ok(KeyPath(s)),
            Err(source) => Err(KeyPathFromStringError { source, string: s }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ParseKeyPathError {
    #[error("paths cannot be empty")]
    Empty,
    #[error("paths cannot start with a forward slash")]
    StartsWithSlash,
    #[error("paths cannot end with a forward slash")]
    EndsWithSlash,
    #[error("paths cannot contain NUL")]
    Nul,
    #[error("path is not normalized")]
    NotNormalized,
    #[error("path contains component with special meaning")]
    Special,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{source}: {string:?}")]
pub(crate) struct KeyPathFromStringError {
    source: ParseKeyPathError,
    string: String,
}

fn validate(s: &str) -> Result<(), ParseKeyPathError> {
    if s.is_empty() {
        Err(ParseKeyPathError::Empty)
    } else if s.starts_with('/') {
        Err(ParseKeyPathError::StartsWithSlash)
    } else if s.ends_with('/') {
        Err(ParseKeyPathError::EndsWithSlash)
    } else if s.contains('\0') {
        Err(ParseKeyPathError::Nul)
    } else if s.split('/').any(|p| p.is_empty() || p == "." || p == "..") {
        Err(ParseKeyPathError::NotNormalized)
    } else if s.split('/').any(is_special_component) {
        Err(ParseKeyPathError::Special)
    } else {
        Ok(())
    }
}

// Test for components that equal `METADATA_FILENAME` or look like
// `{filename}.old.{version_id}.{etag}` (specifically, that are of the form
// `{nonempty}.old.{nonempty}.{nonempty}`)
pub(crate) fn is_special_component(component: &str) -> bool {
    if component == METADATA_FILENAME {
        return true;
    }
    if let Some(i) = component.find(".old.").filter(|&i| i > 0) {
        let post_old = &component[(i + 5)..];
        if post_old
            .find('.')
            .is_some_and(|j| (1..(post_old.len() - 1)).contains(&j))
        {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use rstest::rstest;

    #[rstest]
    #[case("foo.nwb")]
    #[case("foo/bar.nwb")]
    fn test_good_paths(#[case] s: &str) {
        let r = s.parse::<KeyPath>();
        assert_matches!(r, Ok(_));
    }

    #[rstest]
    #[case("")]
    #[case("/")]
    #[case("/foo")]
    #[case("foo/")]
    #[case("/foo/")]
    #[case("foo//bar.nwb")]
    #[case("foo///bar.nwb")]
    #[case("foo/bar\0.nwb")]
    #[case("foo/./bar.nwb")]
    #[case("foo/../bar.nwb")]
    #[case("./foo/bar.nwb")]
    #[case("../foo/bar.nwb")]
    #[case("foo/bar.nwb/.")]
    #[case("foo/bar.nwb/..")]
    fn test_bad_paths(#[case] s: &str) {
        let r = s.parse::<KeyPath>();
        assert_matches!(r, Err(_));
    }

    #[rstest]
    #[case("foo", false)]
    #[case("foo.old", false)]
    #[case("foo.old.bar", false)]
    #[case("foo.old.bar.baz", true)]
    #[case("foo.old.bar.baz.quux.glarch", true)]
    #[case("foo.old.bar.", false)]
    #[case(".old.bar.baz", false)]
    #[case("foo.old..baz", false)]
    #[case("foo.old..", false)]
    #[case(".s3invsync.versions.json", true)]
    fn test_is_special_component(#[case] s: &str, #[case] r: bool) {
        assert_eq!(is_special_component(s), r);
    }
}
