use thiserror::Error;

/// A set of flags denoting which types of errors should be regarded as
/// non-fatal during backup
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ErrorSet {
    /// If true, then a 403 error upon attempting to download an object is not
    /// fatal.
    pub(crate) access_denied: bool,

    /// If true, then an invalid entry in an inventory list file is not fatal.
    pub(crate) invalid_entry: bool,

    /// If true, then an `InvalidObjectState` error from S3 upon attempting to
    /// download an object is not fatal.
    pub(crate) invalid_object_state: bool,

    /// If true, then a 404 error upon attempting to download a non-latest
    /// version of a key is not fatal.
    pub(crate) missing_old_version: bool,
}

impl ErrorSet {
    fn all() -> ErrorSet {
        ErrorSet {
            access_denied: true,
            invalid_entry: true,
            invalid_object_state: true,
            missing_old_version: true,
        }
    }
}

impl std::str::FromStr for ErrorSet {
    type Err = ParseErrorSetError;

    fn from_str(s: &str) -> Result<ErrorSet, ParseErrorSetError> {
        let mut errset = ErrorSet::default();
        for word in s.split(',').map(str::trim) {
            match word {
                "access-denied" => errset.access_denied = true,
                "invalid-entry" => errset.invalid_entry = true,
                "invalid-object-state" => errset.invalid_object_state = true,
                "missing-old-version" => errset.missing_old_version = true,
                "all" => errset = ErrorSet::all(),
                s => return Err(ParseErrorSetError(s.to_owned())),
            }
        }
        Ok(errset)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid error type {0:?}")]
pub(crate) struct ParseErrorSetError(String);
