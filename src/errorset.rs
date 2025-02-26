use crate::s3::DownloadError;
use std::fmt;
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
    pub(crate) fn download_error_to_warning(
        &self,
        e: &DownloadError,
        is_old_version: bool,
    ) -> Option<DownloadWarning> {
        let DownloadError::Get(ref ge) = e else {
            return None;
        };
        if ge.is_404() && self.missing_old_version && is_old_version {
            Some(DownloadWarning::MissingOldVersion)
        } else if ge.is_403() && self.access_denied {
            Some(DownloadWarning::AccessDenied)
        } else if ge.is_invalid_object_state() && self.invalid_object_state {
            Some(DownloadWarning::InvalidObjectState)
        } else {
            None
        }
    }

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DownloadWarning {
    AccessDenied,
    InvalidObjectState,
    MissingOldVersion,
}

impl fmt::Display for DownloadWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DownloadWarning::AccessDenied => write!(f, "access to object denied"),
            DownloadWarning::InvalidObjectState => write!(f, "invalid object state"),
            DownloadWarning::MissingOldVersion => write!(f, "old version of object not found"),
        }
    }
}
