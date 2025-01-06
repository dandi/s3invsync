use std::fmt;
use std::str::FromStr;
use thiserror::Error;

/// The location of an object on S3
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct S3Location {
    /// The bucket name
    bucket: String,

    /// The key
    key: String,

    /// The version ID, if any
    version_id: Option<String>,
}

impl S3Location {
    /// Construct a new `S3Location` for the given bucket & key
    pub(crate) fn new(bucket: String, key: String) -> S3Location {
        S3Location {
            bucket,
            key,
            version_id: None,
        }
    }

    /// Returns the bucket name
    pub(crate) fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the key
    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    /// Returns the version ID, if any
    pub(crate) fn version_id(&self) -> Option<&str> {
        self.version_id.as_deref()
    }

    /// Return a new `S3Location` in which `suffix` is appended to the key,
    /// with a `'/'` inserted in between if one isn't already present.  The
    /// version ID is unset.
    pub(crate) fn join(&self, suffix: &str) -> S3Location {
        let mut joined = self.clone();
        joined.version_id = None;
        if !joined.key.ends_with('/') {
            joined.key.push('/');
        }
        joined.key.push_str(suffix);
        joined
    }

    /// Return a new `S3Location` with the same bucket and using the given key.
    /// The version ID is unset.
    pub(crate) fn with_key<S: Into<String>>(&self, key: S) -> S3Location {
        S3Location {
            bucket: self.bucket.clone(),
            key: key.into(),
            version_id: None,
        }
    }

    /// Return a new `S3Location` with the same bucket & key and using the
    /// given version ID.
    pub(crate) fn with_version_id<S: Into<String>>(&self, version_id: S) -> S3Location {
        S3Location {
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            version_id: Some(version_id.into()),
        }
    }
}

impl fmt::Display for S3Location {
    /// Format an `S3Location` as an S3 URL
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s3://{}/{}", self.bucket, self.key)?;
        if let Some(ref v) = self.version_id {
            write!(f, "?versionId={v}")?;
        }
        Ok(())
    }
}

impl FromStr for S3Location {
    type Err = S3LocationError;

    /// Parse an `S3Location` from an S3 URL.
    ///
    /// Version IDs in URLs are currently not supported.
    fn from_str(s: &str) -> Result<S3Location, S3LocationError> {
        // <https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html>
        fn is_bucket_char(c: char) -> bool {
            c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-'
        }

        let Some(s) = s.strip_prefix("s3://") else {
            return Err(S3LocationError::BadScheme);
        };
        let Some((bucket, key)) = s.split_once('/') else {
            return Err(S3LocationError::NoKey);
        };
        if bucket.is_empty() || !bucket.chars().all(is_bucket_char) {
            return Err(S3LocationError::BadBucket);
        }
        Ok(S3Location {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            version_id: None,
        })
    }
}

/// Error returned when parsing an invalid S3 URL
#[derive(Copy, Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum S3LocationError {
    /// The URL does not start with `"s3://"`
    #[error(r#"URL does not start with "s3://""#)]
    BadScheme,

    /// The URL does not contain a key
    #[error("URL does not contain an S3 object key")]
    NoKey,

    /// The bucket name is invalid
    #[error("invalid S3 bucket name")]
    BadBucket,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("s3://pail/", "pail", "")]
    #[case("s3://pail/index.html", "pail", "index.html")]
    #[case("s3://pail/dir/", "pail", "dir/")]
    #[case("s3://pail/dir/index.html", "pail", "dir/index.html")]
    #[case("s3://pail-of-water/dir/index.html", "pail-of-water", "dir/index.html")]
    fn parse_and_display(#[case] s: &str, #[case] bucket: &str, #[case] key: &str) {
        let loc = s.parse::<S3Location>().unwrap();
        assert_eq!(loc.bucket(), bucket);
        assert_eq!(loc.key(), key);
        assert_eq!(loc.to_string(), s);
    }

    #[rstest]
    #[case("https://dandiarchive.s3.amazonaws.com/zarr/bf47be1a-4fed-4105-bcb4-c52534a45b82/")]
    #[case("s3://pail")]
    #[case("s3:///index.html")]
    #[case("s3://user@pail/index.html")]
    #[case("pail/index.html")]
    #[case("S3://pail/index.html")]
    fn parse_err(#[case] s: &str) {
        assert!(s.parse::<S3Location>().is_err());
    }
}
