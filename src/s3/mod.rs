mod streams;
use self::streams::{ListManifestDates, ListObjectsError};
use crate::manifest::CsvManifest;
use crate::timestamps::{Date, DateHM, DateMaybeHM};
use aws_sdk_s3::Client;
use futures_util::TryStreamExt;
use thiserror::Error;

#[derive(Clone, Debug)]
pub(crate) struct S3Client {
    inner: Client,
    region: String,
    inv_bucket: String,
    inv_prefix: String,
}

impl S3Client {
    pub(crate) async fn get_manifest_for_date(
        &self,
        when: Option<DateMaybeHM>,
    ) -> Result<CsvManifest, GetManifestError> {
        let ts = match when {
            None => self.get_latest_manifest_timestamp(None).await?,
            Some(DateMaybeHM::Date(d)) => self.get_latest_manifest_timestamp(Some(d)).await?,
            Some(DateMaybeHM::DateHM(d)) => d,
        };
        self.get_manifest(ts).await
    }

    pub(crate) async fn get_latest_manifest_timestamp(
        &self,
        day: Option<Date>,
    ) -> Result<DateHM, FindManifestError> {
        // Iterate over `DateHM` prefixes in `s3://{inv_bucket}/{inv_prefix}/`
        // or `s3://{inv_bucket}/{inv_prefix}/{day}T` and return greatest one
        let key_prefix = match day {
            Some(d) => join_prefix(&self.inv_prefix, &format!("{d}T")),
            None => join_prefix(&self.inv_prefix, ""),
        };
        let mut stream = ListManifestDates::new(self, key_prefix.clone());
        let mut maxdate = None;
        while let Some(d) = stream.try_next().await? {
            match maxdate {
                None => maxdate = Some(d),
                Some(d0) if d0 < d => maxdate = Some(d),
                Some(_) => (),
            }
        }
        maxdate.ok_or_else(|| FindManifestError::NoMatch {
            bucket: self.inv_bucket.clone(),
            prefix: key_prefix,
        })
    }

    #[allow(clippy::unused_async)] // XXX
    pub(crate) async fn get_manifest(&self, when: DateHM) -> Result<CsvManifest, GetManifestError> {
        // Get S3 object
        // Stream to temp file while also feeding bytes into MD5 digester
        // Check digest
        // Parse JSON
        todo!()
    }
}

#[derive(Debug, Error)]
pub(crate) enum FindManifestError {
    #[error(transparent)]
    List(#[from] ListObjectsError),
    #[error("no manifests found in bucket {bucket:?} with prefix {prefix:?}")]
    NoMatch { bucket: String, prefix: String },
}

#[derive(Debug, Error)]
pub(crate) enum GetManifestError {
    #[error(transparent)]
    Find(#[from] FindManifestError),
    #[error("failed to download {url}")]
    Download {
        url: String,
        source: std::io::Error, // TODO: Change to actual error used by SDK
    },
    #[error("checksum verification for {url} failed; expected {expected_md5}, got {actual_md5}")]
    Verify {
        url: String,
        expected_md5: String,
        actual_md5: String,
    },
    #[error("failed to deserialize {url}")]
    Parse {
        url: String,
        source: serde_json::Error,
    },
}

fn join_prefix(prefix: &str, suffix: &str) -> String {
    let mut s = prefix.to_owned();
    if !s.ends_with('/') {
        s.push('/');
    }
    s.push_str(suffix);
    s
}
