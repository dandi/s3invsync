mod streams;
use crate::manifest::CsvManifest;
use crate::timestamps::{Date, DateHM, DateMaybeHM};
use aws_sdk_s3::Client;
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
            None => self.get_latest_manifest_timestamp().await?,
            Some(DateMaybeHM::Date(d)) => self.get_latest_manifest_timestamp_within_date(d).await?,
            Some(DateMaybeHM::DateHM(d)) => d,
        };
        self.get_manifest(ts).await
    }

    #[allow(clippy::unused_async)] // XXX
    pub(crate) async fn get_latest_manifest_timestamp(&self) -> Result<DateHM, GetManifestError> {
        // Iterate over `DateHM` prefixes in `s3://{inv_bucket}/{inv_prefix}/`
        // and return greatest one
        todo!()
    }

    #[allow(clippy::unused_async)] // XXX
    pub(crate) async fn get_latest_manifest_timestamp_within_date(
        &self,
        when: Date,
    ) -> Result<DateHM, GetManifestError> {
        // Iterate over `DateHM` prefixes in
        // `s3://{inv_bucket}/{inv_prefix}/{when}T` and return greatest one
        todo!()
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
pub(crate) enum GetManifestError {
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
