use super::manifest::CsvManifest;
use super::timestamps::DateHM;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Client {
    //inner: aws_sdk_s3 :: Client,
    region: String,
    inv_bucket: String,
    inv_prefix: String,
}

impl Client {
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
