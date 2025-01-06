use serde::Deserialize;
use thiserror::Error;

/// Currently, only manifests with this exact fileSchema value are supported.
static EXPECTED_FILE_SCHEMA: &str = "Bucket, Key, VersionId, IsLatest, IsDeleteMarker, Size, LastModifiedDate, ETag, IsMultipartUploaded";

/// A listing of CSV inventory files from a manifest
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(try_from = "Manifest")]
pub(crate) struct CsvManifest {
    pub(crate) files: Vec<FileSpec>,
}

impl TryFrom<Manifest> for CsvManifest {
    type Error = ManifestError;

    fn try_from(value: Manifest) -> Result<CsvManifest, ManifestError> {
        if value.file_format != FileFormat::Csv {
            Err(ManifestError::Format(value.file_format))
        } else if value.file_schema != EXPECTED_FILE_SCHEMA {
            Err(ManifestError::Schema(value.file_schema))
        } else {
            Ok(CsvManifest { files: value.files })
        }
    }
}

/// Error returned when a manifest file contains an unsupported feature
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum ManifestError {
    /// Returned when a manifest specifies an inventory list format other than
    /// CSV
    #[error("inventory files are in {0:?} format; only CSV is supported")]
    Format(FileFormat),

    /// Returned when a manifest's fileSchema is not the supported value
    #[error("inventory schema is unsupported {0:?}; expected {EXPECTED_FILE_SCHEMA:?}")]
    Schema(String),
}

/// Parsed `manifest.json` file
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct Manifest {
    //source_bucket: String,
    //destination_bucket: String,
    //version: String,
    //creation_timestamp: String,
    file_format: FileFormat,
    file_schema: String,
    files: Vec<FileSpec>,
}

/// The possible inventory list file formats
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) enum FileFormat {
    #[serde(rename = "CSV")]
    Csv,
    #[serde(rename = "ORC")]
    Orc,
    #[serde(rename = "Parquet")]
    Parquet,
}

/// An entry in a manifest's "files" list pointing to an inventory list file
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct FileSpec {
    /// S3 object key of the inventory list file
    pub(crate) key: String,

    /// Size of the inventory list file
    pub(crate) size: i64,

    /// MD5 digest of the inventory list file
    #[serde(rename = "MD5checksum")]
    pub(crate) md5_checksum: String,
}
