use serde::Deserialize;
use thiserror::Error;

static EXPECTED_FILE_SCHEMA: &str = "Bucket, Key, VersionId, IsLatest, IsDeleteMarker, Size, LastModifiedDate, ETag, IsMultipartUploaded";

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

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum ManifestError {
    #[error("inventory files are in {0:?} format; only CSV is supported")]
    Format(FileFormat),
    #[error("inventory schema is unsupported {0:?}")]
    Schema(String),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Manifest {
    source_bucket: String,
    //destination_bucket: String,
    //version: String,
    //creation_timestamp: String,
    file_format: FileFormat,
    file_schema: String,
    files: Vec<FileSpec>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) enum FileFormat {
    #[serde(rename = "CSV")]
    Csv,
    #[serde(rename = "ORC")]
    Orc,
    #[serde(rename = "Parquet")]
    Parquet,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct FileSpec {
    pub(crate) key: String,
    pub(crate) size: i64,
    #[serde(rename = "MD5checksum")]
    pub(crate) md5_checksum: String,
}
