use crate::inventory::FileSchema;
use serde::Deserialize;
use thiserror::Error;

/// A listing of CSV inventory files from a manifest
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(try_from = "RawManifest")]
pub(crate) struct CsvManifest {
    pub(crate) files: Vec<FileSpec>,
}

impl TryFrom<RawManifest> for CsvManifest {
    type Error = ManifestError;

    fn try_from(value: RawManifest) -> Result<CsvManifest, ManifestError> {
        if value.file_format != FileFormat::Csv {
            Err(ManifestError::Format(value.file_format))
        } else {
            let files = value
                .files
                .into_iter()
                .map(|spec| FileSpec {
                    key: spec.key,
                    size: spec.size,
                    md5_checksum: spec.md5_checksum,
                    file_schema: value.file_schema.clone(),
                })
                .collect();
            Ok(CsvManifest { files })
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
}

/// Parsed `manifest.json` file
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct RawManifest {
    //source_bucket: String,
    //destination_bucket: String,
    //version: String,
    //creation_timestamp: String,
    file_format: FileFormat,
    file_schema: FileSchema,
    files: Vec<RawFileSpec>,
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FileSpec {
    /// S3 object key of the inventory list file
    pub(crate) key: String,

    /// Size of the inventory list file
    pub(crate) size: i64,

    /// MD5 digest of the inventory list file
    pub(crate) md5_checksum: String,

    /// The fields used by the inventory list file
    pub(crate) file_schema: FileSchema,
}

/// An entry in a manifest's "files" list pointing to an inventory list file,
/// as deserialized directly from a manifest
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct RawFileSpec {
    /// S3 object key of the inventory list file
    pub(crate) key: String,

    /// Size of the inventory list file
    pub(crate) size: i64,

    /// MD5 digest of the inventory list file
    #[serde(rename = "MD5checksum")]
    pub(crate) md5_checksum: String,
}
