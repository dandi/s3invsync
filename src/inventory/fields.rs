use super::item::{Directory, InventoryEntry, InventoryItem, ItemDetails};
use crate::keypath::{KeyPath, KeyPathFromStringError};
use serde::{
    de::{Deserializer, Unexpected},
    Deserialize,
};
use std::collections::HashSet;
use std::fmt;
use thiserror::Error;
use time::OffsetDateTime;

/// Fields that may be present in S3 Inventory list files
///
/// See
/// <https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html>
/// for more information on each field.
#[derive(Clone, Copy, Debug, strum::Display, strum::EnumString, Eq, Hash, PartialEq)]
pub(crate) enum InventoryField {
    Bucket,
    Key,
    VersionId,
    IsLatest,
    IsDeleteMarker,
    Size,
    LastModifiedDate,
    ETag,
    IsMultipartUploaded,
    StorageClass,
    ReplicationStatus,
    EncryptionStatus,
    ObjectLockRetainUntilDate,
    ObjectLockMode,
    ObjectLockLegalHoldStatus,
    IntelligentTieringAccessTier,
    BucketKeyStatus,
    ChecksumAlgorithm,
    ObjectAccessControlList,
    ObjectOwner,
}

impl InventoryField {
    /// `s3invsync` requires these fields to be present in every inventory list
    /// file.
    // IMPORTANT: If a field is ever removed from this list, the corresponding
    // `if Some(field) = field else { unreachable!() };` statement in
    // `FileSchema::parse_csv_fields()` must be removed as well.
    const REQUIRED: [InventoryField; 3] = [
        InventoryField::Bucket,
        InventoryField::Key,
        InventoryField::ETag,
    ];
}

/// A list of [`InventoryField`]s used by an inventory list file
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FileSchema {
    /// The fields, in order of appearance
    fields: Vec<InventoryField>,

    /// The index in `fields` at which `InventoryField::Key` is located (for
    /// convenience)
    key_index: usize,
}

impl FileSchema {
    /// Given a row of strings from an inventory list CSV file, parse them into
    /// an [`InventoryEntry`] according to the file schema
    pub(crate) fn parse_csv_fields(
        &self,
        values: Vec<String>,
    ) -> Result<InventoryEntry, ParseEntryError> {
        let Some(key) = values.get(self.key_index) else {
            return Err(ParseEntryError::NoKey);
        };
        let key = percent_encoding::percent_decode_str(key)
            .decode_utf8()
            .map(std::borrow::Cow::into_owned)
            .map_err(|_| ParseEntryError::InvalidKey(key.to_owned()))?;
        let expected_len = self.fields.len();
        let actual_len = values.len();
        if expected_len != actual_len {
            return Err(ParseEntryError::SizeMismatch {
                key,
                expected_len,
                actual_len,
            });
        }
        let mut bucket = None;
        let mut version_id = None;
        let mut etag = None;
        let mut is_latest = None;
        let mut is_delete_marker = None;
        let mut size = None;
        let mut last_modified_date = None;
        let mut etag_is_md5 = true;
        for (&field, value) in std::iter::zip(&self.fields, values) {
            match field {
                InventoryField::Bucket => {
                    if value.is_empty() {
                        return Err(ParseEntryError::EmptyBucket(key));
                    }
                    bucket = Some(value);
                }
                InventoryField::Key => (),
                InventoryField::VersionId => {
                    if value.is_empty() {
                        // An empty version ID in the inventory means the
                        // object was created when the bucket was unversioned,
                        // in which case the effective version ID to use in
                        // GetObject requests is "null".
                        version_id = Some(String::from("null"));
                    } else {
                        version_id = Some(value);
                    }
                    // Leave `version_id` as `None` if there's no VersionId
                    // field, as the field's absence means that either (a) the
                    // bucket is versioned but the inventory only lists latest
                    // versions, in which case we just want to download the
                    // latest version of each key and don't know the version
                    // IDs, and so no version ID should be supplied in
                    // GetObject requests, or (b) the bucket is unversioned (At
                    // least, I assume the inventory for an unversioned bucket
                    // lacks a VersionId field), in which case the version ID
                    // should be absent from GetObject requests.
                }
                InventoryField::IsLatest => {
                    let Ok(b) = value.parse::<bool>() else {
                        return Err(ParseEntryError::Parse {
                            key,
                            field,
                            value,
                            expected: r#""true" or "false""#,
                        });
                    };
                    is_latest = Some(b);
                }
                InventoryField::IsDeleteMarker => {
                    let Ok(b) = value.parse::<bool>() else {
                        return Err(ParseEntryError::Parse {
                            key,
                            field,
                            value,
                            expected: r#""true" or "false""#,
                        });
                    };
                    is_delete_marker = Some(b);
                }
                InventoryField::Size => {
                    if !value.is_empty() {
                        let Ok(sz) = value.parse::<i64>() else {
                            return Err(ParseEntryError::Parse {
                                key,
                                field,
                                value,
                                expected: "an integer",
                            });
                        };
                        size = Some(sz);
                    }
                }
                InventoryField::LastModifiedDate => {
                    let Ok(ts) = OffsetDateTime::parse(
                        &value,
                        &time::format_description::well_known::Rfc3339,
                    ) else {
                        return Err(ParseEntryError::Parse {
                            key,
                            field,
                            value,
                            expected: "an ISO timestamp",
                        });
                    };
                    last_modified_date = Some(ts);
                }
                InventoryField::ETag => {
                    if !value.is_empty() {
                        etag = Some(value);
                    }
                }
                // TODO: If this field is absent, what can we assume about the
                // etag?
                InventoryField::IsMultipartUploaded => {
                    if value == "true" {
                        etag_is_md5 = false;
                    }
                }
                InventoryField::StorageClass => (),
                InventoryField::ReplicationStatus => (),
                InventoryField::EncryptionStatus => {
                    if !matches!(value.as_str(), "NOT-SSE" | "SSE-S3") {
                        etag_is_md5 = false;
                    }
                }
                InventoryField::ObjectLockRetainUntilDate => (),
                InventoryField::ObjectLockMode => (),
                InventoryField::ObjectLockLegalHoldStatus => (),
                InventoryField::IntelligentTieringAccessTier => (),
                InventoryField::BucketKeyStatus => (),
                InventoryField::ChecksumAlgorithm => (),
                InventoryField::ObjectAccessControlList => (),
                InventoryField::ObjectOwner => (),
            }
        }
        let Some(bucket) = bucket else {
            unreachable!("required field 'Bucket' should always be defined");
        };
        let is_latest = is_latest.unwrap_or(true);
        if key.ends_with('/')
            && (is_delete_marker == Some(true) || size.is_none() || size.is_some_and(|sz| sz == 0))
        {
            return Ok(InventoryEntry::Directory(Directory {
                bucket,
                key,
                version_id,
            }));
        }
        let key = KeyPath::try_from(key)?;
        if is_delete_marker == Some(true) {
            Ok(InventoryEntry::Item(InventoryItem {
                bucket,
                key,
                version_id,
                is_latest,
                last_modified_date,
                details: ItemDetails::Deleted,
            }))
        } else {
            let Some(etag) = etag else {
                return Err(ParseEntryError::NoEtag(key));
            };
            Ok(InventoryEntry::Item(InventoryItem {
                bucket,
                key,
                version_id,
                is_latest,
                last_modified_date,
                details: ItemDetails::Present {
                    size,
                    etag,
                    etag_is_md5,
                },
            }))
        }
    }
}

impl std::str::FromStr for FileSchema {
    type Err = ParseFileSchemaError;

    fn from_str(s: &str) -> Result<FileSchema, ParseFileSchemaError> {
        let mut fields = Vec::new();
        let mut seen = HashSet::new();
        for item in s.split(',') {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }
            let Ok(f) = item.parse::<InventoryField>() else {
                return Err(ParseFileSchemaError::Unknown(item.to_owned()));
            };
            fields.push(f);
            if !seen.insert(f) {
                return Err(ParseFileSchemaError::Duplicate(f));
            }
        }
        let missing = InventoryField::REQUIRED
            .into_iter()
            .filter(|f| !seen.contains(f))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(ParseFileSchemaError::MissingRequired(missing));
        }
        let Some(key_index) = fields.iter().position(|&f| f == InventoryField::Key) else {
            unreachable!(
                "Key should be present in fields after ensuring required fields are present"
            );
        };
        Ok(FileSchema { fields, key_index })
    }
}

impl<'de> Deserialize<'de> for FileSchema {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = FileSchema;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a comma-separated list of S3 Inventory list fields")
            }

            fn visit_str<E>(self, input: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                input
                    .parse::<FileSchema>()
                    .map_err(|e| E::invalid_value(Unexpected::Str(input), &e))
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

/// Error returned by [`FileSchema::parse_csv_fields()`] on invalid input
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum ParseEntryError {
    /// The input values lack a "key" field
    #[error("inventory list entry is missing fields, including key")]
    NoKey,

    /// The input values do not have the expected number of fields
    #[error(
        "inventory list entry for key {key:?} has {actual_len} fields; expected {expected_len}"
    )]
    SizeMismatch {
        key: String,
        expected_len: usize,
        actual_len: usize,
    },

    /// The key field could not be percent-decoded
    #[error("inventory list entry key {0:?} did not decode as percent-encoded UTF-8")]
    InvalidKey(String),

    /// The input has an empty "bucket" field
    #[error("inventory item {0:?} has empty bucket field")]
    EmptyBucket(String),

    /// Failed to parse an individual field
    #[error("could not parse inventory list entry for key {key:?}, field {field}, field value {value:?}; expected {expected}")]
    Parse {
        key: String,
        field: InventoryField,
        value: String,
        expected: &'static str,
    },

    /// The entry was not a delete marker and lacked an etag
    #[error("non-deleted inventory item {0:?} lacks etag")]
    NoEtag(KeyPath),

    /// The key was not an acceptable filepath
    #[error("inventory item key is not an acceptable filepath")]
    KeyPath(#[from] KeyPathFromStringError),
}

/// Error returned by `FileSchema::from_str()` on invalid input
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum ParseFileSchemaError {
    /// The list of fields contained an unknown/unrecognized field
    #[error("unknown inventory field in fileSchema: {0:?}")]
    Unknown(String),

    /// The list of fields contained some field more than once
    #[error("duplicate inventory field in fileSchema: {0}")]
    Duplicate(InventoryField),

    /// The list of fields was missing one or more fields required by s3invsync
    #[error(fmt = fmt_missing)]
    MissingRequired(Vec<InventoryField>),
}

impl serde::de::Expected for ParseFileSchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "a comma-separated list of S3 Inventory list fields, but: {self}"
        )
    }
}

/// [`Display`][std::fmt::Display] formatter for the `MissingRequired` variant
/// of [`ParseFileSchemaError`]
fn fmt_missing(missing: &[InventoryField], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "fileSchema is missing required fields: ")?;
    let mut first = true;
    for field in missing {
        if !std::mem::replace(&mut first, false) {
            write!(f, ", ")?;
        }
        write!(f, "{field}")?;
    }
    Ok(())
}
