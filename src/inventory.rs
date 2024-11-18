use serde::{de, Deserialize};
use std::fmt;
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(try_from = "RawInventoryItem")]
pub(crate) struct InventoryItem {
    bucket: String,
    key: String,
    version_id: String,
    is_latest: bool,
    last_modified_date: OffsetDateTime,
    details: ItemDetails,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ItemDetails {
    Present {
        size: i64,
        etag: String,
        is_multipart_uploaded: bool,
    },
    Deleted,
}

impl TryFrom<RawInventoryItem> for InventoryItem {
    type Error = InventoryItemError;

    fn try_from(value: RawInventoryItem) -> Result<InventoryItem, InventoryItemError> {
        if value.is_delete_marker {
            Ok(InventoryItem {
                bucket: value.bucket,
                key: value.key,
                version_id: value.version_id,
                is_latest: value.is_latest,
                last_modified_date: value.last_modified_date,
                details: ItemDetails::Deleted,
            })
        } else {
            let Some(size) = value.size else {
                return Err(InventoryItemError::NoSize(value.key));
            };
            let Some(etag) = value.etag else {
                return Err(InventoryItemError::NoEtag(value.key));
            };
            // Is there any point in caring if this one is absent?
            let is_multipart_uploaded = value.is_multipart_uploaded.unwrap_or_default();
            Ok(InventoryItem {
                bucket: value.bucket,
                key: value.key,
                version_id: value.version_id,
                is_latest: value.is_latest,
                last_modified_date: value.last_modified_date,
                details: ItemDetails::Present {
                    size,
                    etag,
                    is_multipart_uploaded,
                },
            })
        }
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum InventoryItemError {
    #[error("non-deleted inventory item {0:?} lacks size")]
    NoSize(String),
    #[error("non-deleted inventory item {0:?} lacks etag")]
    NoEtag(String),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
struct RawInventoryItem {
    // IMPORTANT: The order of the fields must match that in
    // `EXPECTED_FILE_SCHEMA` in `manifest.rs`
    bucket: String,
    #[serde(deserialize_with = "percent_decode")]
    key: String,
    version_id: String,
    is_latest: bool,
    is_delete_marker: bool,
    size: Option<i64>,
    #[serde(with = "time::serde::rfc3339")]
    last_modified_date: OffsetDateTime,
    etag: Option<String>,
    is_multipart_uploaded: Option<bool>,
}

fn percent_decode<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: de::Deserializer<'de>,
{
    struct Visitor;

    impl de::Visitor<'_> for Visitor {
        type Value = String;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a percent-encoded UTF-8 string")
        }

        fn visit_str<E>(self, input: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            percent_encoding::percent_decode_str(input)
                .decode_utf8()
                .map(std::borrow::Cow::into_owned)
                .map_err(|_| E::invalid_value(de::Unexpected::Str(input), &self))
        }
    }

    deserializer.deserialize_str(Visitor)
}
