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

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    fn parse_csv(s: &str) -> InventoryItem {
        csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(std::io::Cursor::new(s))
            .deserialize()
            .next()
            .unwrap()
            .unwrap()
    }

    #[test]
    fn parse_item() {
        let item = parse_csv(
            r#""dandiarchive","zarr/73fb586f-b58a-49fc-876e-282ba962d310/0/0/0/14/4/100","nuYD8l5blCvLV3DbAiN1IXuwo7aF3F98","true","false","1511723","2022-12-12T13:20:39.000Z","627c47efe292876b91978324485cd2ec","false""#,
        );
        assert_eq!(item.bucket, "dandiarchive");
        assert_eq!(
            item.key,
            "zarr/73fb586f-b58a-49fc-876e-282ba962d310/0/0/0/14/4/100"
        );
        assert_eq!(item.version_id, "nuYD8l5blCvLV3DbAiN1IXuwo7aF3F98");
        assert!(item.is_latest);
        assert_eq!(item.last_modified_date, datetime!(2022-12-12 13:20:39 UTC));
        assert_eq!(
            item.details,
            ItemDetails::Present {
                size: 1511723,
                etag: "627c47efe292876b91978324485cd2ec".into(),
                is_multipart_uploaded: false
            }
        );
    }

    #[test]
    fn parse_deleted_item() {
        let item = parse_csv(
            r#""dandiarchive","zarr/73fb586f-b58a-49fc-876e-282ba962d310/0/0/0/14/4/100","t5w9XO56_Yi1eF6HE7KUgoLumufisMyo","false","true","","2022-12-11T17:55:08.000Z","","""#,
        );
        assert_eq!(item.bucket, "dandiarchive");
        assert_eq!(
            item.key,
            "zarr/73fb586f-b58a-49fc-876e-282ba962d310/0/0/0/14/4/100"
        );
        assert_eq!(item.version_id, "t5w9XO56_Yi1eF6HE7KUgoLumufisMyo");
        assert!(!item.is_latest);
        assert_eq!(item.last_modified_date, datetime!(2022-12-11 17:55:08 UTC));
        assert_eq!(item.details, ItemDetails::Deleted);
    }

    #[test]
    fn parse_encoded() {
        let item = parse_csv(
            r#""dandiarchive","dandiarchive/dandiarchive/hive/dt%3D2024-05-07-01-00/symlink.txt","t4Z7oFATOK2678GfaU8oLcjWDMAS0RgK","true","false","38129","2024-05-07T21:12:55.000Z","f58c1f0e5fb20a9152788f825375884a","false""#,
        );
        assert_eq!(item.bucket, "dandiarchive");
        assert_eq!(
            item.key,
            "dandiarchive/dandiarchive/hive/dt=2024-05-07-01-00/symlink.txt"
        );
        assert_eq!(item.version_id, "t4Z7oFATOK2678GfaU8oLcjWDMAS0RgK");
        assert!(item.is_latest);
        assert_eq!(item.last_modified_date, datetime!(2024-05-07 21:12:55 UTC));
        assert_eq!(
            item.details,
            ItemDetails::Present {
                size: 38129,
                etag: "f58c1f0e5fb20a9152788f825375884a".into(),
                is_multipart_uploaded: false,
            }
        );
    }
}
