use serde::{de, Deserialize};
use std::fmt;
use time::OffsetDateTime;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct InventoryItem {
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
