use crate::keypath::KeyPath;
use crate::s3::S3Location;
use time::OffsetDateTime;

/// An entry in an inventory list file
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum InventoryEntry {
    Directory(Directory),
    Item(InventoryItem),
}

impl InventoryEntry {
    /// Returns the entry's key
    pub(crate) fn key(&self) -> &str {
        match self {
            InventoryEntry::Directory(Directory { key, .. }) => key,
            InventoryEntry::Item(InventoryItem { key, .. }) => key.as_ref(),
        }
    }
}

/// An entry in an inventory list file pointing to a directory object
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Directory {
    /// The bucket on which the object is located
    pub(super) bucket: String,

    /// The object's key (ends in '/')
    // Not a KeyPath, as the key ends in '/':
    pub(super) key: String,

    /// The object's version ID
    pub(super) version_id: String,
}

impl Directory {
    /// Returns the S3 URL for the object
    pub(crate) fn url(&self) -> S3Location {
        S3Location::new(self.bucket.clone(), self.key.clone())
            .with_version_id(self.version_id.clone())
    }
}

/// A non-directory entry in an inventory list file, describing an object to
/// back up
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InventoryItem {
    /// The bucket on which the object is located
    pub(crate) bucket: String,

    /// The object's key
    pub(crate) key: KeyPath,

    /// The object's version ID
    pub(crate) version_id: String,

    /// True iff this is the latest version of the key
    pub(crate) is_latest: bool,

    /// The object's date of last modification
    pub(crate) last_modified_date: Option<OffsetDateTime>,

    /// Metadata about the object's content
    pub(crate) details: ItemDetails,
}

impl InventoryItem {
    /// Returns the S3 URL for the object
    pub(crate) fn url(&self) -> S3Location {
        S3Location::new(self.bucket.clone(), String::from(&self.key))
            .with_version_id(self.version_id.clone())
    }
}

/// Metadata about an object's content
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ItemDetails {
    /// This version of the object is not a delete marker
    Present {
        /// The object's size
        size: Option<i64>,
        /// The object's etag
        etag: String,
        /// Whether the etag is an MD5 digest of the object's contents
        etag_is_md5: bool,
    },

    /// This version of the object is a delete marker
    Deleted,
}

impl ItemDetails {
    /// Returns the object's MD5 digest, if available
    pub(crate) fn md5_digest(&self) -> Option<&str> {
        // <https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html>
        // Note that encryption type will also need to be taken into account
        // if & when that's supported.
        match self {
            ItemDetails::Present {
                etag,
                etag_is_md5: true,
                ..
            } => Some(etag),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::{CsvReader, FileSchema};
    use assert_matches::assert_matches;
    use time::macros::datetime;

    fn parse_csv(s: &str) -> InventoryEntry {
        let file_schema = "Bucket, Key, VersionId, IsLatest, IsDeleteMarker, Size, LastModifiedDate, ETag, IsMultipartUploaded".parse::<FileSchema>().unwrap();
        CsvReader::new(s.as_bytes(), file_schema)
            .next()
            .unwrap()
            .unwrap()
    }

    #[test]
    fn parse_item() {
        let entry = parse_csv(
            r#""dandiarchive","zarr/73fb586f-b58a-49fc-876e-282ba962d310/0/0/0/14/4/100","nuYD8l5blCvLV3DbAiN1IXuwo7aF3F98","true","false","1511723","2022-12-12T13:20:39.000Z","627c47efe292876b91978324485cd2ec","false""#,
        );
        assert_matches!(entry, InventoryEntry::Item(item) => {
            assert_eq!(item.bucket, "dandiarchive");
            assert_eq!(
                item.key,
                "zarr/73fb586f-b58a-49fc-876e-282ba962d310/0/0/0/14/4/100"
            );
            assert_eq!(item.version_id, "nuYD8l5blCvLV3DbAiN1IXuwo7aF3F98");
            assert!(item.is_latest);
            assert_eq!(item.last_modified_date, Some(datetime!(2022-12-12 13:20:39 UTC)));
            assert_eq!(
                item.details,
                ItemDetails::Present {
                    size: Some(1511723),
                    etag: "627c47efe292876b91978324485cd2ec".into(),
                    etag_is_md5: true,
                }
            );
        });
    }

    #[test]
    fn parse_deleted_item() {
        let entry = parse_csv(
            r#""dandiarchive","zarr/73fb586f-b58a-49fc-876e-282ba962d310/0/0/0/14/4/100","t5w9XO56_Yi1eF6HE7KUgoLumufisMyo","false","true","","2022-12-11T17:55:08.000Z","","""#,
        );
        assert_matches!(entry, InventoryEntry::Item(item) => {
            assert_eq!(item.bucket, "dandiarchive");
            assert_eq!(
                item.key,
                "zarr/73fb586f-b58a-49fc-876e-282ba962d310/0/0/0/14/4/100"
            );
            assert_eq!(item.version_id, "t5w9XO56_Yi1eF6HE7KUgoLumufisMyo");
            assert!(!item.is_latest);
            assert_eq!(item.last_modified_date, Some(datetime!(2022-12-11 17:55:08 UTC)));
            assert_eq!(item.details, ItemDetails::Deleted);
        });
    }

    #[test]
    fn parse_encoded() {
        let entry = parse_csv(
            r#""dandiarchive","dandiarchive/dandiarchive/hive/dt%3D2024-05-07-01-00/symlink.txt","t4Z7oFATOK2678GfaU8oLcjWDMAS0RgK","true","false","38129","2024-05-07T21:12:55.000Z","f58c1f0e5fb20a9152788f825375884a","false""#,
        );
        assert_matches!(entry, InventoryEntry::Item(item) => {
            assert_eq!(item.bucket, "dandiarchive");
            assert_eq!(
                item.key,
                "dandiarchive/dandiarchive/hive/dt=2024-05-07-01-00/symlink.txt"
            );
            assert_eq!(item.version_id, "t4Z7oFATOK2678GfaU8oLcjWDMAS0RgK");
            assert!(item.is_latest);
            assert_eq!(item.last_modified_date, Some(datetime!(2024-05-07 21:12:55 UTC)));
            assert_eq!(
                item.details,
                ItemDetails::Present {
                    size: Some(38129),
                    etag: "f58c1f0e5fb20a9152788f825375884a".into(),
                    etag_is_md5: true,
                }
            );
        });
    }

    #[test]
    fn parse_directory() {
        let entry = parse_csv(
            r#""dandiarchive","dandiarchive/dandiarchive/data/","T_OH5MESsVJ6jygdWfiJfQJ166fQ6kDx","true","false","0","2024-12-18T15:23:29.000Z","d41d8cd98f00b204e9800998ecf8427e","false""#,
        );
        assert_eq!(
            entry,
            InventoryEntry::Directory(Directory {
                bucket: "dandiarchive".into(),
                key: "dandiarchive/dandiarchive/data/".into(),
                version_id: "T_OH5MESsVJ6jygdWfiJfQJ166fQ6kDx".into(),
            })
        );
    }
}
