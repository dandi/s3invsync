/// The name of the file in which metadata (version ID and etag) are stored for
/// the latest versions of objects in each directory
pub(crate) static METADATA_FILENAME: &str = ".s3invsync.versions.json";

/// The number of initial bytes of an inventory csv.gz file to fetch when
/// peeking at just the first entry
pub(crate) const CSV_GZIP_PEEK_SIZE: usize = 1024;
