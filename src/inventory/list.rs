use super::item::InventoryEntry;
use crate::s3::S3Location;
use flate2::bufread::GzDecoder;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use thiserror::Error;

/// A handle for reading entries from an inventory list file
pub(crate) struct InventoryList {
    /// The local path at which the file is located.  Used to delete the file
    /// on drop.
    path: PathBuf,

    /// The S3 URL from which the inventory list was downloaded
    url: S3Location,

    /// The inner filehandle
    inner: csv::DeserializeRecordsIntoIter<GzDecoder<BufReader<File>>, InventoryEntry>,
}

impl InventoryList {
    /// Construct an `InventoryList` from a local file handle `f` at path
    /// `path`, downloaded from `url`
    pub(crate) fn from_gzip_csv_file(path: PathBuf, url: S3Location, f: File) -> InventoryList {
        InventoryList {
            path,
            url,
            inner: csv::ReaderBuilder::new()
                .has_headers(false)
                .from_reader(GzDecoder::new(BufReader::new(f)))
                .into_deserialize(),
        }
    }
}

impl Iterator for InventoryList {
    type Item = Result<InventoryEntry, InventoryListError>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.inner.next()?.map_err(|source| InventoryListError {
            url: self.url.clone(),
            source,
        }))
    }
}

impl Drop for InventoryList {
    /// Delete the local file on drop
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Error returned when an error occurs while reading from an inventory list
/// file
#[derive(Debug, Error)]
#[error("failed to read entry from inventory list at {url}")]
pub(crate) struct InventoryListError {
    url: S3Location,
    source: csv::Error,
}
