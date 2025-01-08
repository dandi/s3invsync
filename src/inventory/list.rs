use super::item::InventoryEntry;
use crate::s3::S3Location;
use flate2::bufread::GzDecoder;
use std::fs::File;
use std::io::{BufRead, BufReader};
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
    inner: CsvGzipReader<BufReader<File>>,
}

impl InventoryList {
    /// Construct an `InventoryList` from a local file handle `f` at path
    /// `path`, downloaded from `url`
    pub(crate) fn from_gzip_csv_file(path: PathBuf, url: S3Location, f: File) -> InventoryList {
        InventoryList {
            path,
            url,
            inner: CsvGzipReader::new(BufReader::new(f)),
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

/// A struct for decoding [`InventoryEntry`]s from a reader containing gzipped
/// CSV data
pub(crate) struct CsvGzipReader<R>(csv::DeserializeRecordsIntoIter<GzDecoder<R>, InventoryEntry>);

impl<R: BufRead> CsvGzipReader<R> {
    pub(crate) fn new(r: R) -> Self {
        CsvGzipReader(
            csv::ReaderBuilder::new()
                .has_headers(false)
                .from_reader(GzDecoder::new(r))
                .into_deserialize(),
        )
    }
}

impl<R: BufRead> Iterator for CsvGzipReader<R> {
    type Item = Result<InventoryEntry, csv::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}
