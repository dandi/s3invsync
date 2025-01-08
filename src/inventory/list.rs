use super::fields::{FileSchema, ParseEntryError};
use super::item::InventoryEntry;
use crate::s3::S3Location;
use flate2::bufread::GzDecoder;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
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
    inner: CsvReader<GzDecoder<BufReader<File>>>,
}

impl InventoryList {
    /// Construct an `InventoryList` from a `CsvReader` reading from the file
    /// at path `path` that was downloaded from `url`
    pub(crate) fn for_downloaded_csv(
        path: PathBuf,
        url: S3Location,
        inner: CsvReader<GzDecoder<BufReader<File>>>,
    ) -> InventoryList {
        InventoryList { path, url, inner }
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
    source: CsvReaderError,
}

/// A struct for decoding [`InventoryEntry`]s from a reader containing CSV data
pub(crate) struct CsvReader<R> {
    inner: csv::DeserializeRecordsIntoIter<R, Vec<String>>,
    file_schema: FileSchema,
}

impl<R: Read> CsvReader<R> {
    pub(crate) fn new(r: R, file_schema: FileSchema) -> Self {
        CsvReader {
            inner: csv::ReaderBuilder::new()
                .has_headers(false)
                .from_reader(r)
                .into_deserialize(),
            file_schema,
        }
    }
}

impl<R: BufRead> CsvReader<GzDecoder<R>> {
    pub(crate) fn from_gzipped_reader(r: R, file_schema: FileSchema) -> Self {
        CsvReader::new(GzDecoder::new(r), file_schema)
    }
}

impl<R: Read> Iterator for CsvReader<R> {
    type Item = Result<InventoryEntry, CsvReaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next()? {
            Ok(values) => match self.file_schema.parse_csv_fields(values) {
                Ok(entry) => Some(Ok(entry)),
                Err(e) => Some(Err(e.into())),
            },
            Err(e) => Some(Err(e.into())),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum CsvReaderError {
    #[error("failed to read entry from CSV file")]
    Csv(#[from] csv::Error),
    #[error("failed to parse fields of CSV entry")]
    Parse(#[from] ParseEntryError),
}
