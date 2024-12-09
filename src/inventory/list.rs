use super::item::InventoryItem;
use crate::s3::S3Location;
use flate2::bufread::GzDecoder;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use thiserror::Error;

pub(crate) struct InventoryList {
    path: PathBuf,
    url: S3Location,
    inner: csv::DeserializeRecordsIntoIter<GzDecoder<BufReader<File>>, InventoryItem>,
}

impl InventoryList {
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
    type Item = Result<InventoryItem, InventoryListError>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.inner.next()?.map_err(|source| InventoryListError {
            url: self.url.clone(),
            source,
        }))
    }
}

impl Drop for InventoryList {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Debug, Error)]
#[error("failed to read entry from inventory list at {url}")]
pub(crate) struct InventoryListError {
    url: S3Location,
    source: csv::Error,
}
