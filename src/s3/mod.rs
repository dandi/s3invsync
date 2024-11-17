mod streams;
use self::streams::{ListManifestDates, ListObjectsError};
use crate::manifest::CsvManifest;
use crate::timestamps::{Date, DateHM, DateMaybeHM};
use aws_sdk_s3::{
    operation::get_object::{GetObjectError, GetObjectOutput},
    primitives::ByteStreamError,
    Client,
};
use aws_smithy_runtime_api::client::{orchestrator::HttpResponse, result::SdkError};
use futures_util::TryStreamExt;
use md5::{Digest, Md5};
use std::fs::File;
use std::io::{BufReader, BufWriter, Seek, Write};
use thiserror::Error;

#[derive(Clone, Debug)]
pub(crate) struct S3Client {
    inner: Client,
    region: String,
    inv_bucket: String,
    inv_prefix: String,
}

impl S3Client {
    pub(crate) async fn get_manifest_for_date(
        &self,
        when: Option<DateMaybeHM>,
    ) -> Result<CsvManifest, GetManifestError> {
        let ts = match when {
            None => self.get_latest_manifest_timestamp(None).await?,
            Some(DateMaybeHM::Date(d)) => self.get_latest_manifest_timestamp(Some(d)).await?,
            Some(DateMaybeHM::DateHM(d)) => d,
        };
        self.get_manifest(ts).await
    }

    pub(crate) async fn get_latest_manifest_timestamp(
        &self,
        day: Option<Date>,
    ) -> Result<DateHM, FindManifestError> {
        // Iterate over `DateHM` prefixes in `s3://{inv_bucket}/{inv_prefix}/`
        // or `s3://{inv_bucket}/{inv_prefix}/{day}T` and return greatest one
        let key_prefix = match day {
            Some(d) => join_prefix(&self.inv_prefix, &format!("{d}T")),
            None => join_prefix(&self.inv_prefix, ""),
        };
        let mut stream = ListManifestDates::new(self, key_prefix.clone());
        let mut maxdate = None;
        while let Some(d) = stream.try_next().await? {
            match maxdate {
                None => maxdate = Some(d),
                Some(d0) if d0 < d => maxdate = Some(d),
                Some(_) => (),
            }
        }
        maxdate.ok_or_else(|| FindManifestError::NoMatch {
            bucket: self.inv_bucket.clone(),
            prefix: key_prefix,
        })
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<GetObjectOutput, GetError> {
        self.inner
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|source| GetError {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                source,
            })
    }

    pub(crate) async fn get_manifest(&self, when: DateHM) -> Result<CsvManifest, GetManifestError> {
        let checksum_key = join_prefix(&self.inv_prefix, &format!("{when}/manifest.checksum"));
        let checksum_obj = self.get_object(&self.inv_bucket, &checksum_key).await?;
        let checksum_bytes = checksum_obj
            .body
            .collect()
            .await
            .map_err(|source| GetManifestError::DownloadChecksum {
                bucket: self.inv_bucket.clone(),
                key: checksum_key.clone(),
                source,
            })?
            .to_vec();
        let checksum = std::str::from_utf8(&checksum_bytes)
            .map_err(|source| GetManifestError::DecodeChecksum {
                bucket: self.inv_bucket.clone(),
                key: checksum_key,
                source,
            })?
            .trim();
        let manifest_key = join_prefix(&self.inv_prefix, &format!("{when}/manifest.json"));
        let mut manifest_file =
            tempfile::tempfile().map_err(|source| GetManifestError::Tempfile {
                bucket: self.inv_bucket.clone(),
                key: manifest_key.clone(),
                source,
            })?;
        self.download_object(&self.inv_bucket, &manifest_key, checksum, &manifest_file)
            .await?;
        manifest_file
            .rewind()
            .map_err(|source| GetManifestError::Rewind {
                bucket: self.inv_bucket.clone(),
                key: manifest_key.clone(),
                source,
            })?;
        let manifest = serde_json::from_reader::<_, CsvManifest>(BufReader::new(manifest_file))
            .map_err(|source| GetManifestError::Parse {
                bucket: self.inv_bucket.clone(),
                key: manifest_key,
                source,
            })?;
        Ok(manifest)
    }

    async fn download_object(
        &self,
        bucket: &str,
        key: &str,
        // `md5_digest` must be a 32-character lowercase hexadecimal string
        md5_digest: &str,
        outfile: &File,
    ) -> Result<(), DownloadError> {
        let obj = self.get_object(bucket, key).await?;
        let mut bytestream = obj.body;
        let mut outfile = BufWriter::new(outfile);
        let mut hasher = Md5::new();
        while let Some(blob) =
            bytestream
                .try_next()
                .await
                .map_err(|source| DownloadError::Download {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                    source,
                })?
        {
            outfile
                .write(&blob)
                .map_err(|source| DownloadError::Write {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                    source,
                })?;
            hasher.update(&blob);
        }
        outfile.flush().map_err(|source| DownloadError::Write {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            source,
        })?;
        let actual_md5 = hex::encode(hasher.finalize());
        if actual_md5 != md5_digest {
            Err(DownloadError::Verify {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                expected_md5: md5_digest.to_owned(),
                actual_md5,
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum FindManifestError {
    #[error(transparent)]
    List(#[from] ListObjectsError),
    #[error("no manifests found in bucket {bucket:?} with prefix {prefix:?}")]
    NoMatch { bucket: String, prefix: String },
}

#[derive(Debug, Error)]
pub(crate) enum GetManifestError {
    #[error(transparent)]
    Find(#[from] FindManifestError),
    #[error(transparent)]
    Get(#[from] GetError),
    #[error(transparent)]
    Download(#[from] DownloadError),
    #[error("failed downloading checksum at bucket {bucket:?}, key {key:?}")]
    DownloadChecksum {
        bucket: String,
        key: String,
        source: ByteStreamError,
    },
    #[error("manifest checksum contents at bucket {bucket:?}, key {key:?} are not UTF-8")]
    DecodeChecksum {
        bucket: String,
        key: String,
        source: std::str::Utf8Error,
    },
    #[error("failed to create tempfile for downloading bucket {bucket:?}, key {key:?}")]
    Tempfile {
        bucket: String,
        key: String,
        source: std::io::Error,
    },
    #[error("failed to rewind tempfile after downloading bucket {bucket:?}, key {key:?}")]
    Rewind {
        bucket: String,
        key: String,
        source: std::io::Error,
    },
    #[error("failed to deserialize manifest at bucket {bucket:?}, key {key:?}")]
    Parse {
        bucket: String,
        key: String,
        source: serde_json::Error,
    },
}

#[derive(Debug, Error)]
pub(crate) enum DownloadError {
    #[error(transparent)]
    Get(#[from] GetError),
    #[error("failed downloading contents for bucket {bucket:?}, key {key:?}")]
    Download {
        bucket: String,
        key: String,
        source: ByteStreamError,
    },
    #[error("failed writing contents of bucket {bucket:?}, key {key:?} to disk")]
    Write {
        bucket: String,
        key: String,
        source: std::io::Error,
    },
    #[error("checksum verification for object at bucket {bucket:?}, key {key:?} failed; expected MD5 {expected_md5:?}, got {actual_md5:?}")]
    Verify {
        bucket: String,
        key: String,
        expected_md5: String,
        actual_md5: String,
    },
}

#[derive(Debug, Error)]
#[error("failed to get object in bucket {bucket:?} at key {key:?}")]
pub(crate) struct GetError {
    bucket: String,
    key: String,
    source: SdkError<GetObjectError, HttpResponse>,
}

fn join_prefix(prefix: &str, suffix: &str) -> String {
    let mut s = prefix.to_owned();
    if !s.ends_with('/') {
        s.push('/');
    }
    s.push_str(suffix);
    s
}
