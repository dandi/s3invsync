//! Working directly with AWS S3
mod location;
mod streams;
pub(crate) use self::location::S3Location;
use self::streams::{ListManifestDates, ListObjectsError};
use crate::consts::CSV_GZIP_PEEK_SIZE;
use crate::inventory::{CsvReader, CsvReaderError, InventoryEntry, InventoryList};
use crate::manifest::{CsvManifest, FileSpec};
use crate::timestamps::{Date, DateHM, DateMaybeHM};
use aws_credential_types::{
    provider::{error::CredentialsError, ProvideCredentials},
    Credentials,
};
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
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Client for interacting with S3
#[derive(Debug)]
pub(crate) struct S3Client {
    /// The inner AWS SDK client object
    inner: Client,

    /// The location of the manifest files for the S3 inventory that is being
    /// backed up
    inventory_base: S3Location,

    /// Whether to emit TRACE messages for download progress
    trace_progress: bool,

    /// A temporary directory in which to download temporary files
    tmpdir: tempfile::TempDir,
}

impl S3Client {
    pub(crate) async fn new(
        region: String,
        inventory_base: S3Location,
        trace_progress: bool,
    ) -> Result<S3Client, ClientBuildError> {
        let tmpdir = tempfile::tempdir().map_err(ClientBuildError::Tempdir)?;
        let mut config = aws_config::from_env()
            .app_name(
                aws_config::AppName::new(env!("CARGO_PKG_NAME"))
                    .expect("crate name should be a valid app name"),
            )
            .region(aws_config::Region::new(region))
            .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(10));
        config = match get_credentials().await? {
            Some(creds) => config.credentials_provider(creds),
            None => config.no_credentials(),
        };
        let inner = Client::new(&config.load().await);
        Ok(S3Client {
            inner,
            inventory_base,
            trace_progress,
            tmpdir,
        })
    }

    /// Create a temporary file at `subpath` within the temporary directory for
    /// downloading `objloc` to.  Returns a filehandle opened for reading &
    /// writing and the full path to the file.
    fn make_dl_tempfile(
        &self,
        subpath: &Path,
        objloc: &S3Location,
    ) -> Result<(File, PathBuf), TempfileError> {
        tracing::debug!(url = %objloc, "Creating temporary file for downloading object");
        let path = self.tmpdir.path().join(subpath);
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p).map_err(|source| TempfileError::Mkdir {
                url: objloc.to_owned(),
                source,
            })?;
        }
        File::options()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(&path)
            .map(|f| (f, path))
            .map_err(|source| TempfileError::Open {
                url: objloc.to_owned(),
                source,
            })
    }

    /// Fetch the manifest file for inventory created at the given timestamp.
    ///
    /// If `when` is `None`, the latest manifest is returned.  If `when`
    /// is a date without an hour & minute, the latest manifest at that date is
    /// returned.  Otherwise, `when` is a date with an hour & minute, and the
    /// manifest for that exact timestamp is returned.
    ///
    /// The return value includes both the manifest and the full, exact
    /// timestamp.
    pub(crate) async fn get_manifest_for_date(
        &self,
        when: Option<DateMaybeHM>,
    ) -> Result<(CsvManifest, DateHM), GetManifestError> {
        let ts = match when {
            None => self.get_latest_manifest_timestamp(None).await?,
            Some(DateMaybeHM::Date(d)) => self.get_latest_manifest_timestamp(Some(d)).await?,
            Some(DateMaybeHM::DateHM(d)) => d,
        };
        tracing::info!(timestamp = %ts, "Getting manifest for timestamp");
        let manifest = self.get_manifest(ts).await?;
        Ok((manifest, ts))
    }

    /// Returns a stream yielding all available inventory manifest timestamps
    pub(crate) fn list_all_manifest_timestamps(&self) -> ListManifestDates {
        ListManifestDates::new(self, &self.inventory_base)
    }

    /// Return the full timestamp for the latest manifest, either (if `day` is
    /// `None`) out of all manifests or else the latest on the given date.
    #[tracing::instrument(skip_all, fields(day = day.map(|d| d.to_string())))]
    async fn get_latest_manifest_timestamp(
        &self,
        day: Option<Date>,
    ) -> Result<DateHM, FindManifestError> {
        // Iterate over `DateHM` prefixes in `s3://{inv_bucket}/{inv_prefix}/`
        // or `s3://{inv_bucket}/{inv_prefix}/{day}T` and return greatest one
        let url = if let Some(d) = day {
            tracing::debug!(date = %d, "Listing manifests for date ...");
            self.inventory_base.join(&format!("{d}T"))
        } else {
            tracing::debug!("Listing all manifests ...");
            self.inventory_base.join("")
        };
        let mut stream = ListManifestDates::new(self, &url);
        let mut maxdate = None;
        while let Some(d) = stream.try_next().await? {
            match maxdate {
                None => maxdate = Some(d),
                Some(d0) if d0 < d => maxdate = Some(d),
                Some(_) => (),
            }
        }
        maxdate.ok_or_else(|| FindManifestError::NoMatch { url })
    }

    /// Perform a "Get Object" request for the object at `url`
    async fn get_object(&self, url: &S3Location) -> Result<GetObjectOutput, GetError> {
        let mut op = self.inner.get_object().bucket(url.bucket()).key(url.key());
        if let Some(v) = url.version_id() {
            op = op.version_id(v);
        }
        op.send().await.map_err(|source| GetError {
            url: url.to_owned(),
            source,
        })
    }

    /// Download, parse, & return the manifest file for the inventory created
    /// at the timestamp `when`.
    ///
    /// The manifest's checksum is also downloaded and used to validate the
    /// manifest download.
    #[tracing::instrument(skip_all, fields(%when))]
    async fn get_manifest(&self, when: DateHM) -> Result<CsvManifest, GetManifestError> {
        tracing::debug!("Fetching manifest.checksum file");
        let checksum_url = self
            .inventory_base
            .join(&format!("{when}/manifest.checksum"));
        let checksum_obj = self.get_object(&checksum_url).await?;
        let checksum_bytes = checksum_obj
            .body
            .collect()
            .await
            .map_err(|source| GetManifestError::DownloadChecksum {
                url: checksum_url.clone(),
                source,
            })?
            .to_vec();
        let checksum = std::str::from_utf8(&checksum_bytes)
            .map_err(|source| GetManifestError::DecodeChecksum {
                url: checksum_url.clone(),
                source,
            })?
            .trim();
        tracing::debug!("Fetching manifest.json file");
        let manifest_url = self.inventory_base.join(&format!("{when}/manifest.json"));
        let (mut manifest_file, manifest_path) = self.make_dl_tempfile(
            &PathBuf::from(format!("manifests/{when}.json")),
            &manifest_url,
        )?;
        self.download_object(&manifest_url, Some(checksum), &manifest_file)
            .await?;
        manifest_file
            .rewind()
            .map_err(|source| GetManifestError::Rewind {
                url: manifest_url.clone(),
                source,
            })?;
        let manifest = serde_json::from_reader::<_, CsvManifest>(BufReader::new(manifest_file))
            .map_err(|source| GetManifestError::Parse {
                url: manifest_url,
                source,
            })?;
        let _ = std::fs::remove_file(manifest_path);
        Ok(manifest)
    }

    /// Download the CSV inventory list file described by `fspec` to a
    /// temporary location and return a filehandle for iterating over its
    /// entries
    #[tracing::instrument(skip_all, fields(key = fspec.key))]
    pub(crate) async fn download_inventory_csv(
        &self,
        fspec: FileSpec,
    ) -> Result<InventoryList, CsvDownloadError> {
        let fname = fspec
            .key
            .rsplit_once('/')
            .map_or(&*fspec.key, |(_, after)| after);
        let url = self.inventory_base.with_key(&fspec.key);
        let (mut outfile, path) =
            self.make_dl_tempfile(&PathBuf::from(format!("data/{fname}")), &url)?;
        self.download_object(&url, Some(&fspec.md5_checksum), &outfile)
            .await?;
        outfile
            .rewind()
            .map_err(|source| CsvDownloadError::Rewind {
                url: url.clone(),
                source,
            })?;
        let reader = CsvReader::from_gzipped_reader(BufReader::new(outfile), fspec.file_schema);
        Ok(InventoryList::for_downloaded_csv(path, url, reader))
    }

    /// Fetch the first [`CSV_GZIP_PEEK_SIZE`] bytes of the CSV inventory list
    /// file described by `fspec` and extract the first line.  Returns `None`
    /// if the file is empty.
    #[tracing::instrument(skip_all, fields(key = fspec.key))]
    pub(crate) async fn peek_inventory_csv(
        &self,
        fspec: &FileSpec,
    ) -> Result<Option<InventoryEntry>, CsvPeekError> {
        tracing::debug!("Peeking at first {CSV_GZIP_PEEK_SIZE} bytes of file");
        let url = self.inventory_base.with_key(&fspec.key);
        let obj = self.get_object(&url).await?;
        let mut bytestream = obj.body;
        let mut header = std::collections::VecDeque::with_capacity(CSV_GZIP_PEEK_SIZE);
        while let Some(blob) =
            bytestream
                .try_next()
                .await
                .map_err(|source| CsvPeekError::Download {
                    url: url.clone(),
                    source,
                })?
        {
            header.extend(blob);
            if header.len() >= CSV_GZIP_PEEK_SIZE {
                break;
            }
        }
        CsvReader::from_gzipped_reader(header, fspec.file_schema.clone())
            .next()
            .transpose()
            .map_err(|source| CsvPeekError::Decode { url, source })
    }

    /// Download the object at `url` and write its bytes to `outfile`.  If
    /// `md5_digest` is non-`None` (in which case it must be a 32-character
    /// lowercase hexadecimal string), it is used to validate the download.
    #[tracing::instrument(skip_all, fields(url = %url))]
    pub(crate) async fn download_object(
        &self,
        url: &S3Location,
        md5_digest: Option<&str>,
        outfile: &File,
    ) -> Result<(), DownloadError> {
        tracing::debug!("Downloading object to disk");
        let obj = self.get_object(url).await?;
        let mut total_received = 0;
        let object_size = obj.content_length;
        let mut bytestream = obj.body;
        let mut outfile = BufWriter::new(outfile);
        let mut hasher = Md5::new();
        while let Some(blob) =
            bytestream
                .try_next()
                .await
                .map_err(|source| DownloadError::Download {
                    url: url.to_owned(),
                    source,
                })?
        {
            total_received += blob.len();
            if self.trace_progress {
                tracing::trace!(
                    chunk_size = blob.len(),
                    total_received,
                    object_size,
                    "Received chunk"
                );
            }
            outfile
                .write(&blob)
                .map_err(|source| DownloadError::Write {
                    url: url.to_owned(),
                    source,
                })?;
            hasher.update(&blob);
        }
        outfile.flush().map_err(|source| DownloadError::Write {
            url: url.to_owned(),
            source,
        })?;
        let actual_md5 = hex::encode(hasher.finalize());
        if let Some(expected_md5) = md5_digest {
            if actual_md5 != expected_md5 {
                return Err(DownloadError::Md5 {
                    url: url.to_owned(),
                    expected_md5: expected_md5.to_owned(),
                    actual_md5,
                });
            }
        }
        tracing::debug!("Finished download");
        Ok(())
    }
}

/// Error returned by [`S3Client::new()`]
#[derive(Debug, Error)]
pub(crate) enum ClientBuildError {
    /// The client's temporary directory could not be created
    #[error("failed to create temporary downloads directory")]
    Tempdir(#[from] std::io::Error),

    /// There was an error fetching AWS credentials
    #[error("failed to fetch AWS credentials")]
    Credentials(#[from] CredentialsError),
}

/// Error returned by [`S3Client::make_dl_tempfile()`]
#[derive(Debug, Error)]
pub(crate) enum TempfileError {
    /// Failed to create parent directories for temporary file path
    #[error("failed to create parent directories for tempfile for downloading {url}")]
    Mkdir {
        url: S3Location,
        source: std::io::Error,
    },

    /// Failed to open temporary file handle
    #[error("failed to open tempfile for downloading {url}")]
    Open {
        url: S3Location,
        source: std::io::Error,
    },
}

/// Error returned by [`S3Client::get_latest_manifest_timestamp()`]
#[derive(Debug, Error)]
pub(crate) enum FindManifestError {
    /// An error occurred while listing the manifest directories
    #[error(transparent)]
    List(Box<ListObjectsError>),

    /// No matching manifests were found
    #[error("no manifests found in {url}")]
    NoMatch { url: S3Location },
}

impl From<ListObjectsError> for FindManifestError {
    fn from(e: ListObjectsError) -> FindManifestError {
        FindManifestError::List(Box::new(e))
    }
}

/// Error returned by [`S3Client::get_manifest_for_date()`] and
/// [`S3Client::get_manifest()`]
#[derive(Debug, Error)]
pub(crate) enum GetManifestError {
    /// Failed to locate manifest for the given timestamp
    #[error(transparent)]
    Find(#[from] FindManifestError),

    /// Failed to perform a "Get Object" request for the manifest's checksum
    #[error(transparent)]
    Get(Box<GetError>),

    /// Failed to download the manifest's checksum
    #[error("failed downloading checksum at {url}")]
    DownloadChecksum {
        url: S3Location,
        source: ByteStreamError,
    },

    /// Failed to decode the manifest's checksum file as UTF-8
    #[error("manifest checksum contents at {url} are not UTF-8")]
    DecodeChecksum {
        url: S3Location,
        source: std::str::Utf8Error,
    },

    /// Failed to create temporary download file
    #[error(transparent)]
    Tempfile(#[from] TempfileError),

    /// Failed to download the manifest
    #[error(transparent)]
    Download(#[from] DownloadError),

    /// Failed to rewind manifest filehandle after downloading
    #[error("failed to rewind tempfile after downloading {url}")]
    Rewind {
        url: S3Location,
        source: std::io::Error,
    },

    /// Failed to parse manifest contents
    #[error("failed to deserialize manifest at {url}")]
    Parse {
        url: S3Location,
        source: serde_json::Error,
    },
}

impl From<GetError> for GetManifestError {
    fn from(e: GetError) -> GetManifestError {
        GetManifestError::Get(Box::new(e))
    }
}

/// Error returned by [`S3Client::download_object()`]
#[derive(Debug, Error)]
pub(crate) enum DownloadError {
    /// Failed to perform "Get Object" request
    #[error(transparent)]
    Get(Box<GetError>),

    /// Error while receiving bytes for the object
    #[error("failed downloading contents for {url}")]
    Download {
        url: S3Location,
        source: ByteStreamError,
    },

    /// Error while writing bytes to disk
    #[error("failed writing contents of {url} to disk")]
    Write {
        url: S3Location,
        source: std::io::Error,
    },

    /// Object's computed MD5 digest did not match the expected MD5 digest
    #[error("checksum verification for object at {url} failed; expected MD5 {expected_md5:?}, got {actual_md5:?}")]
    Md5 {
        url: S3Location,
        expected_md5: String,
        actual_md5: String,
    },
}

impl From<GetError> for DownloadError {
    fn from(e: GetError) -> DownloadError {
        DownloadError::Get(Box::new(e))
    }
}

/// Error returned by [`S3Client::download_inventory_csv()`]
#[derive(Debug, Error)]
pub(crate) enum CsvDownloadError {
    /// Failed to create temporary download file
    #[error(transparent)]
    Tempfile(#[from] TempfileError),

    /// Failed to download the inventory list file
    #[error(transparent)]
    Download(#[from] DownloadError),

    /// Failed to rewind filehandle after downloading
    #[error("failed to rewind tempfile after downloading {url}")]
    Rewind {
        url: S3Location,
        source: std::io::Error,
    },
}

/// Error returned by [`S3Client::peek_inventory_csv()`]
#[derive(Debug, Error)]
pub(crate) enum CsvPeekError {
    /// Failed to perform "Get Object" request
    #[error(transparent)]
    Get(Box<GetError>),

    /// Error while receiving bytes for the object
    #[error("failed downloading contents for {url}")]
    Download {
        url: S3Location,
        source: ByteStreamError,
    },

    /// Failed to read first line from header
    #[error("failed to decode first line from peeking at {url}")]
    Decode {
        url: S3Location,
        source: CsvReaderError,
    },
}

impl From<GetError> for CsvPeekError {
    fn from(e: GetError) -> CsvPeekError {
        CsvPeekError::Get(Box::new(e))
    }
}

/// Error returned by [`S3Client::get_object()`] when a "Get Object" request
/// fails
#[derive(Debug, Error)]
#[error("failed to get object at {url}")]
pub(crate) struct GetError {
    url: S3Location,
    source: SdkError<GetObjectError, HttpResponse>,
}

impl GetError {
    fn status_code(&self) -> Option<u16> {
        if let SdkError::ServiceError(ref e) = self.source {
            Some(e.raw().status().as_u16())
        } else {
            None
        }
    }

    pub(crate) fn is_403(&self) -> bool {
        self.status_code() == Some(403)
    }

    pub(crate) fn is_404(&self) -> bool {
        self.status_code() == Some(404)
    }

    pub(crate) fn is_invalid_object_state(&self) -> bool {
        if let SdkError::ServiceError(ref e) = self.source {
            matches!(e.err(), GetObjectError::InvalidObjectState(_))
        } else {
            false
        }
    }
}

/// Determine the region that the given S3 bucket belongs to
// cf. <https://github.com/awslabs/aws-sdk-rust/issues/1052>
pub(crate) async fn get_bucket_region(bucket: &str) -> Result<String, GetBucketRegionError> {
    let config = aws_config::from_env()
        .app_name(
            aws_config::AppName::new(env!("CARGO_PKG_NAME"))
                .expect("crate name should be a valid app name"),
        )
        .no_credentials()
        .region("us-east-1")
        .load()
        .await;
    let s3 = Client::new(&config);
    let res = s3.head_bucket().bucket(bucket).send().await;
    let bucket_region = match res {
        Ok(res) => res.bucket_region().map(str::to_owned),
        Err(err) => err
            .raw_response()
            .and_then(|res| res.headers().get("x-amz-bucket-region"))
            .map(str::to_owned),
    };
    bucket_region.ok_or(GetBucketRegionError)
}

/// Error returned by [`get_bucket_region()`].
///
/// This usually indicates that the given bucket does not exist.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("could not determine S3 bucket region")]
pub(crate) struct GetBucketRegionError;

/// Load the AWS credentials for the environment.  If there are no credentials,
/// return `None`.
async fn get_credentials() -> Result<Option<Credentials>, CredentialsError> {
    tracing::debug!("Checking for AWS credentials ...");
    let provider = aws_config::default_provider::credentials::default_provider().await;
    match provider.provide_credentials().await {
        Ok(creds) => Ok(Some(creds)),
        Err(CredentialsError::CredentialsNotLoaded(_)) => Ok(None),
        Err(e) => Err(e),
    }
}
