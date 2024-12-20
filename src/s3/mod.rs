mod location;
mod streams;
pub(crate) use self::location::S3Location;
use self::streams::{ListManifestDates, ListObjectsError};
use crate::inventory::InventoryList;
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

#[derive(Debug)]
pub(crate) struct S3Client {
    inner: Client,
    inventory_base: S3Location,
    trace_progress: bool,
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

    #[tracing::instrument(skip_all, fields(day = day.map(|d| d.to_string())))]
    pub(crate) async fn get_latest_manifest_timestamp(
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
        let mut stream = ListManifestDates::new(self, url.clone());
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

    #[tracing::instrument(skip_all, fields(%when))]
    pub(crate) async fn get_manifest(&self, when: DateHM) -> Result<CsvManifest, GetManifestError> {
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
        let (mut manifest_file, _) = self.make_dl_tempfile(
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
        Ok(manifest)
    }

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
        Ok(InventoryList::from_gzip_csv_file(path, url, outfile))
    }

    #[tracing::instrument(skip_all, fields(url = %url))]
    pub(crate) async fn download_object(
        &self,
        url: &S3Location,
        // `md5_digest` must be a 32-character lowercase hexadecimal string
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
                return Err(DownloadError::Verify {
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

#[derive(Debug, Error)]
pub(crate) enum ClientBuildError {
    #[error("failed to create temporary downloads directory")]
    Tempdir(#[from] std::io::Error),
    #[error("failed to fetch AWS credentials")]
    Credentials(#[from] CredentialsError),
}

#[derive(Debug, Error)]
pub(crate) enum TempfileError {
    #[error("failed to create parent directories for tempfile for downloading {url}")]
    Mkdir {
        url: S3Location,
        source: std::io::Error,
    },
    #[error("failed to open tempfile for downloading {url}")]
    Open {
        url: S3Location,
        source: std::io::Error,
    },
}

#[derive(Debug, Error)]
pub(crate) enum FindManifestError {
    #[error(transparent)]
    List(#[from] ListObjectsError),
    #[error("no manifests found in {url}")]
    NoMatch { url: S3Location },
}

#[derive(Debug, Error)]
pub(crate) enum GetManifestError {
    #[error(transparent)]
    Find(#[from] FindManifestError),
    #[error(transparent)]
    Get(#[from] GetError),
    #[error(transparent)]
    Download(#[from] DownloadError),
    #[error("failed downloading checksum at {url}")]
    DownloadChecksum {
        url: S3Location,
        source: ByteStreamError,
    },
    #[error("manifest checksum contents at {url} are not UTF-8")]
    DecodeChecksum {
        url: S3Location,
        source: std::str::Utf8Error,
    },
    #[error(transparent)]
    Tempfile(#[from] TempfileError),
    #[error("failed to rewind tempfile after downloading {url}")]
    Rewind {
        url: S3Location,
        source: std::io::Error,
    },
    #[error("failed to deserialize manifest at {url}")]
    Parse {
        url: S3Location,
        source: serde_json::Error,
    },
}

#[derive(Debug, Error)]
pub(crate) enum DownloadError {
    #[error(transparent)]
    Get(#[from] GetError),
    #[error("failed downloading contents for {url}")]
    Download {
        url: S3Location,
        source: ByteStreamError,
    },
    #[error("failed writing contents of {url} to disk")]
    Write {
        url: S3Location,
        source: std::io::Error,
    },
    #[error("checksum verification for object at {url} failed; expected MD5 {expected_md5:?}, got {actual_md5:?}")]
    Verify {
        url: S3Location,
        expected_md5: String,
        actual_md5: String,
    },
}

#[derive(Debug, Error)]
pub(crate) enum CsvDownloadError {
    #[error(transparent)]
    Tempfile(#[from] TempfileError),
    #[error(transparent)]
    Download(#[from] DownloadError),
    #[error("failed to rewind tempfile after downloading {url}")]
    Rewind {
        url: S3Location,
        source: std::io::Error,
    },
}

#[derive(Debug, Error)]
#[error("failed to get object at {url}")]
pub(crate) struct GetError {
    url: S3Location,
    source: SdkError<GetObjectError, HttpResponse>,
}

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

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("S3 response did not include bucket region")]
pub(crate) struct GetBucketRegionError;

async fn get_credentials() -> Result<Option<Credentials>, CredentialsError> {
    tracing::debug!("Checking for AWS credentials ...");
    let provider = aws_config::default_provider::credentials::default_provider().await;
    match provider.provide_credentials().await {
        Ok(creds) => Ok(Some(creds)),
        Err(CredentialsError::CredentialsNotLoaded(_)) => Ok(None),
        Err(e) => Err(e),
    }
}
