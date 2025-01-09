use super::*;
use serde::{Deserialize, Serialize};

/// Metadata about the latest version of a key
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct Metadata {
    /// The object's version ID
    pub(super) version_id: String,

    /// The object's etag
    pub(super) etag: String,
}

impl Metadata {
    /// Return the filename used for backing up a non-latest object that has
    /// `self` as its metadata and `basename` as the filename portion of its
    /// key
    pub(super) fn old_filename(&self, basename: &str) -> String {
        format!("{}.old.{}.{}", basename, self.version_id, self.etag)
    }
}

/// Handle for manipulating the metadata for the latest version of a key in a
/// local JSON database
pub(super) struct FileMetadataManager<'a> {
    syncer: &'a Syncer,

    /// The manager for the directory's database
    inner: MetadataManager<'a>,

    /// The filename of the object
    filename: &'a str,
}

impl<'a> FileMetadataManager<'a> {
    pub(super) fn new(syncer: &'a Syncer, parentdir: &'a Path, filename: &'a str) -> Self {
        FileMetadataManager {
            syncer,
            inner: MetadataManager::new(parentdir),
            filename,
        }
    }

    /// Acquire a lock on this JSON database
    async fn lock(&self) -> Guard<'a> {
        self.syncer.lock_path(self.database_path().to_owned()).await
    }

    fn database_path(&self) -> &Path {
        &self.inner.database_path
    }

    /// Retrieve the metadata for the key from the database
    pub(super) async fn get(&self) -> anyhow::Result<Metadata> {
        tracing::trace!(file = self.filename, database = %self.database_path().display(), "Fetching object metadata for file from database");
        let mut data = {
            let _guard = self.lock().await;
            self.inner.load()?
        };
        let Some(md) = data.remove(self.filename) else {
            anyhow::bail!(
                "No entry for {:?} in {}",
                self.filename,
                self.database_path().display()
            );
        };
        Ok(md)
    }

    /// Set the metadata for the key in the database to `md`
    pub(super) async fn set(&self, md: Metadata) -> anyhow::Result<()> {
        tracing::trace!(file = self.filename, database = %self.database_path().display(), "Setting object metadata for file in database");
        let _guard = self.lock().await;
        let mut data = self.inner.load()?;
        data.insert(self.filename.to_owned(), md);
        self.inner.store(data)?;
        Ok(())
    }

    /// Remove the metadata for the key from the database
    pub(super) async fn delete(&self) -> anyhow::Result<()> {
        tracing::trace!(file = self.filename, database = %self.database_path().display(), "Deleting object metadata for file from database");
        let _guard = self.lock().await;
        let mut data = self.inner.load()?;
        if data.remove(self.filename).is_some() {
            self.inner.store(data)?;
        }
        Ok(())
    }
}

/// Handle for manipulating the metadata a local JSON database
pub(super) struct MetadataManager<'a> {
    /// The local directory in which the downloaded object and the JSON
    /// database are both located
    dirpath: &'a Path,

    /// The path to the JSON database
    database_path: PathBuf,
}

impl<'a> MetadataManager<'a> {
    pub(super) fn new(dirpath: &'a Path) -> MetadataManager<'a> {
        MetadataManager {
            dirpath,
            database_path: dirpath.join(METADATA_FILENAME),
        }
    }

    /// Read & parse the database file.  If the file does not exist, return an
    /// empty map.
    fn load(&self) -> anyhow::Result<BTreeMap<String, Metadata>> {
        let content = match fs_err::read_to_string(&self.database_path) {
            Ok(content) => content,
            Err(e) if e.kind() == ErrorKind::NotFound => String::from("{}"),
            Err(e) => return Err(e.into()),
        };
        serde_json::from_str(&content).with_context(|| {
            format!(
                "failed to deserialize contents of {}",
                self.database_path.display()
            )
        })
    }

    /// Set the content of the database file to the serialized map
    fn store(&self, data: BTreeMap<String, Metadata>) -> anyhow::Result<()> {
        let fp = tempfile::Builder::new()
            .prefix(".s3invsync.versions.")
            .tempfile_in(self.dirpath)
            .with_context(|| {
                format!(
                    "failed to create temporary database file for updating {}",
                    self.database_path.display()
                )
            })?;
        serde_json::to_writer_pretty(fp.as_file(), &data).with_context(|| {
            format!(
                "failed to serialize metadata to {}",
                self.database_path.display()
            )
        })?;
        fp.persist(&self.database_path).with_context(|| {
            format!(
                "failed to persist temporary database file to {}",
                self.database_path.display()
            )
        })?;
        Ok(())
    }
}
