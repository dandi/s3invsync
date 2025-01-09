use crate::consts::METADATA_FILENAME;
use crate::inventory::{InventoryEntry, InventoryItem, ItemDetails};
use crate::manifest::CsvManifest;
use crate::s3::S3Client;
use crate::timestamps::DateHM;
use crate::util::*;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// Capacity of async channels
const CHANNEL_SIZE: usize = 65535;

/// Lock guard returned by [`Syncer::lock_path()`]
type Guard<'a> = <lockable::LockPool<PathBuf> as lockable::Lockable<PathBuf, ()>>::Guard<'a>;

/// Object responsible for syncing an S3 bucket to a local backup by means of
/// the bucket's S3 Inventory
pub(crate) struct Syncer {
    /// The client for interacting with S3
    client: Arc<S3Client>,

    /// The root path of the local backup directory
    outdir: PathBuf,

    /// The timestamp at which the inventory was created on S3
    manifest_date: DateHM,

    /// The time at which the overall backup procedure started
    start_time: std::time::Instant,

    /// The number of concurrent downloads of CSV inventory lists
    inventory_jobs: NonZeroUsize,

    /// The number of concurrent downloads of S3 objects
    object_jobs: NonZeroUsize,

    /// Only download objects whose keys match the given regex
    path_filter: Option<regex::Regex>,

    /// A pool for managing locks on paths
    locks: lockable::LockPool<PathBuf>,

    /// A [`CancellationToken`] used for managing graceful shutdown
    token: CancellationToken,

    /// A clone of the channel used for sending inventory entries off to be
    /// downloaded.  This is set to `None` after spawning all of the inventory
    /// list download tasks.
    obj_sender: Mutex<Option<async_channel::Sender<InventoryItem>>>,

    /// A clone of the channel used for receiving inventory entries to download
    obj_receiver: async_channel::Receiver<InventoryItem>,

    /// Whether the backup was terminated by Ctrl-C
    terminated: AtomicBool,

    /// Object for emitting log messages about objects skipped due to
    /// `--path-filter`
    filterlog: FilterLogger,
}

impl Syncer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        client: S3Client,
        outdir: PathBuf,
        manifest_date: DateHM,
        start_time: std::time::Instant,
        inventory_jobs: NonZeroUsize,
        object_jobs: NonZeroUsize,
        path_filter: Option<regex::Regex>,
        compress_filter_msgs: Option<NonZeroUsize>,
    ) -> Arc<Syncer> {
        let (obj_sender, obj_receiver) = async_channel::bounded(CHANNEL_SIZE);
        Arc::new(Syncer {
            client: Arc::new(client),
            outdir,
            manifest_date,
            start_time,
            inventory_jobs,
            object_jobs,
            path_filter,
            locks: lockable::LockPool::new(),
            token: CancellationToken::new(),
            obj_sender: Mutex::new(Some(obj_sender)),
            obj_receiver,
            terminated: AtomicBool::new(false),
            filterlog: FilterLogger::new(compress_filter_msgs),
        })
    }

    pub(crate) async fn run(self: &Arc<Self>, manifest: CsvManifest) -> Result<(), MultiError> {
        tokio::spawn({
            let this = self.clone();
            async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    tracing::info!("Ctrl-C received; shutting down momentarily ...");
                    this.shutdown();
                    this.terminated.store(true, Ordering::Release);
                }
            }
        });

        tracing::trace!(path = %self.outdir.display(), "Creating root output directory");
        fs_err::create_dir_all(&self.outdir).map_err(|e| MultiError(vec![e.into()]))?;
        let mut joinset = JoinSet::new();
        let (fspec_sender, fspec_receiver) = async_channel::bounded(CHANNEL_SIZE);
        let obj_sender = {
            let guard = self
                .obj_sender
                .lock()
                .expect("obj_sender mutex should not be poisoned");
            guard
                .as_ref()
                .cloned()
                .expect("obj_sender should not be None")
        };

        for _ in 0..self.inventory_jobs.get() {
            let clnt = self.client.clone();
            let token = self.token.clone();
            let recv = fspec_receiver.clone();
            let sender = obj_sender.clone();
            joinset.spawn(async move {
                while let Ok(fspec) = recv.recv().await {
                    let clnt = clnt.clone();
                    let sender = sender.clone();
                    let r = token
                        .run_until_cancelled(async move {
                            let entries = clnt.download_inventory_csv(fspec).await?;
                            for entry in entries {
                                match entry? {
                                    InventoryEntry::Directory(d) => {
                                        tracing::debug!(url = %d.url(), "Ignoring directory entry in inventory list");
                                    }
                                    InventoryEntry::Item(item) => {
                                        if sender.send(item).await.is_err() {
                                            // Assume we're shutting down
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Ok(())
                        })
                        .await;
                    match r {
                        Some(Ok(())) => (),
                        Some(Err(e)) => return Err(e),
                        None => return Ok(()),
                    }
                }
                Ok(())
            });
        }
        drop(obj_sender);
        {
            let mut guard = self
                .obj_sender
                .lock()
                .expect("obj_sender mutex should not be poisoned");
            *guard = None;
        }
        drop(fspec_receiver);

        joinset.spawn(async move {
            for fspec in manifest.files {
                if fspec_sender.send(fspec).await.is_err() {
                    return Ok(());
                }
            }
            Ok(())
        });

        for _ in 0..self.object_jobs.get() {
            let this = self.clone();
            let recv = self.obj_receiver.clone();
            joinset.spawn(async move {
                while let Ok(item) = recv.recv().await {
                    if this.token.is_cancelled() {
                        return Ok(());
                    }
                    Box::pin(this.process_item(item)).await?;
                }
                Ok(())
            });
        }

        let mut errors = Vec::new();
        while let Some(r) = joinset.join_next().await {
            match r {
                Ok(Ok(())) => (),
                Ok(Err(e)) => {
                    tracing::error!(error = ?e, "Error occurred");
                    if errors.is_empty() {
                        tracing::info!("Shutting down in response to error");
                        self.shutdown();
                    }
                    errors.push(e);
                }
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(_) => (),
            }
        }
        self.filterlog.finish();

        if self.terminated.load(Ordering::Acquire) {
            errors.push(anyhow::anyhow!("Shut down due to Ctrl-C"));
        }
        if !errors.is_empty() {
            Err(MultiError(errors))
        } else {
            Ok(())
        }
    }

    fn shutdown(self: &Arc<Self>) {
        if !self.token.is_cancelled() {
            self.token.cancel();
            self.obj_receiver.close();
            self.log_process_info();
        }
    }

    #[tracing::instrument(skip_all, fields(url = %item.url()))]
    async fn process_item(&self, item: InventoryItem) -> anyhow::Result<()> {
        if let Some(ref rgx) = self.path_filter {
            if !rgx.is_match(&item.key) {
                self.filterlog.log();
                return Ok(());
            }
        }
        tracing::info!("Processing object");

        let etag = match item.details {
            ItemDetails::Present { ref etag, .. } => etag,
            ItemDetails::Deleted => {
                tracing::info!("Object is delete marker; not doing anything");
                return Ok(());
            }
        };
        let md = Metadata {
            version_id: item.version_id.clone(),
            etag: etag.to_owned(),
        };

        let (dirname, filename) = item.key.split();
        let parentdir = if let Some(p) = dirname {
            let pd = self.outdir.join(p);
            tracing::trace!(path = %pd.display(), "Creating output directory");
            force_create_dir_all(&self.outdir, p.split('/'))?;
            pd
        } else {
            self.outdir.clone()
        };
        let mdmanager = MetadataManager::new(self, &parentdir, filename);

        if item.is_latest {
            tracing::info!("Object is latest version of key");
            let latest_path = parentdir.join(filename);
            let _guard = self.lock_path(latest_path.clone()).await;
            if ensure_file(&latest_path).await? {
                let current_md = mdmanager
                    .get()
                    .await
                    .with_context(|| format!("failed to get local metadata for {}", item.url()))?;
                if md == current_md {
                    tracing::info!(path = %latest_path.display(), "Backup path already exists and metadata matches; doing nothing");
                } else {
                    tracing::info!(path = %latest_path.display(), "Backup path already exists but metadata does not match; renaming current file and downloading correct version");
                    self.move_object_file(
                        &latest_path,
                        &parentdir.join(current_md.old_filename(filename)),
                    )?;
                    if self.download_item(&item, &parentdir, latest_path).await? {
                        mdmanager.set(md).await.with_context(|| {
                            format!("failed to set local metadata for {}", item.url())
                        })?;
                    }
                }
            } else {
                let oldpath = parentdir.join(md.old_filename(filename));
                if ensure_file(&oldpath).await? {
                    tracing::info!(path = %latest_path.display(), oldpath = %oldpath.display(), "Backup path does not exist but \"old\" path does; will rename");
                    self.move_object_file(&oldpath, &latest_path)?;
                    mdmanager.set(md).await.with_context(|| {
                        format!("failed to set local metadata for {}", item.url())
                    })?;
                } else {
                    tracing::info!(path = %latest_path.display(), "Backup path does not exist; will download");
                    if self.download_item(&item, &parentdir, latest_path).await? {
                        mdmanager.set(md).await.with_context(|| {
                            format!("failed to set local metadata for {}", item.url())
                        })?;
                    }
                }
            }
        } else {
            tracing::info!("Object is old version of key");
            let oldpath = parentdir.join(md.old_filename(filename));
            if ensure_file(&oldpath).await? {
                tracing::info!(path = %oldpath.display(), "Backup path already exists; doing nothing");
            } else {
                let latest_path = parentdir.join(filename);
                let guard = self.lock_path(latest_path.clone()).await;
                if ensure_file(&latest_path).await?
                    && md
                        == mdmanager.get().await.with_context(|| {
                            format!(
                                "failed to get local metadata for latest version of {}",
                                item.url()
                            )
                        })?
                {
                    tracing::info!(path = %oldpath.display(), "Backup path does not exist, but \"latest\" file has matching metadata; renaming \"latest\" file");
                    self.move_object_file(&latest_path, &oldpath)?;
                    mdmanager.delete().await.with_context(|| {
                        format!(
                            "failed to delete local metadata for latest version of {}",
                            item.url()
                        )
                    })?;
                } else {
                    tracing::info!(path = %oldpath.display(), "Backup path does not exist; will download");
                    // No need for locking here, as this is an "old" path that
                    // doesn't exist, so no other tasks should be working on
                    // it.
                    drop(guard);
                    self.download_item(&item, &parentdir, oldpath).await?;
                }
            }
        }
        tracing::info!("Finished processing object");
        Ok(())
    }

    fn move_object_file(&self, src: &Path, dest: &Path) -> std::io::Result<()> {
        tracing::debug!(src = %src.display(), dest = %dest.display(), "Moving object file");
        fs_err::rename(src, dest)
    }

    #[tracing::instrument(skip_all)]
    async fn download_item(
        &self,
        item: &InventoryItem,
        parentdir: &Path,
        path: PathBuf,
    ) -> anyhow::Result<bool> {
        tracing::trace!("Opening temporary output file");
        let outfile = tempfile::Builder::new()
            .prefix(".s3invsync.download.")
            .tempfile_in(parentdir)
            .with_context(|| {
                format!("failed to create temporary output file for {}", item.url())
            })?;
        match self
            .token
            .run_until_cancelled(self.client.download_object(
                &item.url(),
                item.details.md5_digest(),
                outfile.as_file(),
            ))
            .await
        {
            Some(Ok(())) => {
                tracing::trace!(dest = %path.display(), "Moving temporary output file to destination");
                let fp = outfile.persist(&path).with_context(|| {
                    format!(
                        "failed to persist temporary output file to {}",
                        path.display()
                    )
                })?;
                if let Some(mtime) = item.last_modified_date {
                    fp.set_modified(mtime.into())
                        .with_context(|| format!("failed to set mtime on {}", path.display()))?;
                }
                Ok(true)
            }
            Some(Err(e)) => {
                let e = anyhow::Error::from(e);
                tracing::error!(error = ?e, "Failed to download object");
                if let Err(e2) = self.cleanup_download_path(item, outfile, &path) {
                    tracing::warn!(error = ?e2, "Failed to clean up download path");
                }
                Err(e)
            }
            None => {
                tracing::debug!("Download cancelled");
                self.cleanup_download_path(item, outfile, &path)?;
                Ok(false)
            }
        }
    }

    #[tracing::instrument(skip_all, fields(path = %dlfile.display()))]
    fn cleanup_download_path(
        &self,
        item: &InventoryItem,
        outfile: tempfile::NamedTempFile,
        dlfile: &Path,
    ) -> anyhow::Result<()> {
        // TODO: Synchronize calls to this method?
        tracing::debug!("Cleaning up unfinished download file");
        outfile.close().with_context(|| {
            format!(
                "failed to remove temporary download file for {}",
                item.url()
            )
        })?;
        if let Some(dirpath) = dlfile.parent() {
            rmdir_to_root(dirpath, &self.outdir)?;
        }
        tracing::debug!("Finished cleaning up unfinished download file");
        Ok(())
    }

    async fn lock_path(&self, path: PathBuf) -> Guard<'_> {
        tracing::trace!(path = %path.display(), "Acquiring internal lock for path");
        self.locks.async_lock(path).await
    }

    fn log_process_info(&self) {
        let (physical_mem, virtual_mem) = match memory_stats::memory_stats() {
            Some(st) => (Some(st.physical_mem), Some(st.virtual_mem)),
            None => (None, None),
        };
        tracing::info!(
            version = env!("CARGO_PKG_VERSION"),
            git_commit = option_env!("GIT_COMMIT"),
            manifest_date = %self.manifest_date,
            elapsed = ?self.start_time.elapsed(),
            physical_mem,
            virtual_mem,
            "Process info",
        );
    }
}

/// Metadata about the latest version of a key
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Metadata {
    /// The object's version ID
    version_id: String,

    /// The object's etag
    etag: String,
}

impl Metadata {
    /// Return the filename used for backing up a non-latest object that has
    /// `self` as its metadata and `basename` as the filename portion of its
    /// key
    fn old_filename(&self, basename: &str) -> String {
        format!("{}.old.{}.{}", basename, self.version_id, self.etag)
    }
}

/// Handle for manipulating the metadata for the latest version of a key in a
/// local JSON database
struct MetadataManager<'a> {
    syncer: &'a Syncer,

    /// The local directory in which the downloaded object and the JSON
    /// database are both located
    dirpath: &'a Path,

    /// The path to the JSON database
    database_path: PathBuf,

    /// The filename of the object
    filename: &'a str,
}

impl<'a> MetadataManager<'a> {
    fn new(syncer: &'a Syncer, parentdir: &'a Path, filename: &'a str) -> Self {
        MetadataManager {
            syncer,
            dirpath: parentdir,
            database_path: parentdir.join(METADATA_FILENAME),
            filename,
        }
    }

    /// Acquire a lock on this JSON database
    async fn lock(&self) -> Guard<'a> {
        self.syncer.lock_path(self.database_path.clone()).await
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

    /// Retrieve the metadata for the key from the database
    async fn get(&self) -> anyhow::Result<Metadata> {
        tracing::trace!(file = self.filename, database = %self.database_path.display(), "Fetching object metadata for file from database");
        let mut data = {
            let _guard = self.lock().await;
            self.load()?
        };
        let Some(md) = data.remove(self.filename) else {
            anyhow::bail!(
                "No entry for {:?} in {}",
                self.filename,
                self.database_path.display()
            );
        };
        Ok(md)
    }

    /// Set the metadata for the key in the database to `md`
    async fn set(&self, md: Metadata) -> anyhow::Result<()> {
        tracing::trace!(file = self.filename, database = %self.database_path.display(), "Setting object metadata for file in database");
        let _guard = self.lock().await;
        let mut data = self.load()?;
        data.insert(self.filename.to_owned(), md);
        self.store(data)?;
        Ok(())
    }

    /// Remove the metadata for the key from the database
    async fn delete(&self) -> anyhow::Result<()> {
        tracing::trace!(file = self.filename, database = %self.database_path.display(), "Deleting object metadata for file from database");
        let _guard = self.lock().await;
        let mut data = self.load()?;
        if data.remove(self.filename).is_some() {
            self.store(data)?;
        }
        Ok(())
    }
}

/// An emitter of log messages about objects skipped due to `--path-filter`
#[derive(Debug)]
enum FilterLogger {
    /// Log a message for every object
    All,

    /// Log one message for every `period` objects skipped
    Compressed {
        period: NonZeroUsize,
        progress: Mutex<usize>,
    },
}

impl FilterLogger {
    fn new(compression: Option<NonZeroUsize>) -> FilterLogger {
        if let Some(period) = compression {
            FilterLogger::Compressed {
                period,
                progress: Mutex::new(0),
            }
        } else {
            FilterLogger::All
        }
    }

    /// Called whenever an object is skipped due to its key not matching
    /// `--path-filter`.  If `self` is `All`, a log message is emitted.  If
    /// `self` is `Compressed`, a log message is only emitted if there have
    /// been a multiple of `period` objects skipped so far.
    fn log(&self) {
        match self {
            FilterLogger::All => {
                tracing::info!("Object key does not match --path-filter; skipping");
            }
            FilterLogger::Compressed { period, progress } => {
                let new_progress = {
                    let mut guard = progress
                        .lock()
                        .expect("FilterLogger mutex should not be poisoned");
                    *guard += 1;
                    *guard
                };
                if new_progress % period.get() == 0 {
                    tracing::info!("Skipped {new_progress} keys that did not match --path-filter");
                }
            }
        }
    }

    /// Called after all items have been processed.  If `self` is `Compressed`
    /// and the number of objects skipped is not a multiple of `period`, a
    /// message is logged for the remainder.
    fn finish(&self) {
        if let FilterLogger::Compressed { period, progress } = self {
            let progress_ = {
                let guard = progress
                    .lock()
                    .expect("FilterLogger mutex should not be poisoned");
                *guard
            };
            if progress_ % period.get() != 0 {
                tracing::info!("Skipped {progress_} keys that did not match --path-filter");
            }
        }
    }
}
