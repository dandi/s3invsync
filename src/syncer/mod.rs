mod metadata;
mod treetracker;
use self::metadata::*;
use self::treetracker::*;
use crate::consts::METADATA_FILENAME;
use crate::inventory::{InventoryEntry, InventoryItem, ItemDetails};
use crate::keypath::is_special_component;
use crate::manifest::{CsvManifest, FileSpec};
use crate::nursery::{Nursery, NurseryStream};
use crate::s3::S3Client;
use crate::timestamps::DateHM;
use crate::util::*;
use anyhow::Context;
use futures_util::StreamExt;
use std::collections::BTreeMap;
use std::future::Future;
use std::io::ErrorKind;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Capacity of async channels
const CHANNEL_SIZE: usize = 65535;

/// Lock guard returned by [`Syncer::lock_path()`]
type Guard<'a> = <lockable::LockPool<PathBuf> as lockable::Lockable<PathBuf, ()>>::Guard<'a>;

type ObjChannelItem = (InventoryItem, Option<Arc<Notify>>);

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

    /// The number of concurrent downloads jobs
    jobs: NonZeroUsize,

    /// Only download objects whose keys match the given regex
    path_filter: Option<regex::Regex>,

    /// A pool for managing locks on paths
    locks: lockable::LockPool<PathBuf>,

    /// A [`CancellationToken`] used for managing graceful shutdown
    token: CancellationToken,

    /// A clone of the channel used for sending inventory entries off to be
    /// downloaded.  This is set to `None` after spawning all of the inventory
    /// list download tasks.
    obj_sender: Mutex<Option<async_channel::Sender<ObjChannelItem>>>,

    /// A clone of the channel used for receiving inventory entries to download
    obj_receiver: async_channel::Receiver<ObjChannelItem>,

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
        jobs: NonZeroUsize,
        path_filter: Option<regex::Regex>,
        compress_filter_msgs: Option<NonZeroUsize>,
    ) -> Arc<Syncer> {
        let (obj_sender, obj_receiver) = async_channel::bounded(CHANNEL_SIZE);
        Arc::new(Syncer {
            client: Arc::new(client),
            outdir,
            manifest_date,
            start_time,
            jobs,
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

        let fspecs = self.sort_csvs_by_first_line(manifest.files).await?;

        tracing::trace!(path = %self.outdir.display(), "Creating root output directory");
        fs_err::create_dir_all(&self.outdir).map_err(|e| MultiError(vec![e.into()]))?;
        let (nursery, nursery_stream) = Nursery::new();
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

        let this = self.clone();
        let sender = obj_sender.clone();
        let subnursery = nursery.clone();
        nursery.spawn(
            self.until_cancelled_ok(async move {
                let mut tracker = TreeTracker::new();
                for spec in fspecs {
                    let entries = this.client.download_inventory_csv(spec).await?;
                    for entry in entries {
                        match entry.context("error reading from inventory list file")? {
                            InventoryEntry::Directory(d) => {
                                tracing::debug!(url = %d.url(), "Ignoring directory entry in inventory list");
                            }
                            InventoryEntry::Item(item) => {
                                let notify = if !item.is_deleted() {
                                    let notify = Arc::new(Notify::new());
                                    for dir in tracker.add(&item.key, notify.clone(), item.old_filename())? {
                                        subnursery.spawn({
                                            this.until_cancelled_ok({
                                                let this = this.clone();
                                                async move { this.cleanup_dir(dir).await }
                                            })
                                        });
                                    }
                                    Some(notify)
                                } else {
                                    None
                                };
                                if sender.send((item, notify)).await.is_err() {
                                    // Assume we're shutting down
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
                for dir in tracker.finish() {
                    subnursery.spawn({
                        this.until_cancelled_ok({
                            let this = this.clone();
                            async move { this.cleanup_dir(dir).await }
                        })
                    });
                }
                Ok(())
            })
        );
        drop(obj_sender);
        {
            let mut guard = self
                .obj_sender
                .lock()
                .expect("obj_sender mutex should not be poisoned");
            *guard = None;
        }

        for _ in 0..self.jobs.get() {
            let this = self.clone();
            let recv = self.obj_receiver.clone();
            nursery.spawn(async move {
                while let Ok((item, notify)) = recv.recv().await {
                    if this.token.is_cancelled() {
                        return Ok(());
                    }
                    let r = Box::pin(this.process_item(item)).await;
                    if let Some(n) = notify {
                        n.notify_one();
                    }
                    r?;
                }
                Ok(())
            });
        }

        drop(nursery);
        let r = self.await_nursery(nursery_stream).await;
        self.filterlog.finish();
        r
    }

    /// Fetch the first line of each inventory list file in `specs` and sort
    /// the list by the keys in those lines
    async fn sort_csvs_by_first_line(
        self: &Arc<Self>,
        specs: Vec<FileSpec>,
    ) -> Result<Vec<FileSpec>, MultiError> {
        tracing::info!("Peeking at inventory lists in order to sort by first line ...");
        let (nursery, nursery_stream) = Nursery::new();
        let mut receiver = {
            let specs = Arc::new(Mutex::new(specs));
            let (output_sender, output_receiver) = tokio::sync::mpsc::channel(CHANNEL_SIZE);
            for _ in 0..self.jobs.get() {
                let clnt = self.client.clone();
                let specs = specs.clone();
                let sender = output_sender.clone();
                nursery.spawn(self.until_cancelled_ok(async move {
                    while let Some(fspec) = {
                        let mut guard = specs.lock().expect("specs mutex should not be poisoned");
                        guard.pop()
                    } {
                        if let Some(entry) = clnt.peek_inventory_csv(&fspec).await? {
                            if sender.send((fspec, entry)).await.is_err() {
                                // Assume we're shutting down
                                return Ok(());
                            }
                        }
                    }
                    Ok(())
                }));
            }
            output_receiver
        };
        drop(nursery);
        let mut firsts2fspecs = BTreeMap::new();
        while let Some((fspec, entry)) = receiver.recv().await {
            firsts2fspecs.insert(entry.key().to_owned(), fspec);
        }
        self.await_nursery(nursery_stream).await?;
        Ok(firsts2fspecs.into_values().collect())
    }

    /// Run the given future to completion, cancelling it if `token` is
    /// cancelled, in which case `Ok(())` is returned.
    fn until_cancelled_ok<Fut>(
        &self,
        fut: Fut,
    ) -> impl Future<Output = anyhow::Result<()>> + Send + 'static
    where
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        // Use an async block instead of making the method async so that the
        // future won't capture &self and thus will be 'static
        let token = self.token.clone();
        async move { token.run_until_cancelled(fut).await.unwrap_or(Ok(())) }
    }

    /// Wait for all tasks in a nursery to complete.  If any errors occur,
    /// [`Syncer::shutdown()`] is called, and a [`MultiError`] of all errors
    /// (including a message about Ctrl-C being received if that happened) is
    /// returned.
    async fn await_nursery(
        &self,
        mut stream: NurseryStream<anyhow::Result<()>>,
    ) -> Result<(), MultiError> {
        let mut errors = Vec::new();
        while let Some(r) = stream.next().await {
            if let Err(e) = r {
                tracing::error!(error = ?e, "Error occurred");
                if errors.is_empty() {
                    tracing::info!("Shutting down in response to error");
                    self.shutdown();
                }
                errors.push(e);
            }
        }
        if self.terminated.load(Ordering::Acquire) {
            errors.push(anyhow::anyhow!("Shut down due to Ctrl-C"));
        }
        if !errors.is_empty() {
            Err(MultiError(errors))
        } else {
            Ok(())
        }
    }

    fn shutdown(&self) {
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
        let mdmanager = FileMetadataManager::new(self, &parentdir, filename);

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

    #[tracing::instrument(skip_all, fields(dirpath = %dir.path().unwrap_or("<root>")))]
    async fn cleanup_dir(&self, dir: Directory<Arc<Notify>>) -> anyhow::Result<()> {
        let mut notifiers = Vec::new();
        let dir = dir.map(|n| {
            notifiers.push(n);
        });
        for n in notifiers {
            n.notified().await;
        }
        let dirpath = match dir.path() {
            Some(p) => self.outdir.join(p),
            None => self.outdir.clone(),
        };
        let mut files_to_delete = Vec::new();
        let mut dirs_to_delete = Vec::new();
        let mut dbdeletions = Vec::new();
        let iter = match fs_err::read_dir(&dirpath) {
            Ok(iter) => iter,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        for entry in iter {
            let entry = entry?;
            let is_dir = entry.file_type()?.is_dir();
            let to_delete = match entry.file_name().to_str() {
                Some(name) => {
                    if is_dir {
                        !dir.contains_dir(name)
                    } else {
                        if !is_special_component(name) {
                            dbdeletions.push(name.to_owned());
                        }
                        !dir.contains_file(name) && name != METADATA_FILENAME
                    }
                }
                None => true,
            };
            if to_delete {
                if is_dir {
                    dirs_to_delete.push(entry.path());
                } else {
                    files_to_delete.push(entry.path());
                }
            }
        }
        for p in files_to_delete {
            tracing::debug!(path = %p.display(), "File does not belong in backup; deleting");
            if let Err(e) = fs_err::remove_file(&p) {
                tracing::warn!(error = %e, path = %p.display(), "Failed to delete file");
            }
        }
        for p in dirs_to_delete {
            tracing::debug!(path = %p.display(), "Directory does not belong in backup; deleting");
            if let Err(e) = fs_err::tokio::remove_dir_all(&p).await {
                tracing::warn!(error = %e, path = %p.display(), "Failed to delete directory");
            }
        }
        if !dbdeletions.is_empty() {
            let manager = MetadataManager::new(&dirpath);
            let mut data = manager.load()?;
            for name in dbdeletions {
                data.remove(&name);
            }
            manager.store(data)?;
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
