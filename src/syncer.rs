use crate::asyncutil::LimitedShutdownGroup;
use crate::consts::METADATA_FILENAME;
use crate::inventory::{InventoryItem, ItemDetails};
use crate::manifest::CsvManifest;
use crate::s3::S3Client;
use crate::util::*;
use anyhow::Context;
use fs_err::PathExt;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio_util::sync::CancellationToken;

type Guard<'a> = <lockable::LockPool<PathBuf> as lockable::Lockable<PathBuf, ()>>::Guard<'a>;

pub(crate) struct Syncer {
    client: Arc<S3Client>,
    outdir: PathBuf,
    inventory_jobs: NonZeroUsize,
    object_jobs: NonZeroUsize,
    path_filter: Option<regex::Regex>,
    locks: lockable::LockPool<PathBuf>,
}

impl Syncer {
    pub(crate) fn new(
        client: S3Client,
        outdir: PathBuf,
        inventory_jobs: NonZeroUsize,
        object_jobs: NonZeroUsize,
        path_filter: Option<regex::Regex>,
    ) -> Arc<Syncer> {
        Arc::new(Syncer {
            client: Arc::new(client),
            outdir,
            inventory_jobs,
            object_jobs,
            path_filter,
            locks: lockable::LockPool::new(),
        })
    }

    pub(crate) async fn run(self: &Arc<Self>, manifest: CsvManifest) -> Result<(), MultiError> {
        let mut inventory_dl_pool = LimitedShutdownGroup::new(self.inventory_jobs.get());
        let mut object_dl_pool = LimitedShutdownGroup::new(self.object_jobs.get());
        let (obj_sender, mut obj_receiver) = channel(self.inventory_jobs.get());

        for fspec in manifest.files {
            let clnt = self.client.clone();
            let sender = obj_sender.clone();
            inventory_dl_pool.spawn(move |token| async move {
                token
                    .run_until_cancelled(async move {
                        let itemlist = clnt.download_inventory_csv(fspec).await?;
                        for item in itemlist {
                            if sender.send(item?).await.is_err() {
                                // Assume we're shutting down
                                return Ok(());
                            }
                        }
                        Ok(())
                    })
                    .await
                    .unwrap_or(Ok(()))
            });
        }
        inventory_dl_pool.close();
        drop(obj_sender);

        let mut errors = Vec::new();
        let mut inventory_pool_finished = false;
        let mut object_pool_finished = false;
        let mut all_objects_txed = false;
        loop {
            tokio::select! {
                r = inventory_dl_pool.next(), if !inventory_pool_finished => {
                    match r {
                        Some(Ok(())) => (),
                        Some(Err(e)) => {
                            tracing::error!(error = ?e, "error processing inventory lists");
                            if errors.is_empty() {
                                inventory_dl_pool.shutdown();
                                object_dl_pool.shutdown();
                            }
                            errors.push(e);
                        }
                        None => {
                            tracing::debug!("Finished processing inventory lists");
                            inventory_pool_finished = true;
                            object_dl_pool.close();
                        }
                    }
                }
                r = object_dl_pool.next(), if !object_pool_finished => {
                    match r {
                        Some(Ok(())) => (),
                        Some(Err(e)) => {
                            tracing::error!(error = ?e, "error processing objects");
                            if errors.is_empty() {
                                inventory_dl_pool.shutdown();
                                object_dl_pool.shutdown();
                            }
                            errors.push(e);
                        }
                        None => {
                            tracing::debug!("Finished processing objects");
                            object_pool_finished = true;
                        }
                    }
                }
                r = obj_receiver.recv(), if !all_objects_txed => {
                    if let Some(item) = r {
                        let this = self.clone();
                        object_dl_pool
                            .spawn(move |token| async move { this.process_item(item, token).await });
                    } else {
                        all_objects_txed = true;
                    }
                }
                else => break,
            }
        }

        if !errors.is_empty() {
            Err(MultiError(errors))
        } else {
            Ok(())
        }
    }

    #[tracing::instrument(skip_all, fields(url = %item.url()))]
    async fn process_item(
        self: &Arc<Self>,
        item: InventoryItem,
        token: CancellationToken,
    ) -> anyhow::Result<()> {
        if token.is_cancelled() {
            return Ok(());
        }
        if let Some(ref rgx) = self.path_filter {
            if !rgx.is_match(&item.key) {
                tracing::info!("Object key does not match --path-filter; skipping");
                return Ok(());
            }
        }
        tracing::info!("Processing object");

        let etag = match item.details {
            ItemDetails::Present { ref etag, .. } => etag,
            ItemDetails::Deleted => {
                tracing::debug!("Object is delete marker; not doing anything");
                return Ok(());
            }
        };
        let md = Metadata {
            version_id: item.version_id.clone(),
            etag: etag.to_owned(),
        };

        check_normed_posix_path(&item.key)?;
        let (dirname, filename) = match item.key.rsplit_once('/') {
            Some((pre, post)) => (Some(pre), post),
            None => (None, &*item.key),
        };
        check_special_filename(filename)?;
        let parentdir = if let Some(p) = dirname {
            self.outdir.join(p)
        } else {
            self.outdir.clone()
        };
        tracing::trace!(path = %parentdir.display(), "Creating output directory");
        fs_err::create_dir_all(&parentdir)?;
        let mdmanager = MetadataManager::new(self, &parentdir, filename);

        if item.is_latest {
            tracing::debug!("Object is latest version of key");
            let latest_path = parentdir.join(filename);
            if latest_path.fs_err_try_exists()? {
                let current_md = mdmanager.get().await?;
                if md == current_md {
                    tracing::debug!(path = %latest_path.display(), "Backup path already exists and metadata matches; doing nothing");
                } else {
                    tracing::debug!(path = %latest_path.display(), "Backup path already exists but metadata does not match; renaming current file and downloading correct version");
                    // TODO: Add cancellation & cleanup logic around the rest
                    // of this block:
                    let guard = self.lock_path(latest_path.clone());
                    self.move_object_file(
                        &latest_path,
                        &parentdir.join(current_md.old_filename(filename)),
                    )?;
                    self.download_item(&item, &parentdir, latest_path, token)
                        .await?;
                    drop(guard);
                    mdmanager.set(md).await?;
                }
            } else {
                let oldpath = parentdir.join(md.old_filename(filename));
                if oldpath.fs_err_try_exists()? {
                    tracing::debug!(path = %latest_path.display(), oldpath = %oldpath.display(), "Backup path does not exist but \"old\" path does; will rename");
                    // TODO: Add cancellation & cleanup logic around the rest
                    // of this block:
                    let guard = self.lock_path(latest_path.clone());
                    self.move_object_file(&oldpath, &latest_path)?;
                    drop(guard);
                    mdmanager.set(md).await?;
                } else {
                    tracing::debug!(path = %latest_path.display(), "Backup path does not exist; will download");
                    // TODO: Add cancellation & cleanup logic around the rest
                    // of this block:
                    let guard = self.lock_path(latest_path.clone());
                    self.download_item(&item, &parentdir, latest_path, token)
                        .await?;
                    drop(guard);
                    mdmanager.set(md).await?;
                }
            }
        } else {
            tracing::debug!("Object is old version of key");
            let oldpath = parentdir.join(md.old_filename(filename));
            if oldpath.fs_err_try_exists()? {
                tracing::debug!(path = %oldpath.display(), "Backup path already exists; doing nothing");
            } else {
                let latest_path = parentdir.join(filename);
                if latest_path.fs_err_try_exists()? && md == mdmanager.get().await? {
                    tracing::debug!(path = %oldpath.display(), "Backup path does not exist, but \"latest\" file has matching metadata; renaming \"latest\" file");
                    // TODO: Add cancellation & cleanup logic around the rest
                    // of this block:
                    let guard = self.lock_path(latest_path.clone());
                    self.move_object_file(&latest_path, &oldpath)?;
                    drop(guard);
                    mdmanager.delete().await?;
                } else {
                    tracing::debug!(path = %oldpath.display(), "Backup path does not exist; will download");
                    // No need for locking here, as this is an "old" path that
                    // doesn't exist, so no other tasks should be working on
                    // it.
                    self.download_item(&item, &parentdir, oldpath, token)
                        .await?;
                }
            }
        }
        Ok(())
    }

    fn move_object_file(&self, src: &Path, dest: &Path) -> std::io::Result<()> {
        tracing::debug!(src = %src.display(), dest = %dest.display(), "Moving object file");
        fs_err::rename(src, dest)
    }

    async fn download_item(
        &self,
        item: &InventoryItem,
        parentdir: &Path,
        path: PathBuf,
        token: CancellationToken,
    ) -> anyhow::Result<()> {
        tracing::trace!("Opening temporary output file");
        let outfile = tempfile::Builder::new()
            .prefix(".s3invsync.download.")
            .tempfile_in(parentdir)
            .with_context(|| {
                format!("failed to create temporary output file for {}", item.url())
            })?;
        match token
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
                fp.set_modified(item.last_modified_date.into())
                    .with_context(|| format!("failed to set mtime on {}", path.display()))?;
                Ok(())
            }
            Some(Err(e)) => {
                tracing::error!(error = ?e, "Failed to download object");
                if let Err(e2) = self.cleanup_download_path(item, outfile, &path) {
                    tracing::warn!(error = ?e2, "Failed to clean up download path");
                }
                Err(e.into())
            }
            None => {
                tracing::debug!("Download cancelled");
                self.cleanup_download_path(item, outfile, &path)
                    .map_err(Into::into)
            }
        }
    }

    fn cleanup_download_path(
        &self,
        item: &InventoryItem,
        outfile: tempfile::NamedTempFile,
        dlfile: &Path,
    ) -> anyhow::Result<()> {
        // TODO: Synchronize calls to this method?
        tracing::trace!(path = %dlfile.display(), "Cleaning up unfinished download file");
        outfile.close().with_context(|| {
            format!(
                "failed to remove temporary download file for {}",
                item.url()
            )
        })?;
        let p = dlfile.parent();
        while let Some(pp) = p {
            if pp == self.outdir {
                break;
            }
            if is_empty_dir(pp)? {
                match fs_err::remove_dir(pp) {
                    Ok(()) => (),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(())
    }

    async fn lock_path(&self, path: PathBuf) -> Guard<'_> {
        self.locks.async_lock(path).await
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Metadata {
    version_id: String,
    etag: String,
}

impl Metadata {
    fn old_filename(&self, basename: &str) -> String {
        format!("{}.old.{}.{}", basename, self.version_id, self.etag)
    }
}

struct MetadataManager<'a> {
    syncer: &'a Syncer,
    database_path: PathBuf,
    filename: &'a str,
}

impl<'a> MetadataManager<'a> {
    fn new(syncer: &'a Syncer, parentdir: &Path, filename: &'a str) -> Self {
        MetadataManager {
            syncer,
            database_path: parentdir.join(METADATA_FILENAME),
            filename,
        }
    }

    async fn lock(&self) -> Guard<'a> {
        self.syncer.lock_path(self.database_path.clone()).await
    }

    fn load(&self) -> anyhow::Result<BTreeMap<String, Metadata>> {
        let content = match fs_err::read_to_string(&self.database_path) {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::from("{}"),
            Err(e) => return Err(e.into()),
        };
        serde_json::from_str(&content).with_context(|| {
            format!(
                "failed to deserialize contents of {}",
                self.database_path.display()
            )
        })
    }

    fn store(&self, data: BTreeMap<String, Metadata>) -> anyhow::Result<()> {
        let fp = fs_err::File::create(&self.database_path)?;
        // TODO: Write to tempfile and then move
        serde_json::to_writer_pretty(fp, &data)
            .with_context(|| {
                format!(
                    "failed to serialize metadata to {}",
                    self.database_path.display()
                )
            })
            .map_err(Into::into)
    }

    async fn get(&self) -> anyhow::Result<Metadata> {
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

    async fn set(&self, md: Metadata) -> anyhow::Result<()> {
        let _guard = self.lock().await;
        let mut data = self.load()?;
        data.insert(self.filename.to_owned(), md);
        self.store(data)?;
        Ok(())
    }

    async fn delete(&self) -> anyhow::Result<()> {
        let _guard = self.lock().await;
        let mut data = self.load()?;
        if data.remove(self.filename).is_some() {
            self.store(data)?;
        }
        Ok(())
    }
}
