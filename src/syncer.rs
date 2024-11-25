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
use std::fs::File;
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

        let actions = if item.is_latest {
            tracing::debug!("Object is latest version of key");
            let latest_path = parentdir.join(filename);
            if latest_path.fs_err_try_exists()? {
                let current_md = mdmanager.get().await?;
                if md == current_md {
                    tracing::debug!(path = %latest_path.display(), "Backup path already exists and metadata matches; doing nothing");
                    Vec::new()
                } else {
                    tracing::debug!(path = %latest_path.display(), "Backup path already exists but metadata does not match; renaming current file and downloading correct version");
                    vec![
                        ObjectAction::Move {
                            src: latest_path.clone(),
                            dest: parentdir.join(current_md.old_filename(filename)),
                        },
                        ObjectAction::Download { path: latest_path },
                        ObjectAction::SaveMetadata,
                    ]
                }
            } else {
                let oldpath = parentdir.join(md.old_filename(filename));
                if oldpath.fs_err_try_exists()? {
                    tracing::debug!(path = %latest_path.display(), oldpath = %oldpath.display(), "Backup path does not exist but \"old\" path does; will rename");
                    vec![
                        ObjectAction::Move {
                            src: oldpath,
                            dest: latest_path,
                        },
                        ObjectAction::SaveMetadata,
                    ]
                } else {
                    tracing::debug!(path = %latest_path.display(), "Backup path does not exist; will download");
                    vec![
                        ObjectAction::Download { path: latest_path },
                        ObjectAction::SaveMetadata,
                    ]
                }
            }
        } else {
            tracing::debug!("Object is old version of key");
            let oldpath = parentdir.join(md.old_filename(filename));
            if oldpath.fs_err_try_exists()? {
                tracing::debug!(path = %oldpath.display(), "Backup path already exists; doing nothing");
                Vec::new()
            } else {
                let latest_path = parentdir.join(filename);
                if latest_path.fs_err_try_exists()? && md == mdmanager.get().await? {
                    tracing::debug!(path = %oldpath.display(), "Backup path does not exist, but \"latest\" file has matching metadata; renaming \"latest\" file");
                    vec![
                        ObjectAction::Move {
                            src: latest_path,
                            dest: oldpath,
                        },
                        ObjectAction::DeleteMetadata,
                    ]
                } else {
                    tracing::debug!(path = %oldpath.display(), "Backup path does not exist; will download");
                    vec![ObjectAction::Download { path: oldpath }]
                }
            }
        };

        for act in actions {
            match act {
                ObjectAction::Move { src, dest } => {
                    tracing::debug!(src = %src.display(), dest = %dest.display(), "Moving object file");
                    fs_err::rename(src, dest)?;
                }
                ObjectAction::Download { path } => {
                    self.download_item(&item, path, token.clone()).await?;
                }
                // TODO: Handle cancellation/cleanup around metadata
                // management:
                ObjectAction::SaveMetadata => mdmanager.set(md.clone()).await?,
                ObjectAction::DeleteMetadata => mdmanager.delete().await?,
            }
        }
        Ok(())

        // TODO: Manage object metadata and old versions
        // TODO: Handle concurrent downloads of the same key
    }

    async fn download_item(
        &self,
        item: &InventoryItem,
        path: PathBuf,
        token: CancellationToken,
    ) -> anyhow::Result<()> {
        // TODO: Download to a temp file and then move
        tracing::trace!("Opening output file");
        let outfile = File::create(&path)
            .with_context(|| format!("failed to open output file {}", path.display()))?;
        match token
            .run_until_cancelled(self.client.download_object(
                &item.url(),
                item.details.md5_digest(),
                &outfile,
            ))
            .await
        {
            Some(Ok(())) => outfile
                .set_modified(item.last_modified_date.into())
                .with_context(|| format!("failed to set mtime on {}", path.display())),
            Some(Err(e)) => {
                tracing::error!(error = ?e, "Failed to download object");
                if let Err(e2) = self.cleanup_download_path(&path) {
                    tracing::warn!(error = ?e2, "Failed to clean up download file");
                }
                Err(e.into())
            }
            None => {
                tracing::debug!("Download cancelled");
                self.cleanup_download_path(&path).map_err(Into::into)
            }
        }
    }

    fn cleanup_download_path(&self, dlfile: &Path) -> std::io::Result<()> {
        tracing::trace!(path = %dlfile.display(), "Cleaning up unfinished download file");
        fs_err::remove_file(dlfile)?;
        let p = dlfile.parent();
        while let Some(pp) = p {
            if pp == self.outdir {
                break;
            }
            if is_empty_dir(pp)? {
                fs_err::remove_dir(pp)?;
            }
        }
        Ok(())
    }

    async fn lock_path(&self, path: PathBuf) -> Guard<'_> {
        self.locks.async_lock(path).await
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ObjectAction {
    Move { src: PathBuf, dest: PathBuf },
    Download { path: PathBuf },
    SaveMetadata,
    DeleteMetadata,
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
        let content = fs_err::read_to_string(&self.database_path)?;
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
