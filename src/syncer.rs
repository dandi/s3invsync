use crate::asyncutil::LimitedShutdownGroup;
use crate::inventory::{InventoryItem, ItemDetails};
use crate::manifest::CsvManifest;
use crate::s3::S3Client;
use anyhow::Context;
use fs_err::PathExt;
use futures_util::StreamExt;
use std::fmt;
use std::fs::File;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub(crate) struct Syncer {
    client: Arc<S3Client>,
    outdir: PathBuf,
    inventory_jobs: NonZeroUsize,
    object_jobs: NonZeroUsize,
    path_filter: Option<regex::Regex>,
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

        let (dirname, filename) = match item.key.rsplit_once('/') {
            Some((pre, post)) => (Some(pre), post),
            None => (None, &*item.key),
        };
        // TODO: If `filename` has special meaning to s3invsync, error out
        let parentdir = if let Some(p) = dirname {
            self.outdir.join(p)
        } else {
            self.outdir.clone()
        };
        tracing::trace!(path = %parentdir.display(), "Creating output directory");
        fs_err::create_dir_all(&parentdir)?;

        let actions = if item.is_latest {
            tracing::debug!("Object is latest version of key");
            let path = parentdir.join(filename);
            if path.fs_err_try_exists()? {
                let current_md = self.get_metadata(&parentdir, filename).await?;
                if md == current_md {
                    tracing::debug!(path = %path.display(), "Backup path already exists and metadata matches; doing nothing");
                    Vec::new()
                } else {
                    tracing::debug!(path = %path.display(), "Backup path already exists but metadata does not match; renaming current file and downloading correct version");
                    vec![
                        ObjectAction::Move {
                            src: path.clone(),
                            dest: parentdir.join(current_md.old_filename(filename)),
                        },
                        ObjectAction::Download { path },
                        ObjectAction::SaveMetadata,
                    ]
                }
            } else {
                let oldpath = parentdir.join(md.old_filename(filename));
                if oldpath.fs_err_try_exists()? {
                    tracing::debug!(path = %path.display(), oldpath = %oldpath.display(), "Backup path does not exist but \"old\" path does; will rename");
                    vec![
                        ObjectAction::Move {
                            src: oldpath,
                            dest: path,
                        },
                        ObjectAction::SaveMetadata,
                    ]
                } else {
                    tracing::debug!(path = %path.display(), "Backup path does not exist; will download");
                    vec![ObjectAction::Download { path }, ObjectAction::SaveMetadata]
                }
            }
        } else {
            tracing::debug!("Object is old version of key");
            let path = parentdir.join(md.old_filename(filename));
            if path.fs_err_try_exists()? {
                tracing::debug!(path = %path.display(), "Backup path already exists; doing nothing");
                Vec::new()
            } else {
                let newpath = parentdir.join(filename);
                if newpath.fs_err_try_exists()?
                    && md == self.get_metadata(&parentdir, filename).await?
                {
                    tracing::debug!(path = %path.display(), "Backup path does not exist, but \"latest\" file has matching metadata; renaming \"latest\" file");
                    vec![
                        ObjectAction::Move {
                            src: newpath,
                            dest: path,
                        },
                        ObjectAction::DeleteMetadata,
                    ]
                } else {
                    tracing::debug!(path = %path.display(), "Backup path does not exist; will download");
                    vec![ObjectAction::Download { path }]
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
                    // TODO: Download to a temp file and then move
                    tracing::trace!("Opening output file");
                    let outfile = File::create(&path).with_context(|| {
                        format!("failed to open output file {}", path.display())
                    })?;
                    match token
                        .run_until_cancelled(self.client.download_object(
                            &item.url(),
                            item.details.md5_digest(),
                            &outfile,
                        ))
                        .await
                    {
                        Some(Ok(())) => {
                            outfile
                                .set_modified(item.last_modified_date.into())
                                .with_context(|| {
                                    format!("failed to set mtime on {}", path.display())
                                })?;
                        }
                        Some(Err(e)) => {
                            tracing::error!(error = ?e, "Failed to download object");
                            if let Err(e2) = self.cleanup_download_path(&path) {
                                tracing::warn!(error = ?e2, "Failed to clean up download file");
                            }
                            return Err(e.into());
                        }
                        None => {
                            tracing::debug!("Download cancelled");
                            self.cleanup_download_path(&path)?;
                        }
                    }
                }
                // TODO: Handle cancellation/cleanup around metadata
                // management:
                ObjectAction::SaveMetadata => {
                    self.save_metadata(&parentdir, filename, md.clone()).await?;
                }
                ObjectAction::DeleteMetadata => self.delete_metadata(&parentdir, filename).await?,
            }
        }
        Ok(())

        // TODO: Manage object metadata and old versions
        // TODO: Handle concurrent downloads of the same key
    }

    async fn get_metadata(&self, parentdir: &Path, filename: &str) -> anyhow::Result<Metadata> {
        todo!()
    }

    async fn save_metadata(
        &self,
        parentdir: &Path,
        filename: &str,
        md: Metadata,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_metadata(&self, parentdir: &Path, filename: &str) -> anyhow::Result<()> {
        todo!()
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
}

#[derive(Debug)]
pub(crate) struct MultiError(Vec<anyhow::Error>);

impl fmt::Display for MultiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.len() > 1 {
            writeln!(f, "{} ERRORS:\n---", self.0.len())?;
        }
        let mut first = true;
        for e in &self.0 {
            if !std::mem::replace(&mut first, false) {
                writeln!(f, "\n---")?;
            }
            write!(f, "{e:?}")?;
        }
        Ok(())
    }
}

impl std::error::Error for MultiError {}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ObjectAction {
    Move { src: PathBuf, dest: PathBuf },
    Download { path: PathBuf },
    SaveMetadata,
    DeleteMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Metadata {
    version_id: String,
    etag: String,
}

impl Metadata {
    fn old_filename(&self, basename: &str) -> String {
        format!("{}.old.{}.{}", basename, self.version_id, self.etag)
    }
}

fn is_empty_dir(p: &Path) -> std::io::Result<bool> {
    let mut iter = fs_err::read_dir(p)?;
    match iter.next() {
        None => Ok(true),
        Some(Ok(_)) => Ok(false),
        Some(Err(e)) => Err(e),
    }
}
