use crate::asyncutil::LimitedShutdownGroup;
use crate::inventory::InventoryItem;
use crate::manifest::CsvManifest;
use crate::s3::S3Client;
use anyhow::Context;
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
}

impl Syncer {
    pub(crate) fn new(
        client: S3Client,
        outdir: PathBuf,
        inventory_jobs: NonZeroUsize,
        object_jobs: NonZeroUsize,
    ) -> Arc<Syncer> {
        Arc::new(Syncer {
            client: Arc::new(client),
            outdir,
            inventory_jobs,
            object_jobs,
        })
    }

    pub(crate) async fn run(self: &Arc<Self>, manifest: CsvManifest) -> Result<(), MultiError> {
        let mut inventory_dl_pool = LimitedShutdownGroup::new(self.inventory_jobs.get());
        let mut object_dl_pool = LimitedShutdownGroup::new(self.object_jobs.get());
        let (obj_sender, mut obj_receiver) = channel(self.inventory_jobs.get());

        for fspec in manifest.files {
            let clnt = self.client.clone();
            let sender = obj_sender.clone();
            inventory_dl_pool.spawn(move |_| async move {
                let itemlist = clnt.download_inventory_csv(fspec).await?;
                for item in itemlist {
                    let _ = sender.send(item?).await;
                }
                Ok(())
            });
        }

        let mut errors = Vec::new();
        let mut inventory_pool_closed = false;
        let mut object_pool_closed = false;
        let mut all_objects_txed = false;
        loop {
            tokio::select! {
                r = inventory_dl_pool.next(), if !inventory_pool_closed => {
                    match r {
                        Some(Ok(())) => (),
                        Some(Err(e)) => {
                            if !errors.is_empty() {
                                inventory_dl_pool.shutdown();
                                object_dl_pool.shutdown();
                            }
                            errors.push(e);
                        }
                        None => inventory_pool_closed = true,
                    }
                }
                r = object_dl_pool.next(), if !object_pool_closed => {
                    match r {
                        Some(Ok(())) => (),
                        Some(Err(e)) => {
                            if !errors.is_empty() {
                                inventory_dl_pool.shutdown();
                                object_dl_pool.shutdown();
                            }
                            errors.push(e);
                        }
                        None => object_pool_closed = true,
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

    async fn process_item(
        self: &Arc<Self>,
        item: InventoryItem,
        token: CancellationToken,
    ) -> anyhow::Result<()> {
        if token.is_cancelled() {
            return Ok(());
        }
        if item.is_deleted() || !item.is_latest {
            // TODO
            return Ok(());
        }
        let url = item.url();
        let outpath = self.outdir.join(&item.key);
        if let Some(p) = outpath.parent() {
            fs_err::create_dir_all(p)?;
        }
        // TODO: Download to a temp file and then move
        let outfile = File::create(&outpath)
            .with_context(|| format!("failed to open output file {}", outpath.display()))?;
        match token
            .run_until_cancelled(self.client.download_object(
                &url,
                item.details.md5_digest(),
                &outfile,
            ))
            .await
        {
            Some(Ok(())) => Ok(()),
            Some(Err(e)) => {
                // TODO: Warn on failure?
                let _ = self.cleanup_download_path(&outpath);
                Err(e.into())
            }
            None => {
                self.cleanup_download_path(&outpath)?;
                Ok(())
            }
        }
        // TODO: Manage object metadata and old versions
        // TODO: Handle concurrent downloads of the same key
    }

    fn cleanup_download_path(&self, dlfile: &Path) -> std::io::Result<()> {
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

fn is_empty_dir(p: &Path) -> std::io::Result<bool> {
    let mut iter = fs_err::read_dir(p)?;
    match iter.next() {
        None => Ok(true),
        Some(Ok(_)) => Ok(false),
        Some(Err(e)) => Err(e),
    }
}
