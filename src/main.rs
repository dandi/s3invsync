#![allow(dead_code)] // XXX
#![allow(unused_imports)] // XXX
#![allow(clippy::todo)] // XXX
mod asyncutil;
mod inventory;
mod manifest;
mod s3;
mod timestamps;
use crate::asyncutil::LimitedShutdownGroup;
use crate::inventory::InventoryItem;
use crate::s3::{get_bucket_region, S3Client, S3Location};
use crate::timestamps::DateMaybeHM;
use clap::Parser;
use futures_util::{stream::select, StreamExt};
use std::fs::File;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Eq, Parser, PartialEq)]
#[command(version)]
struct Arguments {
    #[arg(short, long)]
    date: Option<DateMaybeHM>,

    #[arg(short = 'I', long, default_value = "20")]
    inventory_jobs: NonZeroUsize,

    #[arg(short = 'O', long, default_value = "20")]
    object_jobs: NonZeroUsize,

    inventory_base: S3Location,

    outdir: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Arguments::parse();
    let region = get_bucket_region(args.inventory_base.bucket()).await?;
    let client = Arc::new(S3Client::new(region, args.inventory_base).await?);
    let manifest = client.get_manifest_for_date(args.date).await?;
    let mut inventory_dl_pool = LimitedShutdownGroup::new(args.inventory_jobs.get());
    let mut object_dl_pool = LimitedShutdownGroup::new(args.object_jobs.get());
    let (obj_sender, mut obj_receiver) = channel(args.inventory_jobs.get());

    for fspec in manifest.files {
        let clnt = client.clone();
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
                    let clnt = client.clone();
                    let outdir = args.outdir.clone();
                    object_dl_pool
                        .spawn(move |token| async move { download_object(clnt, token, item, outdir).await });
                } else {
                    all_objects_txed = true;
                }
            }
            else => break,
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        todo!("Return multi-error")
    }
}

async fn download_object(
    client: Arc<S3Client>,
    token: CancellationToken,
    item: InventoryItem,
    outdir: PathBuf,
) -> anyhow::Result<()> {
    if token.is_cancelled() {
        return Ok(());
    }
    let url = S3Location::new(item.bucket, item.key.clone()).with_version_id(item.version_id);
    let outpath = if item.is_latest {
        outdir.join(&item.key)
    } else {
        todo!()
    };
    if let Some(p) = outpath.parent() {
        // TODO: Attach error context:
        std::fs::create_dir_all(p)?;
    }
    // TODO: Attach error context:
    let outfile = File::create(outpath)?;
    match token
        .run_until_cancelled(client.download_object(&url, item.details.md5_digest(), &outfile))
        .await
    {
        Some(Ok(())) => Ok(()),
        Some(Err(e)) => Err(e.into()),
        None => todo!("Delete outfile and empty parent directories"),
    }
    // TODO: Manage object metadata and old versions
    // TODO: Handle concurrent downloads of the same key
}
