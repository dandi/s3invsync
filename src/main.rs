#![allow(dead_code)] // XXX
#![allow(unused_imports)] // XXX
#![allow(clippy::todo)] // XXX
mod inventory;
mod manifest;
mod s3;
mod timestamps;
use crate::s3::{get_bucket_region, S3Client, S3Location};
use crate::timestamps::DateMaybeHM;
use clap::Parser;
use std::fs::File;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, Parser, PartialEq)]
#[command(version)]
struct Arguments {
    #[arg(short, long)]
    date: Option<DateMaybeHM>,

    inventory_base: S3Location,

    outdir: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Arguments::parse();
    let region = get_bucket_region(args.inventory_base.bucket()).await?;
    let client = S3Client::new(region, args.inventory_base).await?;
    let manifest = client.get_manifest_for_date(args.date).await?;
    for fspec in &manifest.files {
        // TODO: Add to pool of concurrent download tasks?
        let itemlist = client.download_inventory_csv(fspec).await?;
        // TODO: Do this concurrently:
        for item in itemlist {
            let item = item?;
            let url =
                S3Location::new(item.bucket, item.key.clone()).with_version_id(item.version_id);
            let outpath = if item.is_latest {
                args.outdir.join(&item.key)
            } else {
                todo!()
            };
            if let Some(p) = outpath.parent() {
                // TODO: Attach error context:
                std::fs::create_dir_all(p)?;
            }
            // TODO: Attach error context:
            let outfile = File::create(outpath)?;
            // TODO: Download in task pool:
            client
                .download_object(&url, item.details.md5_digest(), &outfile)
                .await?;
            // - Manage object metadata and old versions
            // - Handle concurrent downloads of the same key
            // - Delete outfile (and empty parent directories) if download is
            //   interrupted
            todo!()
        }
    }
    // TODO: Handle error recovery and Ctrl-C
    Ok(())
}
