#![allow(dead_code)] // XXX
#![allow(unused_imports)] // XXX
mod manifest;
mod s3;
mod timestamps;
use crate::s3::{get_bucket_region, S3Client, S3Location};
use crate::timestamps::DateMaybeHM;
use clap::Parser;
use std::path::PathBuf;

#[derive(Clone, Debug, Eq, Parser, PartialEq)]
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
        client.download_inventory_csv(fspec).await?;
        // TODO: For each entry in CSV:
        // - Download object (in a task pool)
        // - Manage object metadata and old versions
        // - Handle concurrent downloads of the same key
        todo!()
    }
    // TODO: Handle error recovery and Ctrl-C
    Ok(())
}
