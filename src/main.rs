mod asyncutil;
mod inventory;
mod manifest;
mod s3;
mod syncer;
mod timestamps;
use crate::s3::{get_bucket_region, S3Client, S3Location};
use crate::syncer::Syncer;
use crate::timestamps::DateMaybeHM;
use clap::Parser;
use std::num::NonZeroUsize;
use std::path::PathBuf;

#[derive(Clone, Debug, Parser)]
#[command(version)]
struct Arguments {
    #[arg(short, long)]
    date: Option<DateMaybeHM>,

    #[arg(short = 'I', long, default_value = "20")]
    inventory_jobs: NonZeroUsize,

    #[arg(short = 'O', long, default_value = "20")]
    object_jobs: NonZeroUsize,

    #[arg(long)]
    path_filter: Option<regex::Regex>,

    inventory_base: S3Location,

    outdir: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Arguments::parse();
    let region = get_bucket_region(args.inventory_base.bucket()).await?;
    let client = S3Client::new(region, args.inventory_base).await?;
    let manifest = client.get_manifest_for_date(args.date).await?;
    let syncer = Syncer::new(
        client,
        args.outdir,
        args.inventory_jobs,
        args.object_jobs,
        args.path_filter,
    );
    syncer.run(manifest).await?;
    Ok(())
}
