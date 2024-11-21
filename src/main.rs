mod asyncutil;
mod consts;
mod inventory;
mod manifest;
mod s3;
mod syncer;
mod timestamps;
mod util;
use crate::s3::{get_bucket_region, S3Client, S3Location};
use crate::syncer::Syncer;
use crate::timestamps::DateMaybeHM;
use anyhow::Context;
use clap::Parser;
use std::io::{stderr, IsTerminal};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use tracing::Level;
use tracing_subscriber::{filter::Targets, fmt::time::OffsetTime, prelude::*};

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

// See
// <https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/time/struct.OffsetTime.html#method.local_rfc_3339>
// for an explanation of the main + #[tokio::main]run thing
fn main() -> anyhow::Result<()> {
    let timer =
        OffsetTime::local_rfc_3339().context("failed to determine local timezone offset")?;
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_timer(timer)
                .with_ansi(stderr().is_terminal())
                .with_writer(stderr),
        )
        .with(
            Targets::new()
                .with_target(env!("CARGO_CRATE_NAME"), Level::TRACE)
                .with_target("aws_config", Level::DEBUG)
                .with_target("reqwest", Level::TRACE)
                .with_default(Level::INFO),
        )
        .init();
    run()
}

#[tokio::main]
async fn run() -> anyhow::Result<()> {
    let args = Arguments::parse();
    let bucket = args.inventory_base.bucket();
    tracing::info!(%bucket, "Determining region for S3 bucket ...");
    let region = get_bucket_region(args.inventory_base.bucket()).await?;
    tracing::info!(%bucket, %region, "Found S3 bucket region");
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
