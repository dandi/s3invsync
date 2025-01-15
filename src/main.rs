mod consts;
mod inventory;
mod keypath;
mod manifest;
mod nursery;
mod s3;
mod syncer;
mod timestamps;
mod util;
use crate::s3::{get_bucket_region, S3Client, S3Location};
use crate::syncer::Syncer;
use crate::timestamps::DateMaybeHM;
use anyhow::Context;
use clap::Parser;
use futures_util::TryStreamExt;
use std::io::{stderr, IsTerminal};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use tracing::Level;
use tracing_subscriber::{filter::Targets, fmt::time::OffsetTime, prelude::*};

/// Back up an AWS S3 bucket using S3 Inventory files
///
/// See <https://github.com/dandi/s3invsync> for more information.
#[derive(Clone, Debug, Parser)]
#[command(version = env!("VERSION_WITH_GIT"))]
struct Arguments {
    /// Instead of emitting a log message for each object skipped by
    /// `--path-filter`, emit one message for every `N` objects skipped.
    #[arg(long, value_name = "N")]
    compress_filter_msgs: Option<NonZeroUsize>,

    /// Download objects from the inventory created at the given date.
    ///
    /// By default, the most recent inventory is downloaded.
    ///
    /// The date must be in the format `YYYY-MM-DD` (in which case the latest
    /// inventory for the given date is used) or in the format
    /// `YYYY-MM-DDTHH-MMZ` (to specify a specific inventory).
    #[arg(short, long)]
    date: Option<DateMaybeHM>,

    /// Set the maximum number of concurrent download jobs.  Defaults to the
    /// number of available CPU cores, or 20, whichever is lower.
    #[arg(short = 'J', long)]
    jobs: Option<NonZeroUsize>,

    /// List available inventory manifest dates instead of backing anything up
    #[arg(long)]
    list_dates: bool,

    /// Set logging level
    #[arg(
        short,
        long,
        default_value = "DEBUG",
        value_name = "ERROR|WARN|INFO|DEBUG|TRACE"
    )]
    log_level: Level,

    /// Only download objects whose keys match the given regular expression
    #[arg(long, value_name = "REGEX")]
    path_filter: Option<regex::Regex>,

    /// Emit download progress information at TRACE level
    #[arg(long)]
    trace_progress: bool,

    /// The location of the manifest files for the S3 inventory to back up
    ///
    /// `<inventory-base>` must be of the form `s3://{bucket}/{prefix}/`, where
    /// `{bucket}` is the destination bucket on which the inventory files are
    /// stored and `{prefix}/` is the key prefix under which the inventory
    /// manifest files are located in the bucket (i.e., appending a string of
    /// the form `YYYY-MM-DDTHH-MMZ/manifest.json` to `{prefix}/` should yield
    /// a key for a manifest file).
    inventory_base: S3Location,

    /// Directory in which to download the S3 objects.  Defaults to the current
    /// working directory.
    outdir: Option<PathBuf>,
}

impl Arguments {
    fn jobs(&self) -> anyhow::Result<NonZeroUsize> {
        if let Some(j) = self.jobs {
            Ok(j)
        } else {
            let cores = std::thread::available_parallelism()
                .context("failed to determine number of available CPU cores")?;
            Ok(cores.min(NonZeroUsize::new(20).expect("20 != 0")))
        }
    }
}

// See
// <https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/time/struct.OffsetTime.html#method.local_rfc_3339>
// for an explanation of the main + #[tokio::main]run thing
fn main() -> anyhow::Result<()> {
    let args = Arguments::parse();
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
                .with_target(env!("CARGO_CRATE_NAME"), args.log_level)
                .with_target("aws_config", Level::DEBUG.min(args.log_level))
                .with_default(Level::INFO.min(args.log_level)),
        )
        .init();
    run(args)
}

#[tokio::main]
async fn run(args: Arguments) -> anyhow::Result<()> {
    let jobs = args.jobs()?;
    let start_time = std::time::Instant::now();
    let bucket = args.inventory_base.bucket();
    tracing::info!(%bucket, "Determining region for S3 bucket ...");
    let region = get_bucket_region(args.inventory_base.bucket()).await?;
    tracing::info!(%bucket, %region, "Found S3 bucket region");
    let client = S3Client::new(region, args.inventory_base, args.trace_progress).await?;
    if args.list_dates {
        let mut stream = client.list_all_manifest_timestamps();
        while let Some(date) = stream.try_next().await? {
            println!("{date}");
        }
    } else {
        let outdir = match args.outdir {
            Some(p) => p,
            None => std::env::current_dir().context("failed to determine current directory")?,
        };
        tracing::info!("Fetching manifest ...");
        let (manifest, manifest_date) = client.get_manifest_for_date(args.date).await?;
        let syncer = Syncer::new(
            client,
            outdir,
            manifest_date,
            start_time,
            jobs,
            args.path_filter,
            args.compress_filter_msgs,
        );
        tracing::info!("Starting backup ...");
        syncer.run(manifest).await?;
        tracing::info!("Backup complete");
    }
    Ok(())
}
