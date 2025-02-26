mod consts;
mod errorset;
mod inventory;
mod keypath;
mod manifest;
mod nursery;
mod s3;
mod statefile;
mod syncer;
mod timestamps;
mod util;
use crate::errorset::ErrorSet;
use crate::s3::{get_bucket_region, S3Client, S3Location};
use crate::statefile::StateFileManager;
use crate::syncer::Syncer;
use crate::timestamps::DateMaybeHM;
use crate::util::is_empty_dir;
use anyhow::Context;
use clap::Parser;
use fs_err::PathExt;
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
    /// If OUTDIR is nonempty and does not contain an `.s3invsync.state.json`
    /// file, run the backup anyway instead of erroring out.
    #[arg(long)]
    allow_new_nonempty: bool,

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

    /// Treat the given error types as non-fatal.
    ///
    /// If one of the specified types of errors occurs, a warning is emitted,
    /// and the error is otherwise ignored.
    ///
    /// This option takes a comma-separated list of one or more of the
    /// following error types:
    ///
    /// - access-denied — a 403 occurred while trying to download an object
    ///
    /// - invalid-entry — an entry in an inventory list file is invalid
    ///
    /// - invalid-object-state — S3 returned an `InvalidObjectState` error upon
    ///   attempting to download an object, usually because the object is
    ///   archived
    ///
    /// - missing-old-version — a 404 occurred while trying to download a
    ///   non-latest version of a key
    ///
    /// - all — same as listing all of the above error types
    ///
    /// By default, all of the above error types are fatal.
    #[arg(long, value_name = "LIST")]
    ignore_errors: Option<ErrorSet>,

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

    /// Deprecated since v0.2.0.  Use `--ignore-errors` instead.
    #[arg(
        long,
        value_name = "LIST",
        hide = true,
        conflicts_with = "ignore_errors"
    )]
    ok_errors: Option<ErrorSet>,

    /// Only download objects whose keys match the given regular expression
    #[arg(long, value_name = "REGEX")]
    path_filter: Option<regex::Regex>,

    /// Error out immediately if the most recent backup did not complete
    /// successfully
    #[arg(long)]
    require_last_success: bool,

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

    async fn get_client(&self) -> anyhow::Result<S3Client> {
        let bucket = self.inventory_base.bucket();
        tracing::info!(%bucket, "Determining region for S3 bucket ...");
        let region = get_bucket_region(self.inventory_base.bucket()).await?;
        tracing::info!(%bucket, %region, "Found S3 bucket region");
        S3Client::new(region, self.inventory_base.clone(), self.trace_progress)
            .await
            .map_err(Into::into)
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
    if args.list_dates {
        let client = args.get_client().await?;
        let mut stream = client.list_all_manifest_timestamps();
        while let Some(date) = stream.try_next().await? {
            println!("{date}");
        }
    } else {
        let Some(outdir) = args.outdir.clone() else {
            anyhow::bail!("missing required OUTDIR argument");
        };
        let ignore_errors = if let Some(ie) = args.ignore_errors {
            ie
        } else if let Some(ie) = args.ok_errors {
            tracing::warn!("--ok-errors is deprecated; use --ignore-errors instead");
            ie
        } else {
            ErrorSet::default()
        };
        let jobs = args.jobs()?;
        let start_time = std::time::Instant::now();
        tracing::trace!(path = %outdir.display(), "Creating root output directory");
        fs_err::create_dir_all(&outdir)?;
        let sfm = StateFileManager::new(&outdir);
        if !args.allow_new_nonempty && !is_empty_dir(&outdir)? && !sfm.path().fs_err_try_exists()? {
            anyhow::bail!("Backup directory is nonempty and does not contain a .s3invsync.state.json file; pass --allow-new-nonempty to run anyway");
        }
        sfm.start(args.require_last_success)?;
        let client = args.get_client().await?;
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
            ignore_errors,
        );
        tracing::info!("Starting backup ...");
        syncer.run(manifest).await?;
        sfm.end()?;
        tracing::info!("Backup complete");
    }
    Ok(())
}
