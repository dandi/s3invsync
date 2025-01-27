[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)
[![CI Status](https://github.com/dandi/s3invsync/actions/workflows/test.yml/badge.svg)](https://github.com/dandi/s3invsync/actions/workflows/test.yml)
[![codecov.io](https://codecov.io/gh/dandi/s3invsync/branch/main/graph/badge.svg)](https://codecov.io/gh/dandi/s3invsync)
[![Minimum Supported Rust Version](https://img.shields.io/badge/MSRV-1.81-orange)](https://www.rust-lang.org)
[![MIT License](https://img.shields.io/github/license/dandi/s3invsync.svg)](https://opensource.org/licenses/MIT)

[GitHub](https://github.com/dandi/s3invsync) | [Issues](https://github.com/dandi/s3invsync/issues) | [Changelog](https://github.com/dandi/s3invsync/blob/main/CHANGELOG.md)

`s3invsync` is a [Rust](https://www.rust-lang.org) program for creating &
syncing backups of an AWS S3 bucket (including old versions of objects) by
making use of the bucket's [Amazon S3 Inventory][inv] files.

[inv]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html

Currently, only S3 Inventories with CSV output files are supported, and the
CSVs are required to list at least the `Bucket`, `Key`, and `ETag` fields.

**Warning:** This is an in-development program.  They may be bugs, and some
planned features have not been implemented yet.


Installation
============

Installing the Latest Release
-----------------------------

`s3invsync` provides pre-built binaries for the most common platforms as GitHub
release assets.  Simply download the asset for your platform from the latest
release on [the releases page](https://github.com/dandi/s3invsync/releases),
unzip it, and place the `s3invsync` or `s3invsync.exe` file inside somewhere on
your `$PATH`.

Alternatively, if you have
[`cargo-binstall`](https://github.com/cargo-bins/cargo-binstall), you can
install or update to the latest release asset with a single command:

    cargo binstall --git https://github.com/dandi/s3invsync s3invsync

Installing the Latest Development Code
--------------------------------------

In order to build and/or install `s3invsync` from source, you first need to
[install Rust and Cargo](https://www.rust-lang.org/tools/install).  You can
then download & build the program source and install it to `~/.cargo/bin` by
running:

    cargo install --git https://github.com/dandi/s3invsync

See the [`cargo
install`](https://doc.rust-lang.org/cargo/commands/cargo-install.html)
documentation for further options.

Alternatively, you can clone this repository manually and then build a binary
localized to the clone by running `cargo build` (or `cargo build --release` to
enable optimizations) inside it.  The resulting binary can then be run with
`cargo run -- <arguments>` (or `cargo run --release -- <arguments>` to use
optimizations).  The binary file itself is located at either
`target/debug/s3invsync` or `target/release/s3invsync`, depending on whether
`--release` was supplied.  See the [`cargo
build`](https://doc.rust-lang.org/cargo/commands/cargo-build.html) and [`cargo
run`](https://doc.rust-lang.org/cargo/commands/cargo-run.html) documentation
for further options.


Usage
=====

    s3invsync [<options>] <inventory-base> <outdir>

`s3invsync` downloads the contents of an S3 bucket, including old versions of
objects if the bucket is versioned, to the directory `<outdir>` using S3
Inventory files located at `<inventory-base>`.

`<inventory-base>` must be of the form `s3://{bucket}/{prefix}/`, where
`{bucket}` is the destination bucket on which the inventory files are stored
and `{prefix}/` is the key prefix under which the [inventory manifest files][]
are located in the bucket (i.e., appending a string of the form
`YYYY-MM-DDTHH-MMZ/manifest.json` to `{prefix}/` should yield a key for a
manifest file).

[inventory manifest files]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory-location.html

`s3invsync` honors AWS credentials stored in the standard locations (e.g., the
environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and
`AWS_REGION` or the default credentials files `~/.aws/config` and
`~/.aws/credentials`).  For public buckets, no credentials need to be provided.

When downloading a given key from S3, the latest version (if not deleted) is
stored at `{outdir}/{key}`, and the versionIds and etags of all latest object
versions in a given directory are stored in `.s3invsync.versions.json` in that
directory.  Each non-latest, non-deleted version of a given key is stored at
`{outdir}/{key}.old.{versionId}.{etag}`.

`s3invsync` stores the timestamps of the start of the most recent backup and
the end of the most recent successful backup in an `.s3invsync.state.json` file
at the root of `<outdir>`.

Any files or directories under `<outdir>` that do not correspond to an object
listed in the inventory and are not `.s3invsync.*` files are deleted.

Options
-------

- `--compress-filter-msgs <N>` — Instead of emitting a log message for each
  object skipped by `--path-filter`, emit one message for every `<N>` objects
  skipped.

- `-d <DATE>`, `--date <DATE>` — Download objects from the inventory created at
  the given date.

  By default, the most recent inventory is downloaded.

  The date must be in the format `YYYY-MM-DD` (in which case the latest
  inventory for the given date is used) or in the format `YYYY-MM-DDTHH-MMZ`
  (to specify a specific inventory).

- `-J <INT>`, `--jobs <INT>` — Specify the maximum number of concurrent
  download jobs.  Defaults to the number of available CPU cores, or 20,
  whichever is lower.

- `--list-dates` — List available inventory manifest dates instead of
  backing anything up.  When this option is given, the `<outdir>` argument is
  optional and does nothing.

- `-l <level>`, `--log-level <level>` — Set the log level to the given value.
  Possible values are  "`ERROR`", "`WARN`", "`INFO`", "`DEBUG`", and "`TRACE`"
  (all case-insensitive).  [default value: `DEBUG`]

- `--ok-errors <list>` — Treat the given error types as non-fatal.  If one of
  the specified types of errors occurs, a warning is emitted, and the error is
  otherwise ignored.

  This option takes a comma-separated list of one or more of the following
  error types:

  - `invalid-entry` — an entry in an inventory list file is invalid

  - `missing-old-version` — a 404 occurred while trying to download a
    non-latest version of a key

  - `all` — same as listing all of the above error types

  By default, all of the above error types are fatal.

- `--path-filter <REGEX>` — Only download objects whose keys match the given
  [regular expression](https://docs.rs/regex/latest/regex/#syntax)

- `--trace-progress` — Emit per-object download progress at the TRACE level.
  (Note that you still need to specify `--log-level TRACE` separately in order
  for the download progress logs to be visible.)  This is off by default because
  it can make for some very noisy logs.
