[![Project Status: WIP – Initial development is in progress, but there has not yet been a stable, usable release suitable for the public.](https://www.repostatus.org/badges/latest/wip.svg)](https://www.repostatus.org/#wip)
[![CI Status](https://github.com/dandi/s3invsync/actions/workflows/test.yml/badge.svg)](https://github.com/dandi/s3invsync/actions/workflows/test.yml)
[![codecov.io](https://codecov.io/gh/dandi/s3invsync/branch/main/graph/badge.svg)](https://codecov.io/gh/dandi/s3invsync)
[![Minimum Supported Rust Version](https://img.shields.io/badge/MSRV-1.80-orange)](https://www.rust-lang.org)
[![MIT License](https://img.shields.io/github/license/dandi/s3invsync.svg)](https://opensource.org/licenses/MIT)

[GitHub](https://github.com/dandi/s3invsync) | [Issues](https://github.com/dandi/s3invsync/issues)

`s3invsync` is a [Rust](https://www.rust-lang.org) program for creating &
syncing backups of an AWS S3 bucket (including old versions of objects) by
making use of the bucket's [Amazon S3 Inventory][inv] files.

[inv]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html

**Warning:** This is an in-development program.  They may be bugs, and some
planned features have not been implemented yet.


Building & Running
==================

1. [Install Rust and Cargo](https://www.rust-lang.org/tools/install).

2. Clone this repository and `cd` into it.

3. Run `cargo build --release` to build the binary.  The intermediate build
   artifacts will be cached in `target/` in order to speed up subsequent
   builds.

4. Run with `cargo run --release -- <arguments ...>`.

5. If necessary, the actual binary can be found in `target/release/s3invsync`.
   It should run on any system with the same OS and architecture as it was
   built on.


Usage
=====

    cargo run --release -- [<options>] <inventory-base> <outdir>

`s3invsync` downloads the contents of an S3 bucket, including old versions of
objects, to the directory `<outdir>` using S3 Inventory files located at
`<inventory-base>`.

`<inventory-base>` must be of the form `s3://{bucket}/{prefix}/`, where
`{bucket}` is the destination bucket on which the inventory files are stored
and `{prefix}/` is the key prefix under which the [inventory manifest files][]
are located in the bucket (i.e., appending a string of the form
`YYYY-MM-DDTHH-MMZ/manifest.json` to `{prefix}/` should yield a key for a
manifest file).

[inventory manifest files]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory-location.html

When downloading a given key from S3, the latest version (if not deleted) is
stored at `{outdir}/{key}`, and the versionIds and etags of all latest object
versions in a given directory are stored in `.s3invsync.versions.json` in that
directory.  Each non-latest, non-deleted version of a given key is stored at
`{outdir}/{key}.old.{versionId}.{etag}`.

Options
-------

- `-d <DATE>`, `--date <DATE>` — Download objects from the inventory created at
  the given date.

  By default, the most recent inventory is downloaded.

  The date must be in the format `YYYY-MM-DD` (in which case the latest
  inventory for the given date is used) or in the format `YYYY-MM-DDTHH-MMZ`
  (to specify a specific inventory).

- `-I <INT>`, `--inventory-jobs <INT>` — Specify the maximum number of inventory
  list files to download & process at once [default: 20]

- `-l <level>`, `--log-level <level>` — Set the log level to the given value.
  Possible values are  "`ERROR`", "`WARN`", "`INFO`", "`DEBUG`", and "`TRACE`"
  (all case-insensitive).  [default value: `DEBUG`]

- `-O <INT>`, `--object-jobs <INT>` — Specify the maximum number of inventory
  entries to download & process at once [default: 20]

- `--path-filter <REGEX>` — Only download objects whose keys match the given
  [regular expression](https://docs.rs/regex/latest/regex/#syntax)
