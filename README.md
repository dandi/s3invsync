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
