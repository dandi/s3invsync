In Development
--------------
- Add `access-denied` and `invalid-object-state` items to `--ok-errors`

v0.1.0 (2025-02-04)
-------------------
- Add `--compress-filter-msgs` option
- Support all documented S3 Inventory fields in inventory lists
- Add `--list-dates` option
- The `--inventory-jobs` and `--object-jobs` options have been eliminated in
  favor of a new `--jobs` option
- Files & directories in the backup tree that are not listed in the inventory
  are deleted
- Increased MSRV to 1.82
- The temporary file used to download a manifest is now deleted immediately
  after parsing the manifest
- The default `--jobs` value now equals the number of available CPU cores or
  20, whichever is lower
- Add `--ok-errors` option
- Store start & successful end times of program runs in `.s3invsync.state.json`
- Support objects without version IDs
- Error out immediately if outdir is nonempty and does not contain an
  `.s3invsync.state.json` file
    - Added `--allow-new-nonempty` option to disable this check
- Add `--require-last-success` option

v0.1.0-alpha.2 (2025-01-06)
---------------------------
- After fully scanning an inventory list CSV file, delete it
- `--version` now includes the Git commit hash
- Log various process details when an error first occurs
- Fix a stall when cleaning up after an error
- Add `--trace-progress` option
- Drastically reduce memory usage
- Reject keys with directory components with special meaning, not just keys
  with filenames with special meaning
- If the download path for an item already exists and points to a directory,
  delete the directory
- If any of the ancestors for an item's download path points to a file, delete
  the file
- Ignore keys in inventory lists that end with a slash and are zero-sized
- Fix locking of paths currently being processed
- Increase number of retries on failed S3 requests to 10 attempts
- Support loading AWS credentials from standard locations
- Treat Ctrl-C like an error, triggering graceful shutdown

v0.1.0-alpha (2024-11-26)
-------------------------
Alpha release
