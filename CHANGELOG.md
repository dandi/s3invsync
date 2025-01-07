In Development
--------------
- Add `--compress-filter-msgs` option
- Support all documented S3 Inventory fields in inventory lists
- Add `--list-dates` option
- The `<outdir>` command-line argument is now optional and defaults to the
  current directory
- The `--inventory-jobs` and `--object-jobs` options have been eliminated in
  favor of a new `--jobs` option

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
