In Development
--------------
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

v0.1.0-alpha (2024-11-26)
-------------------------
Alpha release
