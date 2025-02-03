use crate::consts::RESERVED_PREFIX;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StateFileManager {
    path: PathBuf,
}

impl StateFileManager {
    pub(super) fn new(outdir: &Path) -> Self {
        StateFileManager {
            path: outdir.join("{RESERVED_PREFIX}.state.json"),
        }
    }

    fn load(&self) -> anyhow::Result<State> {
        let content = match fs_err::read_to_string(&self.path) {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(State::default()),
            Err(e) => return Err(e.into()),
        };
        serde_json::from_str(&content)
            .with_context(|| format!("failed to deserialize contents of {}", self.path.display()))
    }

    fn store(&self, state: State) -> anyhow::Result<()> {
        let fp = tempfile::Builder::new()
            .prefix(&format!("{RESERVED_PREFIX}.state."))
            .tempfile_in(
                self.path
                    .parent()
                    .expect("state file path should have a parent"),
            )
            .with_context(|| {
                format!(
                    "failed to create temporary state file for updating {}",
                    self.path.display()
                )
            })?;
        serde_json::to_writer_pretty(fp.as_file(), &state)
            .with_context(|| format!("failed to serialize state to {}", self.path.display()))?;
        fp.persist(&self.path).with_context(|| {
            format!(
                "failed to persist temporary state file to {}",
                self.path.display()
            )
        })?;
        Ok(())
    }

    pub(super) fn register_start(&self) -> anyhow::Result<()> {
        let mut state = self.load()?;
        state.last_backup_started = Some(OffsetDateTime::now_utc());
        self.store(state)
    }

    pub(super) fn register_end(&self) -> anyhow::Result<()> {
        let mut state = self.load()?;
        state.last_successful_backup_finished = Some(OffsetDateTime::now_utc());
        self.store(state)
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct State {
    #[serde(with = "time::serde::rfc3339::option")]
    last_backup_started: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    last_successful_backup_finished: Option<OffsetDateTime>,
}
