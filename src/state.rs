use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinlogPosition {
    pub file: String,
    pub pos: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    pub binlog: BinlogPosition,
}

impl State {
    #[must_use]
    pub fn new(binlog: BinlogPosition) -> Self {
        Self { binlog }
    }
}

pub fn load(path: &Path) -> Result<Option<State>> {
    if !path.exists() {
        return Ok(None);
    }
    let content =
        fs::read_to_string(path).with_context(|| format!("reading state {}", path.display()))?;
    let state = serde_json::from_str(&content)
        .with_context(|| format!("parsing JSON in {}", path.display()))?;
    Ok(Some(state))
}

pub fn save(path: &Path, state: &State) -> Result<()> {
    if let Some(parent) = parent_dir(path) {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating directory {}", parent.display()))?;
    }
    let parent = parent_dir(path).unwrap_or_else(|| Path::new("."));
    let content = serde_json::to_vec_pretty(state).context("serializing state")?;
    // Keep the temporary file on the same filesystem as the checkpoint.
    let mut temporary = NamedTempFile::new_in(parent)
        .with_context(|| format!("creating temporary state in {}", parent.display()))?;
    temporary
        .write_all(&content)
        .context("writing temporary state")?;
    temporary
        .as_file()
        .sync_all()
        .context("syncing temporary state to disk")?;
    persist_checkpoint(temporary, path)?;
    #[cfg(unix)]
    fs::File::open(parent)?
        .sync_all()
        .context("syncing the state directory to disk")?;
    Ok(())
}

fn persist_checkpoint(mut temporary: NamedTempFile, path: &Path) -> Result<()> {
    for attempt in 0..=10 {
        match temporary.persist(path) {
            Ok(_) => return Ok(()),
            // Windows can briefly refuse replacement while another process reads the target.
            Err(error)
                if cfg!(windows)
                    && error.error.kind() == std::io::ErrorKind::PermissionDenied
                    && attempt < 10 =>
            {
                temporary = error.file;
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(error) => {
                return Err(error.error)
                    .with_context(|| format!("atomically replacing state {}", path.display()));
            }
        }
    }
    unreachable!("the final persistence attempt always returns")
}

fn parent_dir(path: &Path) -> Option<&Path> {
    match path.parent() {
        Some(parent) if parent != Path::new("") => Some(parent),
        _ => None,
    }
}

pub fn absolutize(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let current = std::env::current_dir().context("reading the current directory")?;
    Ok(current.join(path))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    #[test]
    fn replacing_checkpoint_keeps_readers_on_complete_states() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("state.json");
        save(
            &path,
            &State::new(BinlogPosition {
                file: "mysql-bin.000001".into(),
                pos: 4,
            }),
        )?;
        std::thread::scope(|scope| -> Result<()> {
            let reader = scope.spawn(|| -> Result<()> {
                for _ in 0..200 {
                    let state =
                        load(&path)?.context("checkpoint disappeared during replacement")?;
                    assert_eq!(state.binlog.file, "mysql-bin.000001");
                    assert!((4..=24).contains(&state.binlog.pos));
                }
                Ok(())
            });
            for pos in 5..=24 {
                save(
                    &path,
                    &State::new(BinlogPosition {
                        file: "mysql-bin.000001".into(),
                        pos,
                    }),
                )?;
            }
            reader.join().expect("checkpoint reader panicked")?;
            Ok(())
        })?;
        assert_eq!(
            load(&path)?.context("final checkpoint missing")?.binlog.pos,
            24
        );
        assert_eq!(fs::read_dir(directory.path())?.count(), 1);
        Ok(())
    }

    #[test]
    fn failed_replacement_cleans_temporary_file() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("state.json");
        fs::create_dir(&path)?;
        let marker = path.join("keep");
        fs::write(&marker, b"original")?;
        let state = State::new(BinlogPosition {
            file: "mysql-bin.000001".into(),
            pos: 4,
        });
        assert!(save(&path, &state).is_err());
        assert_eq!(fs::read(&marker)?, b"original");
        assert_eq!(fs::read_dir(directory.path())?.count(), 1);
        Ok(())
    }

    fn unique_state_root(test_name: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        std::env::temp_dir().join(format!(
            "meili-mysql-sync-{test_name}-{}-{timestamp}",
            std::process::id()
        ))
    }

    #[test]
    fn load_returns_none_when_file_is_absent() -> Result<()> {
        let path = unique_state_root("absent").join("missing-state.json");

        assert!(load(&path)?.is_none());
        Ok(())
    }

    #[test]
    fn save_creates_parent_directories_and_load_round_trips() -> Result<()> {
        let root = unique_state_root("round-trip");
        let path = root.join("nested").join("state.json");
        let state = State::new(BinlogPosition {
            file: "mysql-bin.000123".to_owned(),
            pos: 456,
        });

        save(&path, &state)?;
        let loaded = load(&path)?.context("state file should exist after save")?;

        assert_eq!(loaded.binlog, state.binlog);

        fs::remove_dir_all(&root)
            .with_context(|| format!("removing test directory {}", root.display()))?;
        Ok(())
    }

    #[test]
    fn parent_dir_ignores_plain_file_names() {
        assert_eq!(parent_dir(Path::new("state.json")), None);
        assert_eq!(
            parent_dir(Path::new("state/state.json")),
            Some(Path::new("state"))
        );
    }

    #[test]
    fn absolutize_keeps_absolute_paths() -> Result<()> {
        let current = std::env::current_dir().context("reading the current directory")?;

        assert_eq!(absolutize(&current)?, current);
        Ok(())
    }
}
