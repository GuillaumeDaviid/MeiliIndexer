use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

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
    let content = fs::read_to_string(path)
        .with_context(|| format!("lecture de l'etat {}", path.display()))?;
    let state = serde_json::from_str(&content)
        .with_context(|| format!("parsing JSON de {}", path.display()))?;
    Ok(Some(state))
}

pub fn save(path: &Path, state: &State) -> Result<()> {
    if let Some(parent) = parent_dir(path) {
        fs::create_dir_all(parent)
            .with_context(|| format!("creation du dossier {}", parent.display()))?;
    }
    let content = serde_json::to_string_pretty(state).context("serialisation de l'etat")?;
    fs::write(path, content).with_context(|| format!("ecriture de l'etat {}", path.display()))
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
    let current = std::env::current_dir().context("lecture du dossier courant")?;
    Ok(current.join(path))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

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
            .with_context(|| format!("suppression du dossier de test {}", root.display()))?;
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
        let current = std::env::current_dir().context("lecture du dossier courant")?;

        assert_eq!(absolutize(&current)?, current);
        Ok(())
    }
}
