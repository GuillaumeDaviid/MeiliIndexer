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
