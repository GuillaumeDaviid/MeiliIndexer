use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub mysql: MysqlConfig,
    pub meilisearch: MeilisearchConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    pub tables: Vec<TableConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MysqlConfig {
    pub url: String,
    pub server_id: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MeilisearchConfig {
    pub host: String,
    pub api_key: Option<String>,
    pub batch_size: usize,
    pub max_in_flight_tasks: usize,
    pub task_poll_ms: u64,
    pub task_timeout_secs: u64,
}

impl Default for MeilisearchConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            api_key: None,
            batch_size: 1_000,
            max_in_flight_tasks: 8,
            task_poll_ms: 100,
            task_timeout_secs: 300,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    pub state_path: PathBuf,
    pub snapshot_on_start: bool,
    pub flush_interval_ms: u64,
    pub checkpoint_every_events: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            state_path: PathBuf::from("meili-sync-state.json"),
            snapshot_on_start: true,
            flush_interval_ms: 1_000,
            checkpoint_every_events: 1_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TableConfig {
    pub database: Option<String>,
    pub table: String,
    pub index: String,
    pub primary_key: String,
    pub fields: Vec<String>,
    pub watch_fields: Vec<String>,
    pub field_aliases: BTreeMap<String, String>,
    pub displayed_attributes: Option<Vec<String>>,
    pub searchable_attributes: Option<Vec<String>>,
    pub filterable_attributes: Option<Vec<String>>,
    pub sortable_attributes: Option<Vec<String>>,
    pub ranking_rules: Option<Vec<String>>,
    pub distinct_attribute: Option<String>,
    pub where_clause: Option<String>,
    pub snapshot_batch_size: usize,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            database: None,
            table: String::new(),
            index: String::new(),
            primary_key: String::new(),
            fields: Vec::new(),
            watch_fields: Vec::new(),
            field_aliases: BTreeMap::new(),
            displayed_attributes: None,
            searchable_attributes: None,
            filterable_attributes: None,
            sortable_attributes: None,
            ranking_rules: None,
            distinct_attribute: None,
            where_clause: None,
            snapshot_batch_size: 10_000,
        }
    }
}

impl Config {
    pub fn from_path(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("lecture de la configuration {}", path.display()))?;
        let config: Self = toml::from_str(&content)
            .with_context(|| format!("parsing TOML de {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.mysql.url.trim().is_empty() {
            bail!("mysql.url est obligatoire");
        }
        if self.mysql.server_id == 0 {
            bail!("mysql.server_id doit etre unique et superieur a 0");
        }
        if self.meilisearch.host.trim().is_empty() {
            bail!("meilisearch.host est obligatoire");
        }
        if self.meilisearch.batch_size == 0 {
            bail!("meilisearch.batch_size doit etre superieur a 0");
        }
        if self.meilisearch.max_in_flight_tasks == 0 {
            bail!("meilisearch.max_in_flight_tasks doit etre superieur a 0");
        }
        if self.tables.is_empty() {
            bail!("au moins une table doit etre declaree");
        }

        let mut seen_tables = BTreeSet::new();
        for table in &self.tables {
            table.validate()?;
            let key = (
                table.database.clone().unwrap_or_default(),
                table.table.clone(),
                table.index.clone(),
            );
            if !seen_tables.insert(key) {
                bail!(
                    "table dupliquee dans la configuration: {} -> {}",
                    table.table,
                    table.index
                );
            }
        }
        Ok(())
    }
}

impl TableConfig {
    fn validate(&self) -> Result<()> {
        if self.table.trim().is_empty() {
            bail!("tables[].table est obligatoire");
        }
        if self.index.trim().is_empty() {
            bail!("tables[].index est obligatoire pour {}", self.table);
        }
        if self.primary_key.trim().is_empty() {
            bail!("tables[].primary_key est obligatoire pour {}", self.table);
        }
        if self.fields.is_empty() {
            bail!("tables[].fields ne peut pas etre vide pour {}", self.table);
        }
        if !self.fields.iter().any(|field| field == &self.primary_key) {
            bail!(
                "la cle primaire '{}' doit etre presente dans fields pour {}",
                self.primary_key,
                self.table
            );
        }
        if self.snapshot_batch_size == 0 {
            bail!(
                "snapshot_batch_size doit etre superieur a 0 pour {}",
                self.table
            );
        }
        let mut seen_fields = BTreeSet::new();
        for field in &self.fields {
            if field.trim().is_empty() {
                bail!("nom de champ vide dans {}", self.table);
            }
            if !seen_fields.insert(field) {
                bail!("champ duplique dans {}: {}", self.table, field);
            }
        }
        for field in &self.watch_fields {
            if field.trim().is_empty() {
                bail!("nom de watch_field vide dans {}", self.table);
            }
        }
        self.validate_attribute_list("displayed_attributes", self.displayed_attributes.as_deref())?;
        self.validate_attribute_list(
            "searchable_attributes",
            self.searchable_attributes.as_deref(),
        )?;
        self.validate_attribute_list(
            "filterable_attributes",
            self.filterable_attributes.as_deref(),
        )?;
        self.validate_attribute_list("sortable_attributes", self.sortable_attributes.as_deref())?;
        self.validate_string_list("ranking_rules", self.ranking_rules.as_deref())?;
        if let Some(attribute) = &self.distinct_attribute {
            self.validate_attribute("distinct_attribute", attribute)?;
        }
        Ok(())
    }

    pub fn target_field<'a>(&'a self, source: &'a str) -> &'a str {
        self.field_aliases
            .get(source)
            .map_or(source, String::as_str)
    }

    #[must_use]
    pub fn document_fields(&self) -> Vec<String> {
        self.fields
            .iter()
            .map(|field| self.target_field(field).to_owned())
            .collect()
    }

    fn validate_attribute_list(&self, label: &str, attributes: Option<&[String]>) -> Result<()> {
        let Some(attributes) = attributes else {
            return Ok(());
        };
        self.validate_string_list(label, Some(attributes))?;
        let mut seen_attributes = BTreeSet::new();
        for attribute in attributes {
            if !seen_attributes.insert(attribute) {
                bail!(
                    "attribut duplique dans {} pour {}: {}",
                    label,
                    self.table,
                    attribute
                );
            }
            self.validate_attribute(label, attribute)?;
        }
        Ok(())
    }

    fn validate_string_list(&self, label: &str, values: Option<&[String]>) -> Result<()> {
        let Some(values) = values else {
            return Ok(());
        };
        for value in values {
            if value.trim().is_empty() {
                bail!("valeur vide dans {} pour {}", label, self.table);
            }
        }
        Ok(())
    }

    fn validate_attribute(&self, label: &str, attribute: &str) -> Result<()> {
        if attribute == "*" {
            return Ok(());
        }
        if self
            .fields
            .iter()
            .any(|field| self.target_field(field) == attribute)
        {
            return Ok(());
        }
        bail!(
            "attribut '{}' dans {} pour {} absent des champs du document",
            attribute,
            label,
            self.table
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_table() -> TableConfig {
        TableConfig {
            table: "products".to_owned(),
            index: "products".to_owned(),
            primary_key: "id".to_owned(),
            fields: vec!["id".to_owned(), "name".to_owned()],
            ..TableConfig::default()
        }
    }

    fn config_with_tables(tables: Vec<TableConfig>) -> Config {
        Config {
            mysql: MysqlConfig {
                url: "mysql://user:password@localhost/shop".to_owned(),
                server_id: 42,
            },
            meilisearch: MeilisearchConfig {
                host: "http://localhost:7700".to_owned(),
                ..MeilisearchConfig::default()
            },
            runtime: RuntimeConfig::default(),
            tables,
        }
    }

    #[test]
    fn toml_deserialization_applies_defaults_and_validates() -> Result<()> {
        let config: Config = toml::from_str(
            r#"
                [mysql]
                url = "mysql://user:password@localhost/shop"
                server_id = 42

                [meilisearch]
                host = "http://localhost:7700"

                [[tables]]
                table = "products"
                index = "products"
                primary_key = "id"
                fields = ["id", "name"]
            "#,
        )?;

        config.validate()?;

        assert_eq!(
            config.runtime.state_path,
            std::path::PathBuf::from("meili-sync-state.json")
        );
        assert!(config.runtime.snapshot_on_start);
        assert_eq!(config.meilisearch.batch_size, 1_000);
        assert_eq!(config.meilisearch.max_in_flight_tasks, 8);
        assert_eq!(config.tables[0].snapshot_batch_size, 10_000);
        Ok(())
    }

    #[test]
    fn document_fields_apply_aliases_in_source_order() {
        let mut table = valid_table();
        table
            .field_aliases
            .insert("name".to_owned(), "display_name".to_owned());

        assert_eq!(table.target_field("name"), "display_name");
        assert_eq!(table.target_field("unknown"), "unknown");
        assert_eq!(
            table.document_fields(),
            vec!["id".to_owned(), "display_name".to_owned()]
        );
    }

    #[test]
    fn validate_accepts_attributes_after_aliasing() -> Result<()> {
        let mut table = valid_table();
        table
            .field_aliases
            .insert("name".to_owned(), "display_name".to_owned());
        table.displayed_attributes = Some(vec!["id".to_owned(), "display_name".to_owned()]);
        table.searchable_attributes = Some(vec!["display_name".to_owned()]);
        table.distinct_attribute = Some("display_name".to_owned());

        config_with_tables(vec![table]).validate()
    }

    #[test]
    fn validate_rejects_attribute_not_present_in_document() {
        let mut table = valid_table();
        table
            .field_aliases
            .insert("name".to_owned(), "display_name".to_owned());
        table.filterable_attributes = Some(vec!["name".to_owned()]);

        let error = config_with_tables(vec![table])
            .validate()
            .expect_err("attribute should be rejected when the source name is aliased");

        assert!(error.to_string().contains("absent des champs du document"));
    }

    #[test]
    fn validate_rejects_primary_key_missing_from_fields() {
        let mut table = valid_table();
        table.fields = vec!["name".to_owned()];

        let error = config_with_tables(vec![table])
            .validate()
            .expect_err("primary key must be part of fields");

        assert!(error.to_string().contains("doit etre presente"));
    }

    #[test]
    fn validate_rejects_duplicate_table_index_mapping() {
        let first = valid_table();
        let second = valid_table();

        let error = config_with_tables(vec![first, second])
            .validate()
            .expect_err("duplicate table/index mapping should be rejected");

        assert!(error.to_string().contains("table dupliquee"));
    }
}
