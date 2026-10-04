use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    net::IpAddr,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use mysql_async::Opts;
use serde::Deserialize;
use url::Url;

const MYSQL_URL_ENV: &str = "MEILI_SYNC_MYSQL_URL";
const MYSQL_URL_FILE_ENV: &str = "MEILI_SYNC_MYSQL_URL_FILE";
const MEILISEARCH_API_KEY_ENV: &str = "MEILI_SYNC_MEILISEARCH_API_KEY";
const MEILISEARCH_API_KEY_FILE_ENV: &str = "MEILI_SYNC_MEILISEARCH_API_KEY_FILE";

#[derive(Clone, Deserialize)]
pub struct Config {
    pub mysql: MysqlConfig,
    pub meilisearch: MeilisearchConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    pub tables: Vec<TableConfig>,
}

#[derive(Clone, Deserialize)]
pub struct MysqlConfig {
    pub url: String,
    pub server_id: u32,
    #[serde(default)]
    pub allow_insecure: bool,
}

#[derive(Clone, Deserialize)]
#[serde(default)]
pub struct MeilisearchConfig {
    pub host: String,
    pub api_key: Option<String>,
    pub allow_insecure: bool,
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
            allow_insecure: false,
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
            .with_context(|| format!("reading configuration {}", path.display()))?;
        let mut config: Self = toml::from_str(&content)
            .with_context(|| format!("parsing TOML in {}", path.display()))?;
        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.mysql.url.trim().is_empty() {
            bail!("mysql.url is required");
        }
        if self.mysql.server_id == 0 {
            bail!("mysql.server_id must be unique and greater than 0");
        }
        let mysql_opts = Opts::from_url(&self.mysql.url).context("parsing mysql.url")?;
        if mysql_opts.ssl_opts().is_none()
            && !is_loopback_host(mysql_opts.ip_or_hostname())
            && !self.mysql.allow_insecure
        {
            bail!(concat!(
                "mysql.url must enable TLS with require_ssl=true for non-loopback hosts; ",
                "mysql.allow_insecure=true explicitly allows an exception"
            ));
        }
        if self.meilisearch.host.trim().is_empty() {
            bail!("meilisearch.host is required");
        }
        let meilisearch_url =
            Url::parse(&self.meilisearch.host).context("parsing meilisearch.host")?;
        if !matches!(meilisearch_url.scheme(), "http" | "https") {
            bail!("meilisearch.host must use the http or https scheme");
        }
        let meilisearch_host = meilisearch_url
            .host_str()
            .context("meilisearch.host must contain a hostname")?;
        if meilisearch_url.scheme() != "https"
            && !is_loopback_host(meilisearch_host)
            && !self.meilisearch.allow_insecure
        {
            bail!(concat!(
                "meilisearch.host must use HTTPS for non-loopback hosts; ",
                "meilisearch.allow_insecure=true explicitly allows an exception"
            ));
        }
        if self.meilisearch.batch_size == 0 {
            bail!("meilisearch.batch_size must be greater than 0");
        }
        if self.meilisearch.max_in_flight_tasks == 0 {
            bail!("meilisearch.max_in_flight_tasks must be greater than 0");
        }
        if self.tables.is_empty() {
            bail!("at least one table must be configured");
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
                    "duplicate table in configuration: {} -> {}",
                    table.table,
                    table.index
                );
            }
        }
        Ok(())
    }

    fn apply_env_overrides(&mut self) -> Result<()> {
        if let Some(url) = read_secret(MYSQL_URL_ENV, MYSQL_URL_FILE_ENV)? {
            self.mysql.url = url;
        }
        if let Some(api_key) = read_secret(MEILISEARCH_API_KEY_ENV, MEILISEARCH_API_KEY_FILE_ENV)? {
            self.meilisearch.api_key = Some(api_key);
        }
        Ok(())
    }
}

fn read_secret(value_env: &str, file_env: &str) -> Result<Option<String>> {
    let value = read_env(value_env)?;
    let file = read_env(file_env)?;
    let secret = match (value, file) {
        (Some(_), Some(_)) => {
            bail!("{value_env} and {file_env} cannot be set together")
        }
        (Some(value), None) => value,
        (None, Some(path)) => fs::read_to_string(&path)
            .with_context(|| format!("reading the secret specified by {file_env}: {path}"))?
            .trim_end_matches(['\r', '\n'])
            .to_owned(),
        (None, None) => return Ok(None),
    };
    if secret.is_empty() {
        bail!("the secret provided by {value_env} or {file_env} is empty");
    }
    Ok(Some(secret))
}

fn read_env(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => {
            bail!("environment variable {name} is not valid UTF-8")
        }
    }
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

impl TableConfig {
    fn validate(&self) -> Result<()> {
        if self.table.trim().is_empty() {
            bail!("tables[].table is required");
        }
        if self.index.trim().is_empty() {
            bail!("tables[].index is required for {}", self.table);
        }
        if self.primary_key.trim().is_empty() {
            bail!("tables[].primary_key is required for {}", self.table);
        }
        if self.fields.is_empty() {
            bail!("tables[].fields cannot be empty for {}", self.table);
        }
        if !self.fields.iter().any(|field| field == &self.primary_key) {
            bail!(
                "primary key '{}' must be included in fields for {}",
                self.primary_key,
                self.table
            );
        }
        if self.snapshot_batch_size == 0 {
            bail!(
                "snapshot_batch_size must be greater than 0 for {}",
                self.table
            );
        }
        let mut seen_fields = BTreeSet::new();
        for field in &self.fields {
            if field.trim().is_empty() {
                bail!("empty field name in {}", self.table);
            }
            if !seen_fields.insert(field) {
                bail!("duplicate field in {}: {}", self.table, field);
            }
        }
        for field in &self.watch_fields {
            if field.trim().is_empty() {
                bail!("empty watch_field name in {}", self.table);
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
                    "duplicate attribute in {} for {}: {}",
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
                bail!("empty value in {} for {}", label, self.table);
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
            "attribute '{}' in {} for {} is missing from document fields",
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
                allow_insecure: false,
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

        assert!(error.to_string().contains("missing from document fields"));
    }

    #[test]
    fn validate_rejects_primary_key_missing_from_fields() {
        let mut table = valid_table();
        table.fields = vec!["name".to_owned()];

        let error = config_with_tables(vec![table])
            .validate()
            .expect_err("primary key must be part of fields");

        assert!(error.to_string().contains("must be included"));
    }

    #[test]
    fn validate_rejects_duplicate_table_index_mapping() {
        let first = valid_table();
        let second = valid_table();

        let error = config_with_tables(vec![first, second])
            .validate()
            .expect_err("duplicate table/index mapping should be rejected");

        assert!(error.to_string().contains("duplicate table"));
    }

    #[test]
    fn validate_rejects_unencrypted_remote_mysql() {
        let mut config = config_with_tables(vec![valid_table()]);
        config.mysql.url = "mysql://user:password@database.example/shop".to_owned();

        let error = config
            .validate()
            .expect_err("remote MySQL without TLS should be rejected");

        assert!(error.to_string().contains("require_ssl=true"));
    }

    #[test]
    fn validate_rejects_unencrypted_remote_meilisearch() {
        let mut config = config_with_tables(vec![valid_table()]);
        config.meilisearch.host = "http://search.example:7700".to_owned();

        let error = config
            .validate()
            .expect_err("remote Meilisearch without TLS should be rejected");

        assert!(error.to_string().contains("must use HTTPS"));
    }

    #[test]
    fn validate_accepts_encrypted_remote_services() -> Result<()> {
        let mut config = config_with_tables(vec![valid_table()]);
        config.mysql.url =
            "mysql://user:password@database.example/shop?require_ssl=true".to_owned();
        config.meilisearch.host = "https://search.example:7700".to_owned();

        config.validate()
    }

    #[test]
    fn validate_requires_explicit_override_for_insecure_private_networks() -> Result<()> {
        let mut config = config_with_tables(vec![valid_table()]);
        config.mysql.url = "mysql://user:password@mysql/shop".to_owned();
        config.mysql.allow_insecure = true;
        config.meilisearch.host = "http://meilisearch:7700".to_owned();
        config.meilisearch.allow_insecure = true;

        config.validate()
    }
}
