use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use meili_mysql_sync::{
    Config,
    meili::{MeiliSink, SyncOperation},
    state::{BinlogPosition, State, load as load_state, save as save_state},
};
use serde_json::json;

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(test_name: &str) -> std::io::Result<Self> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos());
        let path = std::env::temp_dir().join(format!(
            "meili-mysql-sync-integration-{test_name}-{}-{timestamp}",
            std::process::id()
        ));
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        match fs::remove_dir_all(&self.path) {
            Ok(()) | Err(_) => {}
        }
    }
}

#[test]
fn example_config_loads_and_validates() -> anyhow::Result<()> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.toml");

    let config = Config::from_path(&path)?;

    assert_eq!(config.mysql.server_id, 7_101);
    assert_eq!(config.meilisearch.batch_size, 50_000);
    assert_eq!(config.tables.len(), 1);
    assert_eq!(config.tables[0].table, "clubs");
    assert_eq!(config.tables[0].document_fields(), config.tables[0].fields);
    Ok(())
}

#[tokio::test]
async fn config_state_and_sink_buffer_work_together() -> anyhow::Result<()> {
    let temp_dir = TestDir::new("workflow")?;
    let config_path = temp_dir.path().join("config.toml");
    let state_path = temp_dir.path().join("state").join("binlog.json");
    let config_content = format!(
        r#"
            [mysql]
            url = "mysql://sync_user:sync_password@127.0.0.1:3306/shop"
            server_id = 7102

            [meilisearch]
            host = "http://127.0.0.1:7700"
            api_key = "masterKey"
            batch_size = 3
            max_in_flight_tasks = 2
            task_poll_ms = 5
            task_timeout_secs = 1

            [runtime]
            state_path = '{}'
            snapshot_on_start = false
            flush_interval_ms = 250
            checkpoint_every_events = 10

            [[tables]]
            table = "products"
            index = "products"
            primary_key = "id"
            fields = ["id", "name"]
            field_aliases = {{ name = "title" }}
            displayed_attributes = ["id", "title"]
            searchable_attributes = ["title"]
            snapshot_batch_size = 100
        "#,
        state_path.display()
    );
    fs::write(&config_path, config_content)?;

    let config = Config::from_path(&config_path)?;
    assert_eq!(config.runtime.state_path, state_path);
    assert_eq!(
        config.tables[0].document_fields(),
        vec!["id".to_owned(), "title".to_owned()]
    );

    let position = BinlogPosition {
        file: "mysql-bin.000010".to_owned(),
        pos: 2_048,
    };
    save_state(&config.runtime.state_path, &State::new(position.clone()))?;
    let loaded = load_state(&config.runtime.state_path)?.expect("state should have been saved");
    assert_eq!(loaded.binlog, position);

    let mut sink = MeiliSink::new(&config.meilisearch, "test-run".to_owned(), None)?;
    assert_eq!(sink.batch_size(), 3);

    sink.push(SyncOperation::Upsert {
        index_uid: config.tables[0].index.clone(),
        primary_key: config.tables[0].primary_key.clone(),
        document: json!({ "id": 1, "title": "Rust" }),
    })
    .await?;
    sink.push(SyncOperation::Delete {
        index_uid: config.tables[0].index.clone(),
        primary_key: config.tables[0].primary_key.clone(),
        document_id: "2".to_owned(),
    })
    .await?;

    assert_eq!(sink.pending_operations(), 2);
    Ok(())
}

#[test]
fn cli_reports_invalid_config_before_connecting() -> anyhow::Result<()> {
    let temp_dir = TestDir::new("invalid-cli")?;
    let config_path = temp_dir.path().join("invalid-config.toml");
    fs::write(
        &config_path,
        r#"
            [mysql]
            url = ""
            server_id = 7103

            [meilisearch]
            host = "http://127.0.0.1:7700"

            [[tables]]
            table = "products"
            index = "products"
            primary_key = "id"
            fields = ["id", "name"]
        "#,
    )?;

    let output = Command::new(env!("CARGO_BIN_EXE_meili-mysql-sync"))
        .arg("--config")
        .arg(&config_path)
        .arg("position")
        .output()?;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("mysql.url is required"),
        "unexpected stderr: {stderr}"
    );
    Ok(())
}
