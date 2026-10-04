use std::{
    collections::{BTreeMap, VecDeque},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use meilisearch_sdk::{
    client::Client,
    errors::{Error as MeiliError, ErrorCode},
    settings::Settings,
    task_info::TaskInfo,
    tasks::Task,
};
use serde_json::Value as JsonValue;
use tracing::{error, info};

use crate::config::{MeilisearchConfig, TableConfig};
use crate::metrics::SyncMetrics;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BufferKey {
    index_uid: String,
    primary_key: String,
}

#[derive(Debug, Clone)]
enum BufferedOperation {
    Upsert {
        document: JsonValue,
        event_id: Option<String>,
    },
    Delete {
        document_id: String,
        event_id: Option<String>,
    },
}

#[derive(Debug, Default)]
struct IndexBuffer {
    operations: Vec<BufferedOperation>,
}

#[derive(Debug, Clone)]
pub enum SyncOperation {
    Upsert {
        index_uid: String,
        primary_key: String,
        document: JsonValue,
    },
    Delete {
        index_uid: String,
        primary_key: String,
        document_id: String,
    },
}

#[derive(Debug, Clone, Copy, Default)]
pub struct IndexPreparation {
    pub recreate: bool,
    pub clear_documents: bool,
}

pub struct MeiliSink {
    client: Client,
    sync_run_id: String,
    batch_size: usize,
    next_batch_id: u64,
    max_in_flight_tasks: usize,
    task_poll: Duration,
    task_timeout: Duration,
    buffers: BTreeMap<BufferKey, IndexBuffer>,
    in_flight: VecDeque<PendingSyncTask>,
    metrics: Option<SyncMetrics>,
}

impl MeiliSink {
    pub fn new(
        config: &MeilisearchConfig,
        sync_run_id: String,
        metrics: Option<SyncMetrics>,
    ) -> Result<Self> {
        let client = Client::new(config.host.clone(), config.api_key.as_deref())
            .map_err(sanitize_error)
            .context("creating the Meilisearch client")?;
        info!(sync_run_id, "synchronization session started");
        Ok(Self {
            client,
            sync_run_id,
            batch_size: config.batch_size,
            next_batch_id: 1,
            max_in_flight_tasks: config.max_in_flight_tasks,
            task_poll: Duration::from_millis(config.task_poll_ms),
            task_timeout: Duration::from_secs(config.task_timeout_secs),
            buffers: BTreeMap::new(),
            in_flight: VecDeque::new(),
            metrics,
        })
    }

    pub async fn prepare_index(
        &mut self,
        table: &TableConfig,
        preparation: IndexPreparation,
    ) -> Result<()> {
        self.flush_all().await?;
        self.wait_all().await?;

        let index_exists = self.index_exists(&table.index).await?;
        if preparation.recreate && index_exists {
            info!(index = %table.index, "deleting the Meilisearch index");
            let started_at = Instant::now();
            let result = self.client.delete_index(&table.index).await;
            self.record_http_duration(started_at.elapsed());
            let task = result
                .map_err(sanitize_error)
                .with_context(|| format!("deleting index {}", table.index))?;
            self.wait_for_task(task).await?;
        }

        if preparation.recreate || !index_exists {
            info!(
                index = %table.index,
                primary_key = %table.primary_key,
                "creating the Meilisearch index"
            );
            let started_at = Instant::now();
            let result = self
                .client
                .create_index(&table.index, Some(&table.primary_key))
                .await;
            self.record_http_duration(started_at.elapsed());
            let task = result
                .map_err(sanitize_error)
                .with_context(|| format!("creating index {}", table.index))?;
            self.wait_for_task(task).await?;
        } else if preparation.clear_documents {
            info!(index = %table.index, "deleting Meilisearch documents");
            let started_at = Instant::now();
            let result = self.client.index(&table.index).delete_all_documents().await;
            self.record_http_duration(started_at.elapsed());
            let task = result
                .map_err(sanitize_error)
                .with_context(|| format!("deleting documents in {}", table.index))?;
            self.wait_for_task(task).await?;
        }

        self.apply_settings(table).await
    }

    pub async fn push(&mut self, operation: SyncOperation) -> Result<()> {
        self.push_for_event(operation, None).await
    }

    pub async fn push_for_event(
        &mut self,
        operation: SyncOperation,
        event_id: impl Into<Option<String>>,
    ) -> Result<()> {
        let event_id = event_id.into();
        let key = match &operation {
            SyncOperation::Upsert {
                index_uid,
                primary_key,
                ..
            }
            | SyncOperation::Delete {
                index_uid,
                primary_key,
                ..
            } => BufferKey {
                index_uid: index_uid.clone(),
                primary_key: primary_key.clone(),
            },
        };

        let buffered = match operation {
            SyncOperation::Upsert { document, .. } => {
                BufferedOperation::Upsert { document, event_id }
            }
            SyncOperation::Delete { document_id, .. } => BufferedOperation::Delete {
                document_id,
                event_id,
            },
        };

        let should_flush = {
            let buffer = self.buffers.entry(key.clone()).or_default();
            buffer.operations.push(buffered);
            buffer.operations.len() >= self.batch_size
        };

        if should_flush {
            self.flush_key(&key).await?;
        }

        Ok(())
    }

    pub async fn push_documents(
        &mut self,
        index_uid: &str,
        primary_key: &str,
        documents: Vec<JsonValue>,
    ) -> Result<()> {
        if documents.is_empty() {
            return Ok(());
        }
        let key = BufferKey {
            index_uid: index_uid.to_owned(),
            primary_key: primary_key.to_owned(),
        };
        let documents = documents
            .into_iter()
            .map(|document| (document, None))
            .collect::<Vec<_>>();
        self.submit_upserts(&key, &documents).await
    }

    #[must_use]
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    #[must_use]
    pub fn sync_run_id(&self) -> &str {
        &self.sync_run_id
    }

    #[must_use]
    pub fn pending_operations(&self) -> usize {
        self.buffers
            .values()
            .map(|buffer| buffer.operations.len())
            .sum()
    }

    pub async fn flush_all(&mut self) -> Result<()> {
        let keys = self.buffers.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            self.flush_key(&key).await?;
        }
        Ok(())
    }

    pub async fn wait_all(&mut self) -> Result<()> {
        while let Some(task) = self.in_flight.pop_front() {
            self.wait_for_sync_task(task).await?;
        }
        Ok(())
    }

    async fn flush_key(&mut self, key: &BufferKey) -> Result<()> {
        let Some(buffer) = self.buffers.remove(key) else {
            return Ok(());
        };

        let mut upserts = Vec::with_capacity(self.batch_size);
        let mut deletes = Vec::with_capacity(self.batch_size);
        let mut current_kind = None;

        for operation in buffer.operations {
            match operation {
                BufferedOperation::Upsert { document, event_id } => {
                    if current_kind == Some(OperationKind::Delete) {
                        self.submit_deletes(key, &deletes).await?;
                        deletes.clear();
                    }
                    current_kind = Some(OperationKind::Upsert);
                    upserts.push((document, event_id));
                    if upserts.len() == self.batch_size {
                        self.submit_upserts(key, &upserts).await?;
                        upserts.clear();
                    }
                }
                BufferedOperation::Delete {
                    document_id,
                    event_id,
                } => {
                    if current_kind == Some(OperationKind::Upsert) {
                        self.submit_upserts(key, &upserts).await?;
                        upserts.clear();
                    }
                    current_kind = Some(OperationKind::Delete);
                    deletes.push((document_id, event_id));
                    if deletes.len() == self.batch_size {
                        self.submit_deletes(key, &deletes).await?;
                        deletes.clear();
                    }
                }
            }
        }

        if !upserts.is_empty() {
            self.submit_upserts(key, &upserts).await?;
        }
        if !deletes.is_empty() {
            self.submit_deletes(key, &deletes).await?;
        }

        Ok(())
    }

    async fn submit_upserts(
        &mut self,
        key: &BufferKey,
        documents: &[(JsonValue, Option<String>)],
    ) -> Result<()> {
        let document_ids = documents
            .iter()
            .map(|(document, _)| document_id(document, &key.primary_key))
            .collect::<Vec<_>>();
        let event_ids = event_ids(
            documents
                .iter()
                .filter_map(|(_, event_id)| event_id.as_deref()),
        );
        let document_values = documents
            .iter()
            .map(|(document, _)| document)
            .collect::<Vec<_>>();
        let index = self.client.index(&key.index_uid);
        if let Some(metrics) = &self.metrics {
            metrics.record_batch(document_values.len(), &document_values);
        }
        let started_at = Instant::now();
        let result = index
            .add_documents(&document_values, Some(&key.primary_key))
            .await;
        if let Some(metrics) = &self.metrics {
            metrics.record_meilisearch_http(started_at.elapsed());
        }
        let task = match result {
            Ok(task) => task,
            Err(error) => {
                if let Some(metrics) = &self.metrics {
                    metrics.record_task_failure(document_values.len());
                }
                return Err(sanitize_error(error)).with_context(|| {
                    format!(
                        "sending {} documents to {}",
                        document_values.len(),
                        key.index_uid
                    )
                });
            }
        };
        self.track_task(task, key, OperationKind::Upsert, document_ids, event_ids)
            .await
    }

    async fn submit_deletes(
        &mut self,
        key: &BufferKey,
        documents: &[(String, Option<String>)],
    ) -> Result<()> {
        let document_ids = documents
            .iter()
            .map(|(document_id, _)| document_id.clone())
            .collect::<Vec<_>>();
        let event_ids = event_ids(
            documents
                .iter()
                .filter_map(|(_, event_id)| event_id.as_deref()),
        );
        let index = self.client.index(&key.index_uid);
        if let Some(metrics) = &self.metrics {
            metrics.record_batch(document_ids.len(), &document_ids);
        }
        let started_at = Instant::now();
        let result = index.delete_documents(&document_ids).await;
        if let Some(metrics) = &self.metrics {
            metrics.record_meilisearch_http(started_at.elapsed());
        }
        let task = match result {
            Ok(task) => task,
            Err(error) => {
                if let Some(metrics) = &self.metrics {
                    metrics.record_task_failure(document_ids.len());
                }
                return Err(sanitize_error(error)).with_context(|| {
                    format!(
                        "deleting {} documents in {}",
                        document_ids.len(),
                        key.index_uid
                    )
                });
            }
        };
        self.track_task(task, key, OperationKind::Delete, document_ids, event_ids)
            .await
    }

    async fn index_exists(&self, index_uid: &str) -> Result<bool> {
        let started_at = Instant::now();
        let result = self.client.get_index(index_uid).await;
        self.record_http_duration(started_at.elapsed());
        match result {
            Ok(_) => Ok(true),
            Err(error) if is_index_not_found(&error) => Ok(false),
            Err(error) => {
                Err(sanitize_error(error)).with_context(|| format!("reading index {index_uid}"))
            }
        }
    }

    async fn apply_settings(&self, table: &TableConfig) -> Result<()> {
        let settings = settings_for_table(table);
        let started_at = Instant::now();
        let result = self
            .client
            .index(&table.index)
            .set_settings(&settings)
            .await;
        self.record_http_duration(started_at.elapsed());
        let task = result
            .map_err(sanitize_error)
            .with_context(|| format!("configuring index {}", table.index))?;
        self.wait_for_task(task).await
    }

    fn record_http_duration(&self, elapsed: Duration) {
        if let Some(metrics) = &self.metrics {
            metrics.record_meilisearch_http(elapsed);
        }
    }

    async fn track_task(
        &mut self,
        task: TaskInfo,
        key: &BufferKey,
        operation: OperationKind,
        document_ids: Vec<String>,
        event_ids: Vec<String>,
    ) -> Result<()> {
        let batch_id = self.next_batch_id;
        self.next_batch_id = self.next_batch_id.saturating_add(1);
        info!(
            sync_run_id = %self.sync_run_id,
            batch_id,
            meilisearch_task_uid = task.task_uid,
            operation = operation.as_str(),
            index = %key.index_uid,
            primary_key = %key.primary_key,
            documents = document_ids.len(),
            event_ids = ?event_ids,
            "synchronization batch submitted to Meilisearch"
        );
        for event_id in &event_ids {
            info!(
                sync_run_id = %self.sync_run_id,
                batch_id,
                event_id,
                meilisearch_task_uid = task.task_uid,
                "CDC event associated with the Meilisearch task"
            );
        }
        self.in_flight.push_back(PendingSyncTask {
            task,
            batch_id,
            index_uid: key.index_uid.clone(),
            primary_key: key.primary_key.clone(),
            operation,
            document_ids,
            event_ids,
        });
        while self.in_flight.len() >= self.max_in_flight_tasks {
            let task = self
                .in_flight
                .pop_front()
                .context("inconsistent Meilisearch task queue")?;
            let started_at = Instant::now();
            let result = self.wait_for_sync_task(task).await;
            if let Some(metrics) = &self.metrics {
                metrics.record_batch_wait(started_at.elapsed());
            }
            result?;
        }
        Ok(())
    }

    async fn wait_for_sync_task(&self, task: PendingSyncTask) -> Result<()> {
        let task_uid = task.task.task_uid;
        let completed_task = self.wait_for_task_status(task.task).await?;
        if completed_task.is_failure() {
            let failure = completed_task.unwrap_failure();
            error!(
                sync_run_id = %self.sync_run_id,
                batch_id = task.batch_id,
                meilisearch_task_uid = task_uid,
                error_code = %failure.error_code,
                operation = task.operation.as_str(),
                index = %task.index_uid,
                primary_key = %task.primary_key,
                documents = task.document_ids.len(),
                event_ids = ?task.event_ids,
                "Meilisearch task failed"
            );
            if let Some(metrics) = &self.metrics {
                metrics.record_task_failure(task.document_ids.len());
            }
            return Err(task_failure(task_uid, &failure.error_code));
        }
        info!(
            sync_run_id = %self.sync_run_id,
            batch_id = task.batch_id,
            meilisearch_task_uid = task_uid,
            operation = task.operation.as_str(),
            index = %task.index_uid,
            primary_key = %task.primary_key,
            documents = task.document_ids.len(),
            event_ids = ?task.event_ids,
            "Meilisearch task completed successfully"
        );
        if let Some(metrics) = &self.metrics {
            metrics.record_task_success(task.document_ids.len());
        }
        Ok(())
    }

    async fn wait_for_task(&self, task: TaskInfo) -> Result<()> {
        let task = self.wait_for_task_status(task).await?;
        let task_uid = task.get_uid();
        if task.is_failure() {
            let failure = task.unwrap_failure();
            error!(
                meilisearch_task_uid = task_uid,
                error_code = %failure.error_code,
                "Meilisearch task failed"
            );
            return Err(task_failure(task_uid, &failure.error_code));
        }
        Ok(())
    }

    async fn wait_for_task_status(&self, task: TaskInfo) -> Result<Task> {
        let task_uid = task.task_uid;
        let started_at = Instant::now();
        let result = self
            .client
            .wait_for_task(task, Some(self.task_poll), Some(self.task_timeout))
            .await;
        if let Some(metrics) = &self.metrics {
            metrics.record_meilisearch_task_wait(started_at.elapsed());
        }
        result
            .map_err(sanitize_error)
            .with_context(|| format!("waiting for Meilisearch task {task_uid}"))
    }
}

// SDK errors can embed documents, document IDs, HTTP bodies, and URLs.
// Discard their sources as well, since anyhow prints the complete error chain.
fn sanitize_error(error: MeiliError) -> anyhow::Error {
    match error {
        MeiliError::Meilisearch(error) => {
            anyhow::anyhow!("Meilisearch error (code: {})", error.error_code)
        }
        MeiliError::MeilisearchCommunication(error) => anyhow::anyhow!(
            "Meilisearch communication error (HTTP {})",
            error.status_code
        ),
        MeiliError::Timeout => anyhow::anyhow!("Meilisearch task wait timed out"),
        MeiliError::HttpError(error) if error.is_timeout() => {
            anyhow::anyhow!("Meilisearch HTTP request timed out")
        }
        MeiliError::HttpError(_) => anyhow::anyhow!("Meilisearch HTTP request failed"),
        MeiliError::ParseError(_) => anyhow::anyhow!("invalid Meilisearch JSON response"),
        MeiliError::InvalidRequest => anyhow::anyhow!("invalid Meilisearch request"),
        _ => anyhow::anyhow!("Meilisearch client failed"),
    }
}

fn task_failure(task_uid: u32, error_code: &ErrorCode) -> anyhow::Error {
    anyhow::anyhow!("Meilisearch task {task_uid} failed (code: {error_code})")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationKind {
    Upsert,
    Delete,
}

impl OperationKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Upsert => "upsert",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug)]
struct PendingSyncTask {
    task: TaskInfo,
    batch_id: u64,
    index_uid: String,
    primary_key: String,
    operation: OperationKind,
    document_ids: Vec<String>,
    event_ids: Vec<String>,
}

fn event_ids<'a>(event_ids: impl Iterator<Item = &'a str>) -> Vec<String> {
    let mut unique_event_ids = Vec::new();
    for event_id in event_ids {
        if !unique_event_ids.iter().any(|current| current == event_id) {
            unique_event_ids.push(event_id.to_owned());
        }
    }
    unique_event_ids
}

fn document_id(document: &JsonValue, primary_key: &str) -> String {
    document.get(primary_key).map_or_else(
        || format!("<missing key '{primary_key}'>"),
        JsonValue::to_string,
    )
}

fn settings_for_table(table: &TableConfig) -> Settings {
    let displayed_attributes = table
        .displayed_attributes
        .clone()
        .unwrap_or_else(|| table.document_fields());
    let mut settings = Settings::new().with_displayed_attributes(displayed_attributes);

    if let Some(searchable_attributes) = &table.searchable_attributes {
        settings = settings.with_searchable_attributes(searchable_attributes);
    }
    if let Some(filterable_attributes) = &table.filterable_attributes {
        settings = settings.with_filterable_attributes(filterable_attributes);
    }
    if let Some(sortable_attributes) = &table.sortable_attributes {
        settings = settings.with_sortable_attributes(sortable_attributes);
    }
    if let Some(ranking_rules) = &table.ranking_rules {
        settings = settings.with_ranking_rules(ranking_rules);
    }
    if let Some(distinct_attribute) = &table.distinct_attribute {
        settings = settings.with_distinct_attribute(Some(distinct_attribute));
    }

    settings
}

fn is_index_not_found(error: &MeiliError) -> bool {
    matches!(
        error,
        MeiliError::Meilisearch(error) if error.error_code == ErrorCode::IndexNotFound
    )
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{Arc, Mutex},
        thread,
    };

    use meilisearch_sdk::errors::{ErrorType, MeilisearchCommunicationError, MeilisearchError};
    use serde_json::json;

    use super::*;

    #[test]
    fn api_errors_discard_payloads_and_error_sources() {
        let error = MeiliError::Meilisearch(MeilisearchError {
            error_message: "Document: {email: private@example.com}".into(),
            error_code: ErrorCode::MissingDocumentId,
            error_type: ErrorType::InvalidRequest,
            error_link: "https://example.com/private-id".into(),
        });
        let error = sanitize_error(error).context("submission failed");
        let output = format!("{error:#?} {error:#}");
        assert!(output.contains("missing_document_id"));
        assert!(!output.contains("private"));
    }

    #[test]
    fn communication_errors_discard_body_and_url() {
        let error = MeiliError::MeilisearchCommunication(MeilisearchCommunicationError {
            status_code: 500,
            message: Some("private-document".into()),
            url: "https://example.com/documents/private-id".into(),
        });
        let error = sanitize_error(error);
        let output = format!("{error:#?} {error:#}");
        assert!(output.contains("HTTP 500"));
        assert!(!output.contains("private"));
        assert!(!output.contains("https://"));
    }

    #[derive(Clone)]
    struct LogWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for LogWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("log buffer poisoned")
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    // Exercise the SDK polling path and both task-failure reporting paths without real services.
    #[tokio::test]
    async fn failed_tasks_do_not_expose_documents_in_logs_or_errors() -> Result<()> {
        let payload = json!({
            "uid": 7, "indexUid": "products", "status": "failed",
            "type": "documentAdditionOrUpdate", "duration": "PT0.001S",
            "enqueuedAt": "2026-10-04T00:00:00Z", "startedAt": "2026-10-04T00:00:00Z",
            "finishedAt": "2026-10-04T00:00:00Z",
            "error": { "message": "Document: {email: private@example.com}",
                "code": "missing_document_id", "type": "invalid_request",
                "link": "https://example.com/private-id" }
        })
        .to_string();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let host = format!("http://{}", listener.local_addr()?);
        let server = thread::spawn(move || -> std::io::Result<()> {
            let deadline = Instant::now() + Duration::from_secs(10);
            for _ in 0..4 {
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => return Err(error),
                    }
                };
                socket.set_read_timeout(Some(Duration::from_secs(2)))?;
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    socket.read_exact(&mut byte)?;
                    request.push(byte[0]);
                }
                write!(
                    socket,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                    payload.len()
                )?;
            }
            Ok(())
        });
        let logs = LogWriter(Arc::new(Mutex::new(Vec::new())));
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let sink = MeiliSink::new(
            &MeilisearchConfig {
                host,
                task_timeout_secs: 2,
                ..MeilisearchConfig::default()
            },
            "test-run".into(),
            None,
        )?;
        let task: TaskInfo = serde_json::from_value(json!({
            "taskUid": 7, "indexUid": "products", "status": "enqueued",
            "type": "documentAdditionOrUpdate", "enqueuedAt": "2026-10-04T00:00:00Z"
        }))?;
        let preparation_error = sink
            .wait_for_task(task.clone())
            .await
            .expect_err("task must fail");
        let sync_error = sink
            .wait_for_sync_task(PendingSyncTask {
                task,
                batch_id: 1,
                index_uid: "products".into(),
                primary_key: "id".into(),
                operation: OperationKind::Upsert,
                document_ids: vec!["private-id".into()],
                event_ids: vec!["mysql-bin.000001:100".into()],
            })
            .await
            .expect_err("sync task must fail");
        server.join().expect("mock server panicked")?;
        let output = format!(
            "{preparation_error:#?} {sync_error:#?} {}",
            String::from_utf8(logs.0.lock().expect("log buffer poisoned").clone())?
        );
        assert!(output.contains("missing_document_id"));
        assert!(!output.contains("private"));
        Ok(())
    }
}
