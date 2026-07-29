use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use meilisearch_sdk::{
    client::Client,
    errors::{Error as MeiliError, ErrorCode},
    settings::Settings,
    task_info::TaskInfo,
    tasks::Task,
};
use serde_json::Value as JsonValue;
use tracing::{error, info};
use uuid::Uuid;

use crate::config::{MeilisearchConfig, TableConfig};

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
}

impl MeiliSink {
    pub fn new(config: &MeilisearchConfig) -> Result<Self> {
        let client = Client::new(config.host.clone(), config.api_key.as_deref())
            .context("creation du client Meilisearch")?;
        let sync_run_id = Uuid::now_v7().to_string();
        info!(sync_run_id, "session de synchronisation demarree");
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
            info!(index = %table.index, "suppression de l'index Meilisearch");
            let task = self
                .client
                .delete_index(&table.index)
                .await
                .with_context(|| format!("suppression de l'index {}", table.index))?;
            self.wait_for_task(task).await?;
        }

        if preparation.recreate || !index_exists {
            info!(
                index = %table.index,
                primary_key = %table.primary_key,
                "creation de l'index Meilisearch"
            );
            let task = self
                .client
                .create_index(&table.index, Some(&table.primary_key))
                .await
                .with_context(|| format!("creation de l'index {}", table.index))?;
            self.wait_for_task(task).await?;
        } else if preparation.clear_documents {
            info!(index = %table.index, "suppression des documents Meilisearch");
            let task = self
                .client
                .index(&table.index)
                .delete_all_documents()
                .await
                .with_context(|| format!("suppression des documents dans {}", table.index))?;
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
        let task = index
            .add_documents(&document_values, Some(&key.primary_key))
            .await
            .with_context(|| {
                format!(
                    "envoi de {} documents vers {}",
                    document_values.len(),
                    key.index_uid
                )
            })?;
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
        let task = index
            .delete_documents(&document_ids)
            .await
            .with_context(|| {
                format!(
                    "suppression de {} documents dans {}",
                    document_ids.len(),
                    key.index_uid
                )
            })?;
        self.track_task(task, key, OperationKind::Delete, document_ids, event_ids)
            .await
    }

    async fn index_exists(&self, index_uid: &str) -> Result<bool> {
        match self.client.get_index(index_uid).await {
            Ok(_) => Ok(true),
            Err(error) if is_index_not_found(&error) => Ok(false),
            Err(error) => Err(error).with_context(|| format!("lecture de l'index {index_uid}")),
        }
    }

    async fn apply_settings(&self, table: &TableConfig) -> Result<()> {
        let settings = settings_for_table(table);
        let task = self
            .client
            .index(&table.index)
            .set_settings(&settings)
            .await
            .with_context(|| format!("configuration de l'index {}", table.index))?;
        self.wait_for_task(task).await
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
            document_ids = ?document_ids,
            event_ids = ?event_ids,
            "lot de synchronisation soumis a Meilisearch"
        );
        for event_id in &event_ids {
            info!(
                sync_run_id = %self.sync_run_id,
                batch_id,
                event_id,
                meilisearch_task_uid = task.task_uid,
                "evenement CDC rattache a la tache Meilisearch"
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
                .context("file de taches Meilisearch incoherente")?;
            self.wait_for_sync_task(task).await?;
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
                error_message = %failure.error_message,
                operation = task.operation.as_str(),
                index = %task.index_uid,
                primary_key = %task.primary_key,
                document_ids = ?task.document_ids,
                event_ids = ?task.event_ids,
                "tache Meilisearch echouee"
            );
            bail!("tache Meilisearch {task_uid} en echec: {failure}");
        }
        info!(
            sync_run_id = %self.sync_run_id,
            batch_id = task.batch_id,
            meilisearch_task_uid = task_uid,
            operation = task.operation.as_str(),
            index = %task.index_uid,
            primary_key = %task.primary_key,
            documents = task.document_ids.len(),
            document_ids = ?task.document_ids,
            event_ids = ?task.event_ids,
            "tache Meilisearch terminee avec succes"
        );
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
                error_message = %failure.error_message,
                "tache Meilisearch echouee"
            );
            bail!("tache Meilisearch {task_uid} en echec: {failure}");
        }
        Ok(())
    }

    async fn wait_for_task_status(&self, task: TaskInfo) -> Result<Task> {
        let task_uid = task.task_uid;
        self.client
            .wait_for_task(task, Some(self.task_poll), Some(self.task_timeout))
            .await
            .with_context(|| format!("attente de la tache Meilisearch {task_uid}"))
    }
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
        || format!("<cle '{primary_key}' absente>"),
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
