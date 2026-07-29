use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use serde::Serialize;
use serde_json::Value as JsonValue;
use sysinfo::{Pid, ProcessesToUpdate, System, get_current_pid};
use tracing::info;

const RESOURCE_SAMPLE_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    Full,
    Snapshot,
    Cdc,
}

impl SyncMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Snapshot => "snapshot",
            Self::Cdc => "cdc",
        }
    }
}

#[derive(Clone)]
pub struct SyncMetrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    sync_run_id: String,
    sync_mode: SyncMode,
    started_at: Instant,
    state: Arc<Mutex<MetricsState>>,
    sampling: Arc<AtomicBool>,
    sampler: Mutex<Option<thread::JoinHandle<()>>>,
}

#[derive(Debug, Default)]
struct MetricsState {
    documents_read: u64,
    documents_indexed: u64,
    documents_failed: u64,
    bytes_read: u64,
    bytes_sent: u64,
    batches: u64,
    batch_documents: u64,
    mysql_read: Duration,
    transformation: Duration,
    serialization: Duration,
    batch_wait: Duration,
    meilisearch_http: Duration,
    meilisearch_task_wait: Duration,
    checkpoint_write: Duration,
    rss_bytes: u64,
    virtual_memory_bytes: u64,
    peak_rss_bytes: u64,
    cpu_usage_total: f64,
    cpu_samples: u64,
}

impl SyncMetrics {
    #[must_use]
    pub fn start(sync_run_id: String, sync_mode: SyncMode) -> Self {
        let state = Arc::new(Mutex::new(MetricsState::default()));
        let sampling = Arc::new(AtomicBool::new(true));
        let sampler = start_resource_sampler(Arc::clone(&state), Arc::clone(&sampling));

        Self {
            inner: Arc::new(MetricsInner {
                sync_run_id,
                sync_mode,
                started_at: Instant::now(),
                state,
                sampling,
                sampler: Mutex::new(Some(sampler)),
            }),
        }
    }

    pub fn record_document_read(&self, document: &JsonValue) {
        let started_at = Instant::now();
        let bytes = serde_json::to_vec(document).map_or(0, |json| json.len() as u64);
        let elapsed = started_at.elapsed();
        self.with_state(|state| {
            state.documents_read = state.documents_read.saturating_add(1);
            state.bytes_read = state.bytes_read.saturating_add(bytes);
            state.serialization = state.serialization.saturating_add(elapsed);
        });
    }

    pub fn record_transformation(&self, elapsed: Duration) {
        self.with_state(|state| {
            state.transformation = state.transformation.saturating_add(elapsed);
        });
    }

    pub fn record_mysql_read(&self, elapsed: Duration) {
        self.record_duration(|state| &mut state.mysql_read, elapsed);
    }

    pub fn record_batch_wait(&self, elapsed: Duration) {
        self.record_duration(|state| &mut state.batch_wait, elapsed);
    }

    pub fn record_meilisearch_http(&self, elapsed: Duration) {
        self.record_duration(|state| &mut state.meilisearch_http, elapsed);
    }

    pub fn record_meilisearch_task_wait(&self, elapsed: Duration) {
        self.record_duration(|state| &mut state.meilisearch_task_wait, elapsed);
    }

    pub fn record_checkpoint_write(&self, elapsed: Duration) {
        self.record_duration(|state| &mut state.checkpoint_write, elapsed);
    }

    pub fn record_batch<T: Serialize>(&self, document_count: usize, payload: &T) {
        let started_at = Instant::now();
        let bytes = serde_json::to_vec(payload).map_or(0, |json| json.len() as u64);
        let elapsed = started_at.elapsed();
        self.with_state(|state| {
            state.batches = state.batches.saturating_add(1);
            state.batch_documents = state.batch_documents.saturating_add(document_count as u64);
            state.bytes_sent = state.bytes_sent.saturating_add(bytes);
            state.serialization = state.serialization.saturating_add(elapsed);
        });
    }

    pub fn record_task_success(&self, document_count: usize) {
        self.with_state(|state| {
            state.documents_indexed = state
                .documents_indexed
                .saturating_add(document_count as u64);
        });
    }

    pub fn record_task_failure(&self, document_count: usize) {
        self.with_state(|state| {
            state.documents_failed = state.documents_failed.saturating_add(document_count as u64);
        });
    }

    #[allow(clippy::cast_precision_loss)]
    pub fn finish(&self) {
        let duration = self.inner.started_at.elapsed();
        self.inner.sampling.store(false, Ordering::Release);
        let sampler = self
            .inner
            .sampler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(sampler) = sampler {
            let _ = sampler.join();
        }
        self.emit(duration, "terminee", "terminee");
    }

    #[allow(clippy::cast_precision_loss)]
    pub fn report_progress(&self) {
        self.emit(self.inner.started_at.elapsed(), "en cours", "en cours");
    }

    #[allow(clippy::cast_precision_loss)]
    fn emit(&self, duration: Duration, sync_status: &str, timing_status: &str) {
        let state = self.with_state(|state| state.clone());
        let documents_per_second = if duration.is_zero() {
            0.0
        } else {
            state.documents_indexed as f64 / duration.as_secs_f64()
        };
        let average_batch_size = if state.batches == 0 {
            0.0
        } else {
            state.batch_documents as f64 / state.batches as f64
        };
        let average_cpu_percent = if state.cpu_samples == 0 {
            0.0
        } else {
            state.cpu_usage_total / state.cpu_samples as f64
        };

        info!(
            sync_run_id = %self.inner.sync_run_id,
            sync_mode = self.inner.sync_mode.as_str(),
            duration_ms = duration_to_millis(duration),
            documents_read = state.documents_read,
            documents_indexed = state.documents_indexed,
            documents_failed = state.documents_failed,
            documents_per_second,
            bytes_read = state.bytes_read,
            bytes_sent = state.bytes_sent,
            average_batch_size,
            peak_memory_mb = bytes_to_megabytes(state.peak_rss_bytes),
            average_cpu_percent,
            rss_bytes = state.rss_bytes,
            virtual_memory_bytes = state.virtual_memory_bytes,
            peak_rss_bytes = state.peak_rss_bytes,
            "synchronisation {sync_status}"
        );
        info!(
            sync_run_id = %self.inner.sync_run_id,
            sync_duration_ms = duration_to_millis(duration),
            mysql_read_ms = duration_to_millis(state.mysql_read),
            transformation_ms = duration_to_millis(state.transformation),
            serialization_ms = duration_to_millis(state.serialization),
            batch_wait_ms = duration_to_millis(state.batch_wait),
            meilisearch_http_ms = duration_to_millis(state.meilisearch_http),
            meilisearch_task_wait_ms = duration_to_millis(state.meilisearch_task_wait),
            checkpoint_write_ms = duration_to_millis(state.checkpoint_write),
            "repartition du temps de synchronisation {timing_status}"
        );
    }

    fn record_duration(
        &self,
        field: impl FnOnce(&mut MetricsState) -> &mut Duration,
        elapsed: Duration,
    ) {
        self.with_state(|state| {
            let duration = field(state);
            *duration = duration.saturating_add(elapsed);
        });
    }

    fn with_state<T>(&self, operation: impl FnOnce(&mut MetricsState) -> T) -> T {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        operation(&mut state)
    }
}

fn start_resource_sampler(
    state: Arc<Mutex<MetricsState>>,
    sampling: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let Ok(pid) = get_current_pid() else {
            return;
        };
        let mut system = System::new();

        while sampling.load(Ordering::Acquire) {
            sample_resources(&mut system, pid, &state);
            for _ in 0..5 {
                if !sampling.load(Ordering::Acquire) {
                    break;
                }
                thread::sleep(RESOURCE_SAMPLE_INTERVAL / 5);
            }
        }
        sample_resources(&mut system, pid, &state);
    })
}

fn sample_resources(system: &mut System, pid: Pid, state: &Mutex<MetricsState>) {
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    let Some(process) = system.process(pid) else {
        return;
    };
    let rss_bytes = process.memory();
    let virtual_memory_bytes = process.virtual_memory();
    let cpu_usage = f64::from(process.cpu_usage());
    let mut state = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.rss_bytes = rss_bytes;
    state.virtual_memory_bytes = virtual_memory_bytes;
    state.peak_rss_bytes = state.peak_rss_bytes.max(rss_bytes);
    state.cpu_usage_total += cpu_usage;
    state.cpu_samples = state.cpu_samples.saturating_add(1);
}

const fn bytes_to_megabytes(bytes: u64) -> u64 {
    bytes / (1024 * 1024)
}

fn duration_to_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl Clone for MetricsState {
    fn clone(&self) -> Self {
        Self {
            documents_read: self.documents_read,
            documents_indexed: self.documents_indexed,
            documents_failed: self.documents_failed,
            bytes_read: self.bytes_read,
            bytes_sent: self.bytes_sent,
            batches: self.batches,
            batch_documents: self.batch_documents,
            mysql_read: self.mysql_read,
            transformation: self.transformation,
            serialization: self.serialization,
            batch_wait: self.batch_wait,
            meilisearch_http: self.meilisearch_http,
            meilisearch_task_wait: self.meilisearch_task_wait,
            checkpoint_write: self.checkpoint_write,
            rss_bytes: self.rss_bytes,
            virtual_memory_bytes: self.virtual_memory_bytes,
            peak_rss_bytes: self.peak_rss_bytes,
            cpu_usage_total: self.cpu_usage_total,
            cpu_samples: self.cpu_samples,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_mode_uses_documented_values() {
        assert_eq!(SyncMode::Full.as_str(), "full");
        assert_eq!(SyncMode::Snapshot.as_str(), "snapshot");
        assert_eq!(SyncMode::Cdc.as_str(), "cdc");
    }

    #[test]
    fn bytes_to_megabytes_uses_binary_units() {
        assert_eq!(bytes_to_megabytes(2 * 1024 * 1024), 2);
    }
}
