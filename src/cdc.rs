use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use mysql_async::{
    BinlogStreamRequest, Conn, Opts, Pool,
    binlog::events::{EventData, RowsEventData},
};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, info, warn};

use crate::{
    Config,
    meili::{MeiliSink, SyncOperation},
    metrics::SyncMetrics,
    mysql::{
        RowOperation, TablePlan, fetch_document_by_pk, operation_from_rows, plans_for_table,
        rows_event_rows,
    },
    state::{BinlogPosition, State, save as save_state},
    value::mysql_value_to_document_id,
};

pub async fn run(
    config: &Config,
    pool: &Pool,
    plans: &[TablePlan],
    sink: &mut MeiliSink,
    start: BinlogPosition,
    metrics: Option<&SyncMetrics>,
) -> Result<()> {
    let opts = Opts::from_url(&config.mysql.url).context("parsing mysql.url")?;
    let conn = Conn::new(opts)
        .await
        .context("connecting to MySQL for the binlog")?;
    let file_bytes = start.file.clone().into_bytes();
    let request = BinlogStreamRequest::new(config.mysql.server_id)
        .with_filename(&file_bytes)
        .with_pos(start.pos);
    let mut stream = conn
        .get_binlog_stream(request)
        .await
        .context("opening the binlog stream")?;

    let mut positions = CheckpointPositions::new(start);
    let mut events_since_checkpoint = 0_u64;
    let mut events_since_metrics = 0_u64;
    let mut flush_timer = interval(Duration::from_millis(config.runtime.flush_interval_ms));
    flush_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    info!(
        file = %positions.current.file,
        pos = positions.current.pos,
        "binlog streaming started"
    );

    loop {
        tokio::select! {
            biased;
            signal = tokio::signal::ctrl_c() => {
                signal.context("waiting for the Ctrl+C signal")?;
                info!("shutdown requested, flushing pending CDC operations");
                checkpoint(sink, &config.runtime.state_path, &positions.safe, metrics).await?;
                stream.close().await.context("closing the binlog stream")?;
                return Ok(());
            }
            _ = flush_timer.tick() => {
                if events_since_checkpoint > 0 {
                    checkpoint(sink, &config.runtime.state_path, &positions.safe, metrics).await?;
                    events_since_checkpoint = 0;
                }
                if events_since_metrics > 0 {
                    report_progress(metrics);
                    events_since_metrics = 0;
                }
            }
            event = stream.next() => {
                let Some(event) = event else {
                    warn!("binlog stream ended by the server");
                    checkpoint(sink, &config.runtime.state_path, &positions.safe, metrics).await?;
                    return Ok(());
                };
                let event = event.context("reading a binlog event")?;
                if !positions.compressed_transaction {
                    update_position_from_header(&mut positions.current, &event);
                }
                let event_id = format!("{}:{}", positions.current.file, positions.current.pos);

                let data = event.read_data().context("decoding a binlog event")?;
                if let Some(data) = data {
                    positions.observe(&data);
                    if let EventData::RowsEvent(rows_event) = data {
                        process_rows_event(pool, plans, sink, &stream, rows_event, &event_id, metrics).await?;
                    }
                }

                events_since_checkpoint = events_since_checkpoint.saturating_add(1);
                events_since_metrics = events_since_metrics.saturating_add(1);
                if sink.pending_operations() >= config.meilisearch.batch_size
                    || events_since_checkpoint >= config.runtime.checkpoint_every_events
                {
                    checkpoint(sink, &config.runtime.state_path, &positions.safe, metrics).await?;
                    events_since_checkpoint = 0;
                    report_progress(metrics);
                    events_since_metrics = 0;
                }
            }
        }
    }
}

// A persisted cursor must never depend on an earlier table map or a partial transaction.
struct CheckpointPositions {
    current: BinlogPosition,
    safe: BinlogPosition,
    transaction_open: bool,
    compressed_transaction: bool,
}

impl CheckpointPositions {
    fn new(start: BinlogPosition) -> Self {
        Self {
            safe: start.clone(),
            current: start,
            transaction_open: false,
            compressed_transaction: false,
        }
    }

    fn observe(&mut self, data: &EventData<'_>) {
        match data {
            EventData::XidEvent(_) => self.commit(),
            EventData::QueryEvent(query) => {
                let query = query.query();
                let query = query.trim().trim_end_matches(';').trim();
                if query.eq_ignore_ascii_case("BEGIN") {
                    self.transaction_open = true;
                } else if query.eq_ignore_ascii_case("COMMIT")
                    || query.eq_ignore_ascii_case("ROLLBACK")
                    || !self.transaction_open
                {
                    self.commit();
                }
            }
            EventData::TableMapEvent(_)
            | EventData::RowsEvent(_)
            | EventData::GtidEvent(_)
            | EventData::AnonymousGtidEvent(_) => {
                self.transaction_open = true;
            }
            EventData::TransactionPayloadEvent(_) => {
                self.transaction_open = true;
                self.compressed_transaction = true;
            }
            EventData::RotateEvent(rotate) => {
                self.current.file = rotate.name().into_owned();
                self.current.pos = rotate.position();
                if !self.transaction_open {
                    self.safe.clone_from(&self.current);
                }
            }
            _ => {}
        }
    }

    fn commit(&mut self) {
        self.safe.clone_from(&self.current);
        self.transaction_open = false;
        self.compressed_transaction = false;
    }
}

fn report_progress(metrics: Option<&SyncMetrics>) {
    if let Some(metrics) = metrics {
        metrics.report_progress();
    }
}

async fn process_rows_event(
    pool: &Pool,
    plans: &[TablePlan],
    sink: &mut MeiliSink,
    stream: &mysql_async::BinlogStream,
    rows_event: RowsEventData<'_>,
    event_id: &str,
    metrics: Option<&SyncMetrics>,
) -> Result<()> {
    let table_id = rows_event.table_id();
    let table_map_event = stream.get_tme(table_id).with_context(|| {
        format!("table map missing for binlog table {table_id}; resume from a safe position")
    })?;
    let database = table_map_event.database_name();
    let table = table_map_event.table_name();
    let matching_plans =
        plans_for_table(plans, database.as_ref(), table.as_ref()).collect::<Vec<_>>();
    if matching_plans.is_empty() {
        debug!(
            database = %table_map_event.database_name(),
            table = %table_map_event.table_name(),
            "binlog event ignored for an unconfigured table"
        );
        return Ok(());
    }

    for row in rows_event_rows(&rows_event, table_map_event) {
        let (before, after) = row?;
        for plan in &matching_plans {
            let started_at = Instant::now();
            let operation = operation_from_rows(plan, before.as_ref(), after.as_ref())?;
            if let Some(metrics) = metrics {
                metrics.record_transformation(started_at.elapsed());
            }
            queue_row_operation(pool, plan, sink, operation, event_id, metrics).await?;
        }
    }

    Ok(())
}

async fn queue_row_operation(
    pool: &Pool,
    plan: &TablePlan,
    sink: &mut MeiliSink,
    operation: RowOperation,
    event_id: &str,
    metrics: Option<&SyncMetrics>,
) -> Result<()> {
    match operation {
        RowOperation::Ignored => Ok(()),
        RowOperation::Delete { document_id } => {
            queue_delete(
                sink,
                plan,
                document_id,
                "deletion in MySQL",
                false,
                None,
                event_id,
            )
            .await
        }
        RowOperation::Upsert {
            document,
            primary_key,
            needs_fetch,
            previous_id,
        } => {
            let document_id = mysql_value_to_document_id(&primary_key);
            if let Some(previous_id) = previous_id
                && previous_id != document_id
            {
                queue_delete(
                    sink,
                    plan,
                    previous_id,
                    "primary key changed",
                    false,
                    Some(&document_id),
                    event_id,
                )
                .await?;
            }

            let document = if needs_fetch {
                fetch_document_by_pk(pool, plan, &primary_key, metrics).await?
            } else {
                document
            };
            if let Some(document) = document {
                if !needs_fetch && let Some(metrics) = metrics {
                    metrics.record_document_read(&document);
                }
                queue_upsert(sink, plan, document, needs_fetch, event_id).await
            } else {
                queue_delete(
                    sink,
                    plan,
                    document_id,
                    "document missing or excluded by the where clause",
                    needs_fetch,
                    None,
                    event_id,
                )
                .await
            }
        }
    }
}

async fn queue_upsert(
    sink: &mut MeiliSink,
    plan: &TablePlan,
    document: serde_json::Value,
    reread_from_mysql: bool,
    event_id: &str,
) -> Result<()> {
    info!(
        sync_mode = "cdc",
        sync_run_id = %sink.sync_run_id(),
        event_id,
        operation = "upsert",
        source_database = %plan.key.database,
        source_table = %plan.key.table,
        index = %plan.config.index,
        primary_key = %plan.config.primary_key,
        reread_from_mysql,
        "document added to the synchronization batch"
    );
    sink.push_for_event(
        SyncOperation::Upsert {
            index_uid: plan.config.index.clone(),
            primary_key: plan.config.primary_key.clone(),
            document,
        },
        event_id.to_owned(),
    )
    .await
}

async fn queue_delete(
    sink: &mut MeiliSink,
    plan: &TablePlan,
    document_id: String,
    reason: &str,
    reread_from_mysql: bool,
    replacement_document_id: Option<&str>,
    event_id: &str,
) -> Result<()> {
    info!(
        sync_mode = "cdc",
        sync_run_id = %sink.sync_run_id(),
        event_id,
        operation = "delete",
        reason,
        source_database = %plan.key.database,
        source_table = %plan.key.table,
        index = %plan.config.index,
        primary_key = %plan.config.primary_key,
        reread_from_mysql,
        replaces_document = replacement_document_id.is_some(),
        "document marked for deletion in Meilisearch"
    );
    sink.push_for_event(
        SyncOperation::Delete {
            index_uid: plan.config.index.clone(),
            primary_key: plan.config.primary_key.clone(),
            document_id,
        },
        event_id.to_owned(),
    )
    .await
}

async fn checkpoint(
    sink: &mut MeiliSink,
    state_path: &std::path::Path,
    position: &BinlogPosition,
    metrics: Option<&SyncMetrics>,
) -> Result<()> {
    sink.flush_all().await?;
    sink.wait_all().await?;
    let started_at = Instant::now();
    let result = save_state(state_path, &State::new(position.clone()));
    if let Some(metrics) = metrics {
        metrics.record_checkpoint_write(started_at.elapsed());
    }
    result.with_context(|| {
        format!(
            "saving binlog checkpoint {}:{}",
            position.file, position.pos
        )
    })
}

fn update_position_from_header(
    position: &mut BinlogPosition,
    event: &mysql_async::binlog::events::Event,
) {
    let log_pos = event.header().log_pos();
    if log_pos != 0 {
        position.pos = u64::from(log_pos);
    }
}

#[cfg(test)]
mod tests {
    use mysql_async::binlog::{
        BinlogVersion,
        events::{Event, FormatDescriptionEvent, QueryEvent, RotateEvent, XidEvent},
    };

    use super::*;

    fn start() -> BinlogPosition {
        BinlogPosition {
            file: "mysql-bin.000001".into(),
            pos: 4,
        }
    }

    fn query(sql: &'static [u8]) -> EventData<'static> {
        EventData::QueryEvent(QueryEvent::new(&b""[..], &b"shop"[..]).with_query(sql))
    }

    #[test]
    fn table_map_is_never_a_restart_boundary() -> Result<()> {
        // A real TABLE_MAP_EVENT for shop.products(id INT), with checksums disabled.
        let payload = [
            42, 0, 0, 0, 0, 0, 0, 0, // table ID and flags
            4, b's', b'h', b'o', b'p', 0, 8, b'p', b'r', b'o', b'd', b'u', b'c', b't', b's', 0, 1,
            3, 0, 0, // column count, INT type, metadata length, null bitmap
        ];
        let mut bytes = vec![0, 0, 0, 0, 19, 1, 0, 0, 0];
        bytes.extend_from_slice(&u32::try_from(19 + payload.len())?.to_le_bytes());
        bytes.extend_from_slice(&100_u32.to_le_bytes());
        bytes.extend_from_slice(&[0, 0]);
        bytes.extend_from_slice(&payload);
        let event = Event::read(
            &FormatDescriptionEvent::new(BinlogVersion::Version4),
            &bytes[..],
        )?;
        let data = event
            .read_data()?
            .context("table map fixture not decoded")?;
        assert!(matches!(data, EventData::TableMapEvent(_)));
        let mut positions = CheckpointPositions::new(start());
        update_position_from_header(&mut positions.current, &event);
        positions.observe(&data);
        assert_eq!(positions.current.pos, 100);
        assert_eq!(positions.safe, start());
        positions.current.pos = 200;
        positions.observe(&EventData::XidEvent(XidEvent { xid: 1 }));
        assert_eq!(positions.safe.pos, 200);
        Ok(())
    }

    #[test]
    fn restart_cursor_stays_before_an_unfinished_transaction() -> Result<()> {
        let mut positions = CheckpointPositions::new(start());
        positions.current.pos = 50;
        positions.observe(&query(b"BEGIN"));
        positions.current.pos = 100;
        positions.observe(&EventData::HeartbeatEvent);
        assert_eq!(positions.safe, start());
        // This is the same cursor persisted on a timer flush or a clean stop.
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("state.json");
        save_state(&path, &State::new(positions.safe.clone()))?;
        assert_eq!(
            crate::state::load(&path)?
                .context("checkpoint missing")?
                .binlog,
            start()
        );
        positions.current.pos = 120;
        positions.observe(&EventData::XidEvent(XidEvent { xid: 1 }));
        assert_eq!(positions.safe.pos, 120);
        positions.current.pos = 150;
        positions.observe(&query(b"BEGIN"));
        positions.current.pos = 200;
        assert_eq!(positions.safe.pos, 120);
        Ok(())
    }

    #[test]
    fn commit_queries_advance_cursor_but_savepoints_do_not() {
        for commit in [&b"COMMIT"[..], &b" rollback; "[..]] {
            let mut positions = CheckpointPositions::new(start());
            positions.observe(&query(b"BEGIN"));
            positions.current.pos = 50;
            positions.observe(&query(b"SAVEPOINT s"));
            assert_eq!(positions.safe, start());
            positions.current.pos = 100;
            positions.observe(&query(commit));
            assert_eq!(positions.safe.pos, 100);
        }
    }

    #[test]
    fn rotation_does_not_checkpoint_a_partial_transaction() {
        let rotation = EventData::RotateEvent(RotateEvent::new(4, &b"mysql-bin.000002"[..]));
        let mut idle = CheckpointPositions::new(start());
        idle.observe(&rotation);
        assert_eq!(idle.safe.file, "mysql-bin.000002");
        let mut active = CheckpointPositions::new(start());
        active.observe(&query(b"BEGIN"));
        active.observe(&rotation);
        assert_eq!(active.current.file, "mysql-bin.000002");
        assert_eq!(active.safe, start());
        active.current.pos = 100;
        active.observe(&EventData::XidEvent(XidEvent { xid: 1 }));
        assert_eq!(active.safe, active.current);
    }
}
