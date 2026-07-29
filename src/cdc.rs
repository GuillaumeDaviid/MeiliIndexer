use std::time::Duration;

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
    mysql::{
        RowOperation, TablePlan, fetch_document_by_pk, find_plan_for_table, operation_from_rows,
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
) -> Result<()> {
    let opts = Opts::from_url(&config.mysql.url).context("parsing de mysql.url")?;
    let conn = Conn::new(opts)
        .await
        .context("connexion MySQL dediee au binlog")?;
    let file_bytes = start.file.clone().into_bytes();
    let request = BinlogStreamRequest::new(config.mysql.server_id)
        .with_filename(&file_bytes)
        .with_pos(start.pos);
    let mut stream = conn
        .get_binlog_stream(request)
        .await
        .context("ouverture du flux binlog")?;

    let mut latest_position = start;
    let mut events_since_checkpoint = 0_u64;
    let mut flush_timer = interval(Duration::from_millis(config.runtime.flush_interval_ms));
    flush_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    info!(
        file = %latest_position.file,
        pos = latest_position.pos,
        "ecoute binlog demarree"
    );

    loop {
        tokio::select! {
            biased;
            signal = tokio::signal::ctrl_c() => {
                signal.context("attente du signal Ctrl+C")?;
                info!("arret demande, flush des operations CDC en attente");
                checkpoint(sink, &config.runtime.state_path, &latest_position).await?;
                stream.close().await.context("fermeture du flux binlog")?;
                return Ok(());
            }
            _ = flush_timer.tick() => {
                if sink.pending_operations() > 0 {
                    checkpoint(sink, &config.runtime.state_path, &latest_position).await?;
                }
            }
            event = stream.next() => {
                let Some(event) = event else {
                    warn!("flux binlog termine par le serveur");
                    checkpoint(sink, &config.runtime.state_path, &latest_position).await?;
                    return Ok(());
                };
                let event = event.context("lecture d'un evenement binlog")?;
                update_position_from_header(&mut latest_position, &event);
                let event_id = format!("{}:{}", latest_position.file, latest_position.pos);

                let data = event.read_data().context("decodage d'un evenement binlog")?;
                if let Some(data) = data {
                    if let EventData::RotateEvent(rotate) = &data {
                        latest_position.file = rotate.name().into_owned();
                        latest_position.pos = rotate.position();
                    }
                    if let EventData::RowsEvent(rows_event) = data {
                        process_rows_event(pool, plans, sink, &stream, rows_event, &event_id).await?;
                    }
                }

                events_since_checkpoint = events_since_checkpoint.saturating_add(1);
                if sink.pending_operations() >= config.meilisearch.batch_size
                    || events_since_checkpoint >= config.runtime.checkpoint_every_events
                {
                    checkpoint(sink, &config.runtime.state_path, &latest_position).await?;
                    events_since_checkpoint = 0;
                }
            }
        }
    }
}

async fn process_rows_event(
    pool: &Pool,
    plans: &[TablePlan],
    sink: &mut MeiliSink,
    stream: &mysql_async::BinlogStream,
    rows_event: RowsEventData<'_>,
    event_id: &str,
) -> Result<()> {
    let table_id = rows_event.table_id();
    let Some(table_map_event) = stream.get_tme(table_id) else {
        warn!(table_id, "evenement rows sans table map connue");
        return Ok(());
    };
    let Some(plan) = find_plan_for_table(plans, table_map_event) else {
        debug!(
            database = %table_map_event.database_name(),
            table = %table_map_event.table_name(),
            "evenement binlog ignore pour table non configuree"
        );
        return Ok(());
    };

    for row in rows_event_rows(&rows_event, table_map_event) {
        let (before, after) = row?;
        let operation = operation_from_rows(plan, before.as_ref(), after.as_ref())?;
        queue_row_operation(pool, plan, sink, operation, event_id).await?;
    }

    Ok(())
}

async fn queue_row_operation(
    pool: &Pool,
    plan: &TablePlan,
    sink: &mut MeiliSink,
    operation: RowOperation,
    event_id: &str,
) -> Result<()> {
    match operation {
        RowOperation::Ignored => Ok(()),
        RowOperation::Delete { document_id } => {
            queue_delete(
                sink,
                plan,
                document_id,
                "suppression dans MySQL",
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
                    "changement de cle primaire",
                    false,
                    Some(&document_id),
                    event_id,
                )
                .await?;
            }

            let document = if needs_fetch {
                fetch_document_by_pk(pool, plan, &primary_key).await?
            } else {
                document
            };
            if let Some(document) = document {
                queue_upsert(sink, plan, document_id, document, needs_fetch, event_id).await
            } else {
                queue_delete(
                    sink,
                    plan,
                    document_id,
                    "document absent ou exclu par la clause where",
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
    document_id: String,
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
        document_id = %document_id,
        reread_from_mysql,
        document = %document,
        "document ajoute au lot de synchronisation"
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
        document_id = %document_id,
        reread_from_mysql,
        replacement_document_id = ?replacement_document_id,
        "document marque pour suppression dans Meilisearch"
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
) -> Result<()> {
    sink.flush_all().await?;
    sink.wait_all().await?;
    save_state(state_path, &State::new(position.clone()))
        .with_context(|| format!("checkpoint binlog {}:{}", position.file, position.pos))
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
