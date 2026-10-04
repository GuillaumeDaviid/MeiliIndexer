use std::{path::PathBuf, time::Instant};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use meili_mysql_sync::{
    Config, cdc,
    meili::{IndexPreparation, MeiliSink},
    metrics::{SyncMetrics, SyncMode},
    mysql::{create_pool, current_binlog_position, load_table_plans, run_snapshot},
    state::{BinlogPosition, State, absolutize, load as load_state, save as save_state},
};
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Synchronize MySQL tables to Meilisearch using a snapshot and binlog"
)]
struct Cli {
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
    #[arg(
        long,
        global = true,
        help = "Report performance metrics during CDC and at completion"
    )]
    metrics: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run {
        #[arg(long)]
        force_snapshot: bool,
        #[arg(long)]
        recreate_indexes: bool,
        #[arg(long)]
        clear_documents: bool,
    },
    Snapshot {
        #[arg(long)]
        recreate_indexes: bool,
        #[arg(long)]
        clear_documents: bool,
    },
    Cdc {
        #[arg(long)]
        file: Option<String>,
        #[arg(long)]
        pos: Option<u64>,
    },
    Position,
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let config = Config::from_path(&cli.config)?;
    let state_path = absolutize(&config.runtime.state_path)?;
    let pool = create_pool(&config)?;
    let plans = load_table_plans(&config, &pool).await?;

    let command = cli.command.unwrap_or(Command::Run {
        force_snapshot: false,
        recreate_indexes: false,
        clear_documents: false,
    });
    let sync_mode = match command {
        Command::Run { .. } => SyncMode::Full,
        Command::Snapshot { .. } => SyncMode::Snapshot,
        Command::Cdc { .. } => SyncMode::Cdc,
        Command::Position => {
            let position = current_binlog_position(&pool).await?;
            println!("{}:{}", position.file, position.pos);
            pool.disconnect()
                .await
                .context("disconnecting the MySQL pool")?;
            return Ok(());
        }
    };
    let sync_run_id = Uuid::now_v7().to_string();
    let metrics = cli
        .metrics
        .then(|| SyncMetrics::start(sync_run_id.clone(), sync_mode));

    let result = async {
        match command {
            Command::Run {
                force_snapshot,
                recreate_indexes,
                clear_documents,
            } => {
                let mut sink =
                    MeiliSink::new(&config.meilisearch, sync_run_id.clone(), metrics.clone())?;
                let state = load_state(&state_path)?;
                let start = if state.is_none() || force_snapshot {
                    let position = current_binlog_position(&pool).await?;
                    if config.runtime.snapshot_on_start || force_snapshot {
                        let index_preparation = IndexPreparation {
                            recreate: recreate_indexes,
                            clear_documents,
                        };
                        run_snapshot(
                            &pool,
                            &plans,
                            &mut sink,
                            index_preparation,
                            metrics.as_ref(),
                        )
                        .await?;
                    }
                    let started_at = Instant::now();
                    let result = save_state(&state_path, &State::new(position.clone()));
                    if let Some(metrics) = &metrics {
                        metrics.record_checkpoint_write(started_at.elapsed());
                    }
                    result?;
                    position
                } else {
                    state.context("state missing")?.binlog
                };
                cdc::run(&config, &pool, &plans, &mut sink, start, metrics.as_ref()).await
            }
            Command::Snapshot {
                recreate_indexes,
                clear_documents,
            } => {
                let mut sink =
                    MeiliSink::new(&config.meilisearch, sync_run_id.clone(), metrics.clone())?;
                let index_preparation = IndexPreparation {
                    recreate: recreate_indexes,
                    clear_documents,
                };
                run_snapshot(
                    &pool,
                    &plans,
                    &mut sink,
                    index_preparation,
                    metrics.as_ref(),
                )
                .await
            }
            Command::Cdc { file, pos } => {
                let start = match (file, pos) {
                    (Some(file), Some(pos)) => BinlogPosition { file, pos },
                    (None, None) => match load_state(&state_path)? {
                        Some(state) => state.binlog,
                        None => current_binlog_position(&pool).await?,
                    },
                    _ => anyhow::bail!("--file and --pos must be provided together"),
                };
                let mut sink =
                    MeiliSink::new(&config.meilisearch, sync_run_id.clone(), metrics.clone())?;
                cdc::run(&config, &pool, &plans, &mut sink, start, metrics.as_ref()).await
            }
            Command::Position => {
                unreachable!("position is handled before metrics initialization")
            }
        }
    }
    .await;

    let disconnect_result = pool
        .disconnect()
        .await
        .context("disconnecting the MySQL pool");
    if let Some(metrics) = metrics {
        metrics.finish();
    }
    result?;
    disconnect_result?;
    info!("finished");
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}
