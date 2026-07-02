use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use meili_mysql_sync::{
    Config, cdc,
    meili::{IndexPreparation, MeiliSink},
    mysql::{create_pool, current_binlog_position, load_table_plans, run_snapshot},
    state::{BinlogPosition, State, absolutize, load as load_state, save as save_state},
};
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Synchronise des tables MySQL vers Meilisearch via snapshot + binlog"
)]
struct Cli {
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
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
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let config = Config::from_path(&cli.config)?;
    let state_path = absolutize(&config.runtime.state_path)?;
    let pool = create_pool(&config)?;
    let plans = load_table_plans(&config, &pool).await?;

    match cli.command.unwrap_or(Command::Run {
        force_snapshot: false,
        recreate_indexes: false,
        clear_documents: false,
    }) {
        Command::Run {
            force_snapshot,
            recreate_indexes,
            clear_documents,
        } => {
            let mut sink = MeiliSink::new(&config.meilisearch)?;
            let state = load_state(&state_path)?;
            let start = if state.is_none() || force_snapshot {
                let position = current_binlog_position(&pool).await?;
                if config.runtime.snapshot_on_start || force_snapshot {
                    let index_preparation = IndexPreparation {
                        recreate: recreate_indexes,
                        clear_documents,
                    };
                    run_snapshot(&pool, &plans, &mut sink, index_preparation).await?;
                }
                save_state(&state_path, &State::new(position.clone()))?;
                position
            } else {
                state.context("etat absent")?.binlog
            };
            cdc::run(&config, &pool, &plans, &mut sink, start).await?;
        }
        Command::Snapshot {
            recreate_indexes,
            clear_documents,
        } => {
            let mut sink = MeiliSink::new(&config.meilisearch)?;
            let index_preparation = IndexPreparation {
                recreate: recreate_indexes,
                clear_documents,
            };
            run_snapshot(&pool, &plans, &mut sink, index_preparation).await?;
        }
        Command::Cdc { file, pos } => {
            let start = match (file, pos) {
                (Some(file), Some(pos)) => BinlogPosition { file, pos },
                (None, None) => match load_state(&state_path)? {
                    Some(state) => state.binlog,
                    None => current_binlog_position(&pool).await?,
                },
                _ => anyhow::bail!("--file et --pos doivent etre fournis ensemble"),
            };
            let mut sink = MeiliSink::new(&config.meilisearch)?;
            cdc::run(&config, &pool, &plans, &mut sink, start).await?;
        }
        Command::Position => {
            let position = current_binlog_position(&pool).await?;
            println!("{}:{}", position.file, position.pos);
        }
    }

    pool.disconnect().await.context("fermeture du pool MySQL")?;
    info!("termine");
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}
