# Operations

The following examples use Cargo. For a built binary, replace
`cargo run --release --` with `meili-mysql-sync`. The Docker image already
sets the configuration path; append the command after the image name.

## Commands

```bash
cargo run --release -- --help
cargo run --release -- --config config.toml run
cargo run --release -- position
cargo run --release -- snapshot
cargo run --release -- cdc --file mysql-bin.000001 --pos 4
```

- `run` performs a snapshot when needed and then starts CDC. It is the default command.
- `position` prints the current MySQL binlog position as `file:position`.
- `snapshot` loads the configured tables and exits.
- `cdc` starts streaming from the saved checkpoint, or from the current MySQL
  position if no checkpoint exists. It does not take a snapshot. Supply
  `--file` and `--pos` together to use an explicit position.

## Rebuild indexes

```bash
cargo run --release -- snapshot --recreate-indexes
cargo run --release -- snapshot --clear-documents
cargo run --release -- run --force-snapshot --clear-documents
```

`--recreate-indexes` deletes and recreates the configured indexes before
loading documents. `--clear-documents` deletes existing documents before
reloading them without recreating the index. Snapshot preparation still
applies settings declared in the configuration.

With `run`, use `--force-snapshot` to rebuild when a checkpoint already exists.
These commands affect every configured destination index.

## Recovery

If synchronization stops because the binlog table map is missing, resume
from a known safe position that includes the required table map, or rebuild:

```bash
cargo run --release -- run --force-snapshot --clear-documents
```

Preserve the state file during routine restarts and upgrades. Use `Ctrl+C`
for a clean local shutdown; the service flushes pending CDC operations and
saves the last safe transaction position.

## Logs

Set `RUST_LOG` to adjust verbosity, for example `info`, `warn`, or
`meili_mysql_sync=debug`. Service messages and CLI help are in English.
Meilisearch errors expose their codes and technical metadata; server error
messages and sources that may contain document data are discarded.

## Performance metrics

Add `--metrics` to `run`, `snapshot`, or `cdc`. The option is disabled by
default and emits two structured `INFO` records: volume/resource statistics
and a breakdown of time spent at each stage.

```bash
cargo run --release -- snapshot --metrics
cargo run --release -- run --metrics
cargo run --release -- cdc --metrics --file mysql-bin.000001 --pos 4
```

`snapshot` reports at completion. `run` and `cdc` also emit cumulative
progress after flush intervals that received CDC events, and when batch or
checkpoint thresholds trigger a flush. A final report is emitted on a clean
shutdown, such as `Ctrl+C`, or when synchronization returns an error.

Example output, with illustrative values:

```text
INFO synchronization finished sync_run_id="..." sync_mode="full" duration_ms=48231 documents_read=125000 documents_indexed=124998 documents_failed=2 documents_per_second=2591.7 bytes_read=184293891 bytes_sent=91293821 average_batch_size=487.0 peak_memory_mb=184 average_cpu_percent=72.4 rss_bytes=192937984 virtual_memory_bytes=823132160 peak_rss_bytes=192937984
INFO synchronization timing breakdown finished sync_run_id="..." sync_duration_ms=48231 mysql_read_ms=12843 transformation_ms=4382 serialization_ms=2144 batch_wait_ms=318 meilisearch_http_ms=11622 meilisearch_task_wait_ms=16417 checkpoint_write_ms=42
```

`bytes_read` measures normalized document JSON read from MySQL;
`bytes_sent` measures JSON batches sent to Meilisearch. `rss_bytes`,
`virtual_memory_bytes`, and `peak_rss_bytes` describe the synchronization
process, sampled every 500 ms. Indexed and failed document counters are
updated when Meilisearch tasks succeed or fail. Metrics do not include
document values or identifiers.
