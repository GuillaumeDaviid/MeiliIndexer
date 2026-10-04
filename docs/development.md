# Development and benchmarks

Follow [AGENTS.md](../AGENTS.md) and the repository's Rust guidelines.
Before completing a Rust change, run:

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo test
```

## Local benchmarks (Windows / PowerShell 7)

If the local benchmark scripts are available, use them to measure snapshots
of configured tables, compare batch sizes on 100,000 synthetic documents,
and verify CDC mutations with `FULL` and `MINIMAL` row images:

```powershell
cargo build --release --locked
pwsh -NoProfile -File benchmarks/local-perf.ps1
```

The `benchmarks/` directory is currently ignored by Git, so these scripts
may be absent from a fresh checkout.

The script expects MySQL and Meilisearch on loopback and reads secrets from
the configuration or the standard environment overrides. Its default
container names are `football-mysql` and `meilisearch`; override them with
`-MysqlContainer` and `-MeiliContainer`.

The MySQL container must provide `MYSQL_ROOT_PASSWORD` to create and remove
the test table. The synchronization user must be able to read that table
and the binlog; the Meilisearch API key must allow operations on test indexes.

Real tables are only read. The benchmark table and indexes have unique names
and are removed at the end. Measurements and reports are saved under
`.docker-run/codex_perf_...`; generated configurations contain no secrets.
CDC synchronization processes stop after checkpoint verification. The
benchmark disables per-document logs to isolate synchronization cost.

Useful options include `-Rows 100000`, `-Repeats 2`, and
`-ConfigPath config.toml`. To measure CDC alone with the production flush interval:

```powershell
pwsh -NoProfile -File benchmarks/local-perf.ps1 -CdcOnly -CdcFlushMs 1000
```
