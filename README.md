# meili-mysql-sync

## Prerequisites

- MySQL with `log_bin=ON`, `binlog_format=ROW`, and preferably `binlog_row_image=FULL`.
- A MySQL user with `SELECT` on the synchronized databases and `REPLICATION CLIENT` / `REPLICATION SLAVE` privileges.
- A reachable Meilisearch instance and an API key with access to the configured indexes and actions.
- Rust 1.88 or newer for a local build, or Docker for a container deployment.

See [configuration](docs/configuration.md) for MySQL setup and permissions.

## Configure

Copy the example configuration (PowerShell):

```powershell
Copy-Item config.example.toml config.toml
```

Or in Bash:

```bash
cp config.example.toml config.toml
```

Edit `config.toml`:

1. Set `mysql.url` and a unique, nonzero `mysql.server_id`.
2. Set `meilisearch.host` and provide the API key if required.
3. Adapt `[[tables]]` to your database, table, index, primary key, and fields.
4. Set `runtime.state_path` to a writable, persistent location.

For example, save the following as `config.toml` to synchronize `app.users`
to the Meilisearch index `users`. This example assumes the table has the
columns `id`, `name`, `email`, and `created_at`. Adjust the database, columns,
and connection settings to match your setup.

```toml
[mysql]
url = "mysql://sync_user@127.0.0.1:3306/app"
server_id = 7101

[meilisearch]
host = "http://127.0.0.1:7700"
# Provide the API key through MEILI_SYNC_MEILISEARCH_API_KEY_FILE if required.

[runtime]
state_path = "meili-sync-state.json"
snapshot_on_start = true
flush_interval_ms = 1000
checkpoint_every_events = 1000

[[tables]]
database = "app"
table = "users"
index = "users"
primary_key = "id"
fields = ["id", "name", "email", "created_at"]
displayed_attributes = ["id", "name", "email", "created_at"]
searchable_attributes = ["name", "email"]
filterable_attributes = ["id"]
sortable_attributes = ["name", "created_at"]
snapshot_batch_size = 10000
```

Grant the MySQL user `SELECT` on `app.*` and the replication privileges
listed above. If MySQL requires a password, supply the full connection URL
through `MEILI_SYNC_MYSQL_URL_FILE`.

For production, provide the full MySQL URL and Meilisearch API key through
`MEILI_SYNC_MYSQL_URL_FILE` and `MEILI_SYNC_MEILISEARCH_API_KEY_FILE`. Each
variable points to a file containing only its secret value. Do not also set
the corresponding direct environment variable. See
[secrets](docs/configuration.md#secrets-and-transport-security) for examples.

Non-loopback hosts require MySQL TLS (`require_ssl=true`) and Meilisearch
HTTPS. An explicit `allow_insecure = true` setting in each service section
allows a network exception.

## Run locally

```bash
cargo run --release -- run
```

To use a different configuration file:

```bash
cargo run --release -- --config /path/to/config.toml run
```

Keep the state file between runs. Use `Ctrl+C` for a clean shutdown.

## Run with Docker

Configure endpoints reachable from the container; `127.0.0.1` refers to the
container itself. For dependencies in other containers, use their service
names and add `--network <network-name>` to the command below. See
[Docker deployment](docs/deployment.md) for host networking and secret mounts.

Build and start from PowerShell:

```powershell
docker build -t meili-mysql-sync:local .
docker volume create meili-sync-data

docker run -d `
  --name meili-mysql-sync `
  --restart unless-stopped `
  --env RUST_LOG=info `
  --mount type=bind,source="${PWD}\config.toml",target=/config/config.toml,readonly `
  --mount type=volume,source=meili-sync-data,target=/data `
  meili-mysql-sync:local
```

The image runs `run` by default and stores relative state paths under `/data`.
If you use secret-file variables, also pass them and mount their files as
shown in the [production startup command](docs/deployment.md#start-the-container).

View logs and stop the service:

```powershell
docker logs -f meili-mysql-sync
docker stop meili-mysql-sync
```

See the [documentation](docs/README.md) for configuration reference,
production deployment, commands, recovery, metrics, synchronization behavior,
and benchmarks.
