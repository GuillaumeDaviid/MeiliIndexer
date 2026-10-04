# Configuration

Start from [config.example.toml](../config.example.toml). The service reads
`config.toml` by default; use `--config <path>` before the command to select
another file. At least one `[[tables]]` mapping is required.

## MySQL prerequisites

Enable binary logging with row-based events. Check the server settings:

```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';
```

Expected values:

```text
log_bin = ON
binlog_format = ROW
binlog_row_image = FULL
```

`binlog_row_image=MINIMAL` is supported. If an update does not include every
document field, the service rereads the row from MySQL. Add columns used by
`where_clause` to `watch_fields` if they are absent from `fields`, so updates
to those columns trigger a reread and, when necessary, a deletion in Meilisearch.

Grant read access only to the databases being synchronized, plus replication
access. Replace the database, user, and host with your own values:

```sql
GRANT SELECT ON football_data.* TO 'sync_user'@'%';
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'sync_user'@'%';
```

## Secrets and transport security

Environment variables override the corresponding TOML values:

| Variable | Purpose |
| --- | --- |
| `MEILI_SYNC_MYSQL_URL_FILE` | File containing the full MySQL URL, including its password. Preferred in production. |
| `MEILI_SYNC_MEILISEARCH_API_KEY_FILE` | File containing a Meilisearch API key restricted to the required indexes and actions. Preferred in production. |
| `MEILI_SYNC_MYSQL_URL` | Direct override for `mysql.url`, useful in development. |
| `MEILI_SYNC_MEILISEARCH_API_KEY` | Direct override for `meilisearch.api_key`, useful in development. |
| `RUST_LOG` | Log filter, defaulting to `info`. Examples: `debug`, `warn`, `meili_mysql_sync=debug`. |

Each secret file contains only its corresponding value. Trailing CR/LF
characters are removed. Do not set both a direct variable and its `_FILE`
variant. Empty secrets are rejected.

For example, in Bash:

```bash
export MEILI_SYNC_MYSQL_URL_FILE=/run/secrets/mysql-url
export MEILI_SYNC_MEILISEARCH_API_KEY_FILE=/run/secrets/meilisearch-api-key
```

In PowerShell:

```powershell
$env:MEILI_SYNC_MYSQL_URL_FILE = "C:\secrets\mysql-url"
$env:MEILI_SYNC_MEILISEARCH_API_KEY_FILE = "C:\secrets\meilisearch-api-key"
```

Outside `localhost`, `127.0.0.0/8`, and `::1`, the MySQL URL must enable TLS
with `require_ssl=true`, and Meilisearch must use `https://`. Each service
section supports `allow_insecure = true` for an explicit network exception.

## Service settings

Defaults below apply when a field is omitted. The example configuration uses
larger batches and different task settings.

| TOML path | Default | Purpose |
| --- | --- | --- |
| `mysql.url` | Required | MySQL connection URL; the URL database is the default for table mappings. |
| `mysql.server_id` | Required | Unique, nonzero replication identifier for this CDC client. |
| `mysql.allow_insecure` | `false` | Allow MySQL without TLS on a non-loopback host. |
| `meilisearch.host` | Required | Meilisearch HTTP or HTTPS endpoint. |
| `meilisearch.api_key` | Unset | API key, preferably supplied through a secret file. |
| `meilisearch.allow_insecure` | `false` | Allow HTTP on a non-loopback host. |
| `meilisearch.batch_size` | `1000` | Maximum operations per batch; must be greater than zero. |
| `meilisearch.max_in_flight_tasks` | `8` | Limit on pending synchronization tasks; must be greater than zero. |
| `meilisearch.task_poll_ms` | `100` | Meilisearch task polling interval in milliseconds. |
| `meilisearch.task_timeout_secs` | `300` | Meilisearch task wait timeout in seconds. |
| `runtime.state_path` | `meili-sync-state.json` | Persistent binlog checkpoint, relative to the working directory unless absolute. |
| `runtime.snapshot_on_start` | `true` | Take a snapshot when `run` has no checkpoint. |
| `runtime.flush_interval_ms` | `1000` | CDC flush timer interval in milliseconds. Use a positive value. |
| `runtime.checkpoint_every_events` | `1000` | Event count that triggers a flush and checkpoint. |

## Table mappings

Each `[[tables]]` entry maps one MySQL table to one Meilisearch index.
The same source table can feed several indexes; duplicate database/table/index
mappings are rejected.

| Field | Purpose |
| --- | --- |
| `database` | Source database; defaults to the database in `mysql.url`. |
| `table` | Required source table name. |
| `index` | Required destination index name. |
| `primary_key` | Required source primary key; must be included in `fields`. |
| `fields` | Nonempty list of source columns to include in each document. |
| `watch_fields` | Additional source columns whose updates trigger a reread. Defaults to an empty list. |
| `field_aliases` | Map of source column names to document field names. |
| `where_clause` | Optional SQL filter used for snapshots and row rereads. |
| `snapshot_batch_size` | Rows per snapshot page; defaults to `10000` and must be greater than zero. |
| `displayed_attributes` | Meilisearch displayed fields; defaults to all document fields. |
| `searchable_attributes` | Meilisearch fields used for text search. |
| `filterable_attributes` | Meilisearch fields available for filtering. |
| `sortable_attributes` | Meilisearch fields available for sorting. |
| `ranking_rules` | Meilisearch ranking rule order. |
| `distinct_attribute` | Optional Meilisearch distinct field. |

Attribute names refer to document fields after aliasing. Attribute validation
also accepts `*`. Source fields, watch fields, and the primary key must exist
in the MySQL schema.

Declare `searchable_attributes`, `filterable_attributes`, and
`sortable_attributes` explicitly when possible. Meilisearch searches all text
fields by default; limiting searchable fields reduces indexing work.
