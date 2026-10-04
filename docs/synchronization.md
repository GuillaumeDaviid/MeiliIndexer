# How synchronization works

The service synchronizes MySQL tables to Meilisearch in two stages:

1. An initial snapshot reads rows in primary key order using keyset pagination,
   without loading the entire table into memory.
2. Change data capture (CDC) reads row-based MySQL binlog events and applies
   inserts, updates, and deletes to each configured destination index.

## Startup and snapshots

`run` resumes from the saved checkpoint when one exists. Otherwise it records
the current binlog position, takes a snapshot if `runtime.snapshot_on_start`
is enabled, saves the position, and starts CDC from that position.
`--force-snapshot` takes a new snapshot even when a checkpoint exists or
automatic snapshots are disabled.

Snapshots create missing indexes, apply configured Meilisearch settings, and
submit documents in batches. `snapshot` exits after loading documents; it
does not start CDC or save a new binlog checkpoint. See
[operations](operations.md) for full rebuild commands.

## CDC behavior

Each event is applied to every mapping for its source table. In
`binlog_row_image=MINIMAL` mode, updates use the primary key from the previous
row image when the key has not changed. Missing document columns are reread
from MySQL. Rows that no longer match `where_clause` are removed from the
destination index.

## Checkpoints and replay

The state file is replaced atomically after pending operations are flushed
and Meilisearch tasks are confirmed. Its cursor stays at the last complete
transaction. If shutdown occurs during a transaction, that transaction is
replayed on restart. Operations can therefore be applied more than once.

Keep the checkpoint on persistent storage. In Docker, a relative
`runtime.state_path` is resolved under the `/data` working directory.

If a checkpoint or explicit `--file`/`--pos` position starts without the
required table map, synchronization stops rather than skipping rows. See
[recovery instructions](operations.md#recovery) for a safe restart.
