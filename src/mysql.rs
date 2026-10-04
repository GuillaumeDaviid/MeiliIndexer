use std::{
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
    time::Instant,
};

use anyhow::{Context, Result, bail};
use mysql_async::{
    Opts, Params, Pool, Row, Value as MySqlValue,
    binlog::{
        events::{RowsEventData, TableMapEvent},
        row::BinlogRow,
        value::BinlogValue,
    },
    prelude::Queryable,
};
use serde_json::{Map as JsonMap, Value as JsonValue};
use tracing::info;

use crate::{
    config::{Config, TableConfig},
    meili::{IndexPreparation, MeiliSink},
    metrics::SyncMetrics,
    state::BinlogPosition,
    value::{mysql_value_to_document_id, mysql_value_to_json},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableKey {
    pub database: String,
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TablePlan {
    pub key: TableKey,
    pub config: TableConfig,
    schema: TableSchema,
    primary_key_select_index: usize,
}

impl TablePlan {
    #[must_use]
    pub fn matches(&self, database: &str, table: &str) -> bool {
        self.key.database == database && self.key.table == table
    }

    fn source_table(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.key.database),
            quote_identifier(&self.key.table)
        )
    }

    fn select_columns(&self) -> String {
        self.config
            .fields
            .iter()
            .map(|field| quote_identifier(field))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn snapshot_query(&self, has_last_pk: bool) -> String {
        let mut filters = Vec::new();
        if has_last_pk {
            filters.push(format!(
                "{} > ?",
                quote_identifier(&self.config.primary_key)
            ));
        }
        if let Some(where_clause) = &self.config.where_clause {
            filters.push(format!("({where_clause})"));
        }
        let where_sql = if filters.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", filters.join(" AND "))
        };
        format!(
            "SELECT {} FROM {}{} ORDER BY {} ASC LIMIT {}",
            self.select_columns(),
            self.source_table(),
            where_sql,
            quote_identifier(&self.config.primary_key),
            self.config.snapshot_batch_size
        )
    }

    fn fetch_query(&self) -> String {
        let mut filters = vec![format!(
            "{} = ?",
            quote_identifier(&self.config.primary_key)
        )];
        if let Some(where_clause) = &self.config.where_clause {
            filters.push(format!("({where_clause})"));
        }
        format!(
            "SELECT {} FROM {} WHERE {} LIMIT 1",
            self.select_columns(),
            self.source_table(),
            filters.join(" AND ")
        )
    }

    fn row_primary_key(&self, row: &Row) -> Result<MySqlValue> {
        row.as_ref(self.primary_key_select_index)
            .cloned()
            .with_context(|| {
                format!(
                    "primary key '{}' missing from the result",
                    self.config.primary_key
                )
            })
    }

    fn row_to_document(&self, row: &Row) -> Result<JsonValue> {
        let mut document = JsonMap::with_capacity(self.config.fields.len());
        for (index, field) in self.config.fields.iter().enumerate() {
            let value = row
                .as_ref(index)
                .with_context(|| format!("field '{field}' missing from the result"))?;
            document.insert(
                self.config.target_field(field).to_owned(),
                mysql_value_to_json(value),
            );
        }
        Ok(JsonValue::Object(document))
    }
}

pub fn create_pool(config: &Config) -> Result<Pool> {
    let opts = Opts::from_url(&config.mysql.url).context("parsing mysql.url")?;
    Ok(Pool::new(opts))
}

pub async fn load_table_plans(config: &Config, pool: &Pool) -> Result<Vec<TablePlan>> {
    let opts = Opts::from_url(&config.mysql.url).context("parsing mysql.url")?;
    let default_database = opts.db_name().map(ToOwned::to_owned);
    let mut conn = pool
        .get_conn()
        .await
        .context("connecting to MySQL to load schemas")?;
    let mut plans = Vec::with_capacity(config.tables.len());

    for table in &config.tables {
        let Some(database) = table.database.clone().or_else(|| default_database.clone()) else {
            bail!(
                "no default MySQL database in the URL; set database for {}",
                table.table
            );
        };
        let schema = load_table_schema(&mut conn, &database, &table.table).await?;
        validate_table_fields(table, &schema)?;
        let primary_key_select_index = table
            .fields
            .iter()
            .position(|field| field == &table.primary_key)
            .with_context(|| format!("primary key missing from fields of {}", table.table))?;
        plans.push(TablePlan {
            key: TableKey {
                database,
                table: table.table.clone(),
            },
            config: table.clone(),
            schema,
            primary_key_select_index,
        });
    }

    Ok(plans)
}

async fn load_table_schema(
    conn: &mut mysql_async::Conn,
    database: &str,
    table: &str,
) -> Result<TableSchema> {
    let rows = conn
        .exec::<String, _, _>(
            "SELECT COLUMN_NAME \
             FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
            (database, table),
        )
        .await
        .with_context(|| format!("reading the schema of {database}.{table}"))?;
    if rows.is_empty() {
        bail!("table not found in information_schema: {database}.{table}");
    }
    Ok(TableSchema { columns: rows })
}

fn validate_table_fields(table: &TableConfig, schema: &TableSchema) -> Result<()> {
    for field in &table.fields {
        if !schema.columns.iter().any(|column| column == field) {
            bail!("field '{}' not found in table {}", field, table.table);
        }
    }
    for field in &table.watch_fields {
        if !schema.columns.iter().any(|column| column == field) {
            bail!("watch_field '{}' not found in table {}", field, table.table);
        }
    }
    if !schema
        .columns
        .iter()
        .any(|column| column == &table.primary_key)
    {
        bail!(
            "primary key '{}' not found in table {}",
            table.primary_key,
            table.table
        );
    }
    Ok(())
}

pub async fn current_binlog_position(pool: &Pool) -> Result<BinlogPosition> {
    let mut conn = pool
        .get_conn()
        .await
        .context("connecting to MySQL to read the binlog position")?;

    match read_binlog_position_with(&mut conn, "SHOW BINARY LOG STATUS").await {
        Ok(position) => Ok(position),
        Err(first_error) => read_binlog_position_with(&mut conn, "SHOW MASTER STATUS")
            .await
            .with_context(|| {
                format!(
                    "SHOW BINARY LOG STATUS failed before falling back to SHOW MASTER STATUS: {first_error}"
                )
            }),
    }
}

async fn read_binlog_position_with(
    conn: &mut mysql_async::Conn,
    statement: &str,
) -> Result<BinlogPosition> {
    let row: Row = conn
        .query_first(statement)
        .await?
        .with_context(|| format!("{statement} returned no rows"))?;
    let file = row
        .get::<String, _>(0)
        .with_context(|| format!("{statement}: cannot read the File column"))?;
    let pos = row
        .get::<u64, _>(1)
        .with_context(|| format!("{statement}: cannot read the Position column"))?;
    Ok(BinlogPosition { file, pos })
}

pub async fn run_snapshot(
    pool: &Pool,
    plans: &[TablePlan],
    sink: &mut MeiliSink,
    index_preparation: IndexPreparation,
    metrics: Option<&SyncMetrics>,
) -> Result<()> {
    for plan in plans {
        sink.prepare_index(&plan.config, index_preparation).await?;
        snapshot_table(pool, plan, sink, metrics).await?;
    }
    sink.flush_all().await?;
    sink.wait_all().await
}

async fn snapshot_table(
    pool: &Pool,
    plan: &TablePlan,
    sink: &mut MeiliSink,
    metrics: Option<&SyncMetrics>,
) -> Result<()> {
    info!(
        table = %plan.key.table,
        database = %plan.key.database,
        index = %plan.config.index,
        "snapshot started"
    );

    let mut conn = pool
        .get_conn()
        .await
        .with_context(|| format!("connecting to MySQL for snapshot of {}", plan.key.table))?;
    let mut last_pk = None;
    let mut documents = Vec::with_capacity(
        sink.batch_size()
            .min(plan.config.snapshot_batch_size)
            .max(1),
    );
    let mut total_rows = 0_u64;
    let first_query = plan.snapshot_query(false);
    let next_query = plan.snapshot_query(true);

    loop {
        let query = if last_pk.is_some() {
            next_query.as_str()
        } else {
            first_query.as_str()
        };
        let params = last_pk
            .clone()
            .map_or(Params::Empty, |value| Params::Positional(vec![value]));
        let started_at = Instant::now();
        let result = conn.exec_iter(query, params).await;
        if let Some(metrics) = metrics {
            metrics.record_mysql_read(started_at.elapsed());
        }
        let mut result =
            result.with_context(|| format!("querying snapshot of {}", plan.key.table))?;

        let mut rows_in_batch = 0_usize;
        loop {
            let started_at = Instant::now();
            let next = result.next().await;
            if let Some(metrics) = metrics {
                metrics.record_mysql_read(started_at.elapsed());
            }
            let Some(row) = next? else {
                break;
            };
            let pk = plan.row_primary_key(&row)?;
            let started_at = Instant::now();
            let document = plan.row_to_document(&row)?;
            if let Some(metrics) = metrics {
                metrics.record_transformation(started_at.elapsed());
                metrics.record_document_read(&document);
            }
            info!(
                sync_mode = "snapshot",
                source_database = %plan.key.database,
                source_table = %plan.key.table,
                index = %plan.config.index,
                primary_key = %plan.config.primary_key,
                "document added to the synchronization batch"
            );
            documents.push(document);
            last_pk = Some(pk);
            rows_in_batch += 1;
            total_rows += 1;

            if documents.len() >= sink.batch_size() {
                submit_snapshot_documents(sink, plan, &mut documents).await?;
            }
        }
        result.drop_result().await?;

        if rows_in_batch < plan.config.snapshot_batch_size {
            break;
        }
    }
    submit_snapshot_documents(sink, plan, &mut documents).await?;

    info!(
        table = %plan.key.table,
        database = %plan.key.database,
        rows = total_rows,
        "snapshot completed"
    );
    Ok(())
}

async fn submit_snapshot_documents(
    sink: &mut MeiliSink,
    plan: &TablePlan,
    documents: &mut Vec<JsonValue>,
) -> Result<()> {
    let capacity = sink
        .batch_size()
        .min(plan.config.snapshot_batch_size)
        .max(1);
    let batch = std::mem::replace(documents, Vec::with_capacity(capacity));
    sink.push_documents(&plan.config.index, &plan.config.primary_key, batch)
        .await
}

pub async fn fetch_document_by_pk(
    pool: &Pool,
    plan: &TablePlan,
    primary_key: &MySqlValue,
    metrics: Option<&SyncMetrics>,
) -> Result<Option<JsonValue>> {
    let mut conn = pool
        .get_conn()
        .await
        .with_context(|| format!("connecting to MySQL to reread {}", plan.key.table))?;
    let started_at = Instant::now();
    let result = conn
        .exec_first::<Row, _, _>(
            plan.fetch_query(),
            Params::Positional(vec![primary_key.clone()]),
        )
        .await;
    if let Some(metrics) = metrics {
        metrics.record_mysql_read(started_at.elapsed());
    }
    let row = result.with_context(|| format!("rereading {}", plan.key.table))?;
    let started_at = Instant::now();
    let document = row
        .as_ref()
        .map(|row| plan.row_to_document(row))
        .transpose()?;
    if let Some(metrics) = metrics {
        metrics.record_transformation(started_at.elapsed());
        if let Some(document) = &document {
            metrics.record_document_read(document);
        }
    }
    Ok(document)
}

pub fn operation_from_rows(
    plan: &TablePlan,
    before: Option<&BinlogRow>,
    after: Option<&BinlogRow>,
) -> Result<RowOperation> {
    let before = before
        .map(|row| binlog_row_to_image(row, &plan.schema))
        .transpose()?;
    let after = after
        .map(|row| binlog_row_to_image(row, &plan.schema))
        .transpose()?;

    match (before, after) {
        (None, Some(after)) => Ok(RowOperation::Upsert {
            document: after.to_document(&plan.config),
            primary_key: after.primary_key(&plan.config.primary_key)?,
            needs_fetch: plan.config.where_clause.is_some()
                || !after.has_all_document_fields(&plan.config),
            previous_id: None,
        }),
        (Some(before), None) => Ok(RowOperation::Delete {
            document_id: before.document_id(&plan.config.primary_key)?,
        }),
        (Some(before), Some(after)) => {
            if !after.touches_interesting_fields(&before, &plan.config) {
                return Ok(RowOperation::Ignored);
            }
            let previous_id = before
                .value(&plan.config.primary_key)
                .map(mysql_value_to_document_id);
            let document = after.to_document(&plan.config);
            Ok(RowOperation::Upsert {
                document,
                primary_key: after
                    .value(&plan.config.primary_key)
                    .or_else(|| before.value(&plan.config.primary_key))
                    .cloned()
                    .context("primary key missing from both binlog update row images")?,
                needs_fetch: plan.config.where_clause.is_some()
                    || !after.has_all_document_fields(&plan.config),
                previous_id,
            })
        }
        (None, None) => Ok(RowOperation::Ignored),
    }
}

#[derive(Debug)]
pub enum RowOperation {
    Upsert {
        document: Option<JsonValue>,
        primary_key: MySqlValue,
        needs_fetch: bool,
        previous_id: Option<String>,
    },
    Delete {
        document_id: String,
    },
    Ignored,
}

pub fn plans_for_table<'a>(
    plans: &'a [TablePlan],
    database: &'a str,
    table: &'a str,
) -> impl Iterator<Item = &'a TablePlan> + 'a {
    plans
        .iter()
        .filter(move |plan| plan.matches(database, table))
}

pub fn rows_event_rows<'a>(
    rows_event: &'a RowsEventData<'a>,
    table_map_event: &'a TableMapEvent<'a>,
) -> impl Iterator<Item = Result<(Option<BinlogRow>, Option<BinlogRow>)>> + 'a {
    rows_event
        .rows(table_map_event)
        .map(|row| row.map_err(anyhow::Error::from))
}

#[derive(Debug, Clone)]
struct RowImage {
    values: BTreeMap<String, Option<MySqlValue>>,
}

impl RowImage {
    fn contains_field(&self, field: &str) -> bool {
        self.values.contains_key(field)
    }

    fn value(&self, field: &str) -> Option<&MySqlValue> {
        self.values.get(field).and_then(Option::as_ref)
    }

    fn primary_key(&self, primary_key: &str) -> Result<MySqlValue> {
        self.value(primary_key)
            .cloned()
            .with_context(|| format!("primary key '{primary_key}' missing from the binlog event"))
    }

    fn document_id(&self, primary_key: &str) -> Result<String> {
        self.value(primary_key)
            .map(mysql_value_to_document_id)
            .with_context(|| format!("primary key '{primary_key}' missing from the binlog event"))
    }

    fn has_all_document_fields(&self, table: &TableConfig) -> bool {
        table
            .fields
            .iter()
            .all(|field| matches!(self.values.get(field), Some(Some(_))))
    }

    fn to_document(&self, table: &TableConfig) -> Option<JsonValue> {
        if !self.has_all_document_fields(table) {
            return None;
        }
        let mut document = JsonMap::with_capacity(table.fields.len());
        for field in &table.fields {
            let value = self.value(field)?;
            document.insert(
                table.target_field(field).to_owned(),
                mysql_value_to_json(value),
            );
        }
        Some(JsonValue::Object(document))
    }

    fn touches_interesting_fields(&self, before: &Self, table: &TableConfig) -> bool {
        table.fields.iter().chain(&table.watch_fields).any(|field| {
            if !self.contains_field(field) {
                return false;
            }
            match (before.value(field), self.value(field)) {
                (Some(before), Some(after)) => before != after,
                _ => true,
            }
        })
    }
}

fn binlog_row_to_image(row: &BinlogRow, schema: &TableSchema) -> Result<RowImage> {
    let mut values = BTreeMap::new();
    for (index, column) in row.columns_ref().iter().enumerate() {
        let column_name = resolve_binlog_column_name(column.name_str().as_ref(), schema)?;
        let value = match row.as_ref(index) {
            Some(value) => binlog_value_to_mysql_value(value)
                .with_context(|| format!("converting binlog column '{column_name}'"))?,
            None => None,
        };
        values.insert(column_name, value);
    }
    Ok(RowImage { values })
}

fn binlog_value_to_mysql_value(value: &BinlogValue<'_>) -> Result<Option<MySqlValue>> {
    match value {
        BinlogValue::JsonDiff(_) => Ok(None),
        _ => MySqlValue::try_from(value.clone())
            .map(Some)
            .map_err(anyhow::Error::from),
    }
}

fn resolve_binlog_column_name(raw_name: &str, schema: &TableSchema) -> Result<String> {
    if let Some(raw_index) = raw_name.strip_prefix('@') {
        let index = raw_index
            .parse::<usize>()
            .with_context(|| format!("invalid binlog column name: {raw_name}"))?;
        return schema
            .columns
            .get(index)
            .cloned()
            .with_context(|| format!("binlog column index out of bounds: {raw_name}"));
    }
    Ok(raw_name.to_owned())
}

fn quote_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

#[must_use]
pub fn plans_by_table(plans: &[TablePlan]) -> HashMap<TableKey, TablePlan> {
    plans
        .iter()
        .map(|plan| (plan.key.clone(), plan.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::{Column, consts::ColumnType};

    fn test_plan(index: &str) -> TablePlan {
        TablePlan {
            key: TableKey {
                database: "shop".into(),
                table: "products".into(),
            },
            config: TableConfig {
                table: "products".into(),
                index: index.into(),
                primary_key: "id".into(),
                fields: vec!["id".into(), "name".into()],
                ..TableConfig::default()
            },
            schema: TableSchema {
                columns: vec!["id".into(), "name".into()],
            },
            primary_key_select_index: 0,
        }
    }

    fn binlog_row(values: &[(&str, MySqlValue)]) -> BinlogRow {
        let columns = values
            .iter()
            .map(|(name, _)| Column::new(ColumnType::MYSQL_TYPE_LONG).with_name(name.as_bytes()))
            .collect::<Vec<_>>();
        let values = values
            .iter()
            .map(|(_, value)| Some(BinlogValue::Value(value.clone())))
            .collect();
        BinlogRow::new(values, columns.into())
    }

    #[test]
    fn minimal_update_uses_primary_key_from_before_image() -> Result<()> {
        let plan = test_plan("products");
        let before = binlog_row(&[("@0", MySqlValue::Int(42))]);
        let after = binlog_row(&[("@1", MySqlValue::Bytes(b"updated".to_vec()))]);
        let operation = operation_from_rows(&plan, Some(&before), Some(&after))?;
        let RowOperation::Upsert {
            primary_key,
            needs_fetch,
            previous_id,
            ..
        } = operation
        else {
            panic!("a minimal update must produce an upsert");
        };
        assert_eq!(primary_key, MySqlValue::Int(42));
        assert!(needs_fetch);
        assert_eq!(previous_id.as_deref(), Some("42"));
        Ok(())
    }

    #[test]
    fn changed_primary_key_prefers_after_image() -> Result<()> {
        let plan = test_plan("products");
        let before = binlog_row(&[("id", MySqlValue::Int(42))]);
        let after = binlog_row(&[("id", MySqlValue::Int(43))]);
        let RowOperation::Upsert {
            primary_key,
            previous_id,
            ..
        } = operation_from_rows(&plan, Some(&before), Some(&after))?
        else {
            panic!("a primary key change must produce an upsert");
        };
        assert_eq!(primary_key, MySqlValue::Int(43));
        assert_eq!(previous_id.as_deref(), Some("42"));
        Ok(())
    }

    #[test]
    fn table_routing_includes_every_configured_index() {
        let mut unrelated = test_plan("unrelated");
        unrelated.key.database = "other_shop".into();
        let plans = [
            test_plan("products"),
            unrelated,
            test_plan("products_public"),
        ];
        let indexes = plans_for_table(&plans, "shop", "products")
            .map(|plan| plan.config.index.as_str())
            .collect::<Vec<_>>();
        assert_eq!(indexes, ["products", "products_public"]);
        assert_eq!(plans_for_table(&plans, "shop", "unknown").count(), 0);
    }

    #[test]
    fn resolves_default_binlog_column_name() {
        let schema = TableSchema {
            columns: vec!["id".to_owned(), "title".to_owned()],
        };
        assert_eq!(resolve_binlog_column_name("@1", &schema).unwrap(), "title");
    }

    #[test]
    fn update_filter_ignores_before_only_primary_key() {
        let table = TableConfig {
            table: "products".to_owned(),
            index: "products".to_owned(),
            primary_key: "id".to_owned(),
            fields: vec!["id".to_owned(), "name".to_owned()],
            ..TableConfig::default()
        };
        let before = RowImage {
            values: BTreeMap::from([("id".to_owned(), Some(MySqlValue::Int(1)))]),
        };
        let after = RowImage {
            values: BTreeMap::from([("stock".to_owned(), Some(MySqlValue::Int(4)))]),
        };
        assert!(!after.touches_interesting_fields(&before, &table));
    }

    #[test]
    fn update_filter_detects_document_field_change() {
        let table = TableConfig {
            table: "products".to_owned(),
            index: "products".to_owned(),
            primary_key: "id".to_owned(),
            fields: vec!["id".to_owned(), "name".to_owned()],
            ..TableConfig::default()
        };
        let before = RowImage {
            values: BTreeMap::from([
                ("id".to_owned(), Some(MySqlValue::Int(1))),
                ("name".to_owned(), Some(MySqlValue::Bytes(b"a".to_vec()))),
            ]),
        };
        let after = RowImage {
            values: BTreeMap::from([
                ("id".to_owned(), Some(MySqlValue::Int(1))),
                ("name".to_owned(), Some(MySqlValue::Bytes(b"b".to_vec()))),
            ]),
        };
        assert!(after.touches_interesting_fields(&before, &table));
    }

    #[test]
    fn update_filter_detects_watch_field_change() {
        let table = TableConfig {
            table: "products".to_owned(),
            index: "products".to_owned(),
            primary_key: "id".to_owned(),
            fields: vec!["id".to_owned(), "name".to_owned()],
            watch_fields: vec!["deleted_at".to_owned()],
            ..TableConfig::default()
        };
        let before = RowImage {
            values: BTreeMap::from([("id".to_owned(), Some(MySqlValue::Int(1)))]),
        };
        let after = RowImage {
            values: BTreeMap::from([("deleted_at".to_owned(), Some(MySqlValue::Int(1)))]),
        };
        assert!(after.touches_interesting_fields(&before, &table));
    }
}
