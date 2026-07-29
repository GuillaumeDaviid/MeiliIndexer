use std::{
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
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
                    "cle primaire '{}' absente du resultat",
                    self.config.primary_key
                )
            })
    }

    fn row_to_document(&self, row: &Row) -> Result<JsonValue> {
        let mut document = JsonMap::with_capacity(self.config.fields.len());
        for (index, field) in self.config.fields.iter().enumerate() {
            let value = row
                .as_ref(index)
                .with_context(|| format!("champ '{field}' absent du resultat"))?;
            document.insert(
                self.config.target_field(field).to_owned(),
                mysql_value_to_json(value),
            );
        }
        Ok(JsonValue::Object(document))
    }
}

pub fn create_pool(config: &Config) -> Result<Pool> {
    let opts = Opts::from_url(&config.mysql.url).context("parsing de mysql.url")?;
    Ok(Pool::new(opts))
}

pub async fn load_table_plans(config: &Config, pool: &Pool) -> Result<Vec<TablePlan>> {
    let opts = Opts::from_url(&config.mysql.url).context("parsing de mysql.url")?;
    let default_database = opts.db_name().map(ToOwned::to_owned);
    let mut conn = pool
        .get_conn()
        .await
        .context("connexion MySQL pour charger les schemas")?;
    let mut plans = Vec::with_capacity(config.tables.len());

    for table in &config.tables {
        let Some(database) = table.database.clone().or_else(|| default_database.clone()) else {
            bail!(
                "aucune base MySQL par defaut dans l'URL; renseigner database pour {}",
                table.table
            );
        };
        let schema = load_table_schema(&mut conn, &database, &table.table).await?;
        validate_table_fields(table, &schema)?;
        let primary_key_select_index = table
            .fields
            .iter()
            .position(|field| field == &table.primary_key)
            .with_context(|| format!("cle primaire absente des champs de {}", table.table))?;
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
        .with_context(|| format!("lecture du schema de {database}.{table}"))?;
    if rows.is_empty() {
        bail!("table introuvable dans information_schema: {database}.{table}");
    }
    Ok(TableSchema { columns: rows })
}

fn validate_table_fields(table: &TableConfig, schema: &TableSchema) -> Result<()> {
    for field in &table.fields {
        if !schema.columns.iter().any(|column| column == field) {
            bail!(
                "champ '{}' introuvable dans la table {}",
                field,
                table.table
            );
        }
    }
    for field in &table.watch_fields {
        if !schema.columns.iter().any(|column| column == field) {
            bail!(
                "watch_field '{}' introuvable dans la table {}",
                field,
                table.table
            );
        }
    }
    if !schema
        .columns
        .iter()
        .any(|column| column == &table.primary_key)
    {
        bail!(
            "cle primaire '{}' introuvable dans la table {}",
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
        .context("connexion MySQL pour lire la position binlog")?;

    match read_binlog_position_with(&mut conn, "SHOW BINARY LOG STATUS").await {
        Ok(position) => Ok(position),
        Err(first_error) => read_binlog_position_with(&mut conn, "SHOW MASTER STATUS")
            .await
            .with_context(|| {
                format!(
                    "SHOW BINARY LOG STATUS a echoue avant le fallback SHOW MASTER STATUS: {first_error}"
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
        .with_context(|| format!("{statement} n'a retourne aucune ligne"))?;
    let file = row
        .get::<String, _>(0)
        .with_context(|| format!("{statement}: colonne File illisible"))?;
    let pos = row
        .get::<u64, _>(1)
        .with_context(|| format!("{statement}: colonne Position illisible"))?;
    Ok(BinlogPosition { file, pos })
}

pub async fn run_snapshot(
    pool: &Pool,
    plans: &[TablePlan],
    sink: &mut MeiliSink,
    index_preparation: IndexPreparation,
) -> Result<()> {
    for plan in plans {
        sink.prepare_index(&plan.config, index_preparation).await?;
        snapshot_table(pool, plan, sink).await?;
    }
    sink.flush_all().await?;
    sink.wait_all().await
}

async fn snapshot_table(pool: &Pool, plan: &TablePlan, sink: &mut MeiliSink) -> Result<()> {
    info!(
        table = %plan.key.table,
        database = %plan.key.database,
        index = %plan.config.index,
        "demarrage du snapshot"
    );

    let mut conn = pool
        .get_conn()
        .await
        .with_context(|| format!("connexion MySQL pour snapshot {}", plan.key.table))?;
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
        let mut result = conn
            .exec_iter(query, params)
            .await
            .with_context(|| format!("requete snapshot {}", plan.key.table))?;

        let mut rows_in_batch = 0_usize;
        while let Some(row) = result.next().await? {
            let pk = plan.row_primary_key(&row)?;
            let document_id = mysql_value_to_document_id(&pk);
            let document = plan.row_to_document(&row)?;
            info!(
                sync_mode = "snapshot",
                source_database = %plan.key.database,
                source_table = %plan.key.table,
                index = %plan.config.index,
                primary_key = %plan.config.primary_key,
                document_id = %document_id,
                document = %document,
                "document ajoute au lot de synchronisation"
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
        "snapshot termine"
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
) -> Result<Option<JsonValue>> {
    let mut conn = pool
        .get_conn()
        .await
        .with_context(|| format!("connexion MySQL pour relire {}", plan.key.table))?;
    let row = conn
        .exec_first::<Row, _, _>(
            plan.fetch_query(),
            Params::Positional(vec![primary_key.clone()]),
        )
        .await
        .with_context(|| format!("relecture de {}", plan.key.table))?;
    row.as_ref()
        .map(|row| plan.row_to_document(row))
        .transpose()
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
                primary_key: after.primary_key(&plan.config.primary_key)?,
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

#[must_use]
pub fn find_plan_for_table<'a>(
    plans: &'a [TablePlan],
    table_map_event: &TableMapEvent<'_>,
) -> Option<&'a TablePlan> {
    let database = table_map_event.database_name();
    let table = table_map_event.table_name();
    plans
        .iter()
        .find(|plan| plan.matches(database.as_ref(), table.as_ref()))
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
            .with_context(|| format!("cle primaire '{primary_key}' absente de l'evenement binlog"))
    }

    fn document_id(&self, primary_key: &str) -> Result<String> {
        self.value(primary_key)
            .map(mysql_value_to_document_id)
            .with_context(|| format!("cle primaire '{primary_key}' absente de l'evenement binlog"))
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
                .with_context(|| format!("conversion de la colonne binlog '{column_name}'"))?,
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
            .with_context(|| format!("nom de colonne binlog invalide: {raw_name}"))?;
        return schema
            .columns
            .get(index)
            .cloned()
            .with_context(|| format!("index de colonne binlog hors limites: {raw_name}"));
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
