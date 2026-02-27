#[cfg(feature = "arrow")]
mod arrow_mapping;
mod engine;

#[cfg(feature = "arrow")]
pub use arrow_mapping::arrow_to_data_type;
pub use engine::Engine;

use std::fmt::{self, Display, Write};

use clickhouse_types::DataTypeNode;
use clickhouse_types::data_types::Column;

/// A builder for ClickHouse `CREATE TABLE` DDL statements.
///
/// Inspired by SeaQuery's `TableCreateStatement` builder pattern.
///
/// # Examples
///
/// Basic MergeTree table:
///
/// ```
/// use clickhouse::schema::{ClickHouseSchema, SchemaColumn, Engine};
/// use clickhouse_types::DataTypeNode;
///
/// let ddl = ClickHouseSchema::new([
///     SchemaColumn::new("id", DataTypeNode::UInt64),
///     SchemaColumn::new("name", DataTypeNode::String),
///     SchemaColumn::new("created_at", DataTypeNode::DateTime(None)),
/// ])
/// .table_name("events")
/// .primary_key(["id"])
/// .to_string();
///
/// assert_eq!(ddl, r#"CREATE TABLE events (
///     id UInt64,
///     name String,
///     created_at DateTime
/// ) ENGINE = MergeTree()
/// PRIMARY KEY (id)"#);
/// ```
///
/// ReplacingMergeTree with settings and LowCardinality columns:
///
/// ```
/// use clickhouse::schema::{ClickHouseSchema, SchemaColumn, Engine};
/// use clickhouse_types::DataTypeNode;
///
/// let ddl = ClickHouseSchema::new([
///     SchemaColumn::new("ts", DataTypeNode::DateTime(None)),
///     SchemaColumn::new("sensor_id", DataTypeNode::Int32),
///     SchemaColumn::new("status", DataTypeNode::String).low_cardinality(),
///     SchemaColumn::new("value", DataTypeNode::Float64).nullable(),
/// ])
/// .table_name("sensor_data")
/// .engine(Engine::ReplacingMergeTree)
/// .primary_key(["ts", "sensor_id"])
/// .index_granularity(8192)
/// .to_string();
///
/// assert_eq!(ddl, r#"CREATE TABLE sensor_data (
///     ts DateTime,
///     sensor_id Int32,
///     status LowCardinality(String),
///     value Nullable(Float64)
/// ) ENGINE = ReplacingMergeTree()
/// PRIMARY KEY (ts, sensor_id)
/// SETTINGS index_granularity = 8192"#);
/// ```
///
/// Mutating columns after construction with [`find_column_mut`](ClickHouseSchema::find_column_mut):
///
/// ```
/// use clickhouse::schema::{ClickHouseSchema, SchemaColumn, Engine};
/// use clickhouse_types::DataTypeNode;
///
/// let mut schema = ClickHouseSchema::new([
///     SchemaColumn::new("id", DataTypeNode::UInt64),
///     SchemaColumn::new("tag", DataTypeNode::String),
/// ]);
/// schema.table_name("logs").primary_key(["id"]);
/// schema.find_column_mut("tag")
///     .set_low_cardinality(true)
///     .set_nullable(true);
///
/// assert_eq!(schema.to_string(), r#"CREATE TABLE logs (
///     id UInt64,
///     tag LowCardinality(Nullable(String))
/// ) ENGINE = MergeTree()
/// PRIMARY KEY (id)"#);
/// ```
#[derive(Debug, Clone)]
pub struct ClickHouseSchema {
    pub(crate) table_name: String,
    pub(crate) columns: Vec<SchemaColumn>,
    pub(crate) engine: Engine,
    primary_key: Vec<String>,
    order_by: Vec<String>,
    partition_by: Option<String>,
    settings: Vec<(String, String)>,
    extra: Option<String>,
    if_not_exists: bool,
}

/// A column definition for use in [`ClickHouseSchema`], wrapping a base
/// [`DataTypeNode`] with optional `Nullable` and `LowCardinality` modifiers.
///
/// # Examples
///
/// Owned builder methods (consume `self`, ideal for inline construction):
///
/// ```
/// use clickhouse::schema::SchemaColumn;
/// use clickhouse_types::DataTypeNode;
///
/// let col = SchemaColumn::new("tag", DataTypeNode::String)
///     .low_cardinality()
///     .nullable();
///
/// assert!(col.low_cardinality);
/// assert!(col.nullable);
/// ```
///
/// Mutable setters (return `&mut Self`, ideal for post-construction updates):
///
/// ```
/// use clickhouse::schema::SchemaColumn;
/// use clickhouse_types::DataTypeNode;
///
/// let mut col = SchemaColumn::new("status", DataTypeNode::String);
/// col.set_low_cardinality(true);
///
/// assert!(col.low_cardinality);
/// assert!(!col.nullable);
/// ```
#[derive(Debug, Clone)]
pub struct SchemaColumn {
    pub name: String,
    pub data_type: DataTypeNode,
    pub nullable: bool,
    pub low_cardinality: bool,
}

impl Default for ClickHouseSchema {
    fn default() -> Self {
        Self {
            table_name: String::new(),
            columns: Vec::new(),
            engine: Engine::MergeTree,
            primary_key: Vec::new(),
            order_by: Vec::new(),
            partition_by: None,
            settings: Vec::new(),
            extra: None,
            if_not_exists: false,
        }
    }
}

impl ClickHouseSchema {
    /// Creates a new schema builder from a list of column definitions.
    /// Accepts anything convertible to [`SchemaColumn`], including [`Column`].
    pub fn new(columns: impl IntoIterator<Item = impl Into<SchemaColumn>>) -> Self {
        Self {
            columns: columns.into_iter().map(Into::into).collect(),
            ..Default::default()
        }
    }

    /// Creates a new schema builder from an Arrow [`Schema`], converting each
    /// Arrow field to the corresponding ClickHouse column type.
    ///
    /// Nullable Arrow fields set the `nullable` flag on the [`SchemaColumn`].
    #[cfg(feature = "arrow")]
    pub fn from_arrow(schema: &sea_orm_arrow::arrow::datatypes::Schema) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let dt = arrow_to_data_type(field.data_type());
                let mut col = SchemaColumn::new(field.name().clone(), dt);
                if field.is_nullable() {
                    col.nullable = true;
                }
                col
            })
            .collect();

        Self {
            columns,
            ..Default::default()
        }
    }

    /// Creates a new schema builder from a [`DataRow`](crate::data_row::DataRow),
    /// using its column names and types.
    #[cfg(feature = "sea-ql")]
    pub fn from_data_row(row: &crate::data_row::DataRow) -> Self {
        Self::from_names_and_types(&row.column_names, &row.column_types)
    }

    /// Creates a new schema builder from a [`RowBatch`](crate::data_row::RowBatch),
    /// using its column names and types.
    #[cfg(feature = "sea-ql")]
    pub fn from_row_batch(batch: &crate::data_row::RowBatch) -> Self {
        Self::from_names_and_types(&batch.column_names, &batch.column_types)
    }

    #[cfg(feature = "sea-ql")]
    fn from_names_and_types(names: &[std::sync::Arc<str>], types: &[DataTypeNode]) -> Self {
        let columns = names
            .iter()
            .zip(types.iter())
            .map(|(name, dt)| Self::decompose_type(name.as_ref(), dt.clone()))
            .collect();
        Self {
            columns,
            ..Default::default()
        }
    }

    /// Unwraps `LowCardinality` and `Nullable` wrappers from a
    /// [`DataTypeNode`] into [`SchemaColumn`] flags.
    #[cfg(feature = "sea-ql")]
    fn decompose_type(name: &str, dt: DataTypeNode) -> SchemaColumn {
        let (dt, low_cardinality) = match dt {
            DataTypeNode::LowCardinality(inner) => (*inner, true),
            other => (other, false),
        };
        let (dt, nullable) = match dt {
            DataTypeNode::Nullable(inner) => (*inner, true),
            other => (other, false),
        };
        SchemaColumn {
            name: name.to_string(),
            data_type: dt,
            nullable,
            low_cardinality,
        }
    }

    /// Set the table name.
    pub fn table_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.table_name = name.into();
        self
    }

    /// Returns a mutable reference to the column with the given name.
    ///
    /// # Panics
    ///
    /// Panics if no column with that name exists.
    pub fn find_column_mut(&mut self, name: &str) -> &mut SchemaColumn {
        self.columns
            .iter_mut()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("column '{name}' not found"))
    }

    /// Append a column definition.
    pub fn col(&mut self, column: impl Into<SchemaColumn>) -> &mut Self {
        self.columns.push(column.into());
        self
    }

    /// Set the table engine. Defaults to [`Engine::MergeTree`].
    pub fn engine(&mut self, engine: Engine) -> &mut Self {
        self.engine = engine;
        self
    }

    /// Set the `PRIMARY KEY` columns.
    ///
    /// In ClickHouse, if no separate [`order_by`](Self::order_by) is specified,
    /// the primary key also determines the `ORDER BY`.
    pub fn primary_key(&mut self, keys: impl IntoIterator<Item = impl Into<String>>) -> &mut Self {
        self.primary_key = keys.into_iter().map(Into::into).collect();
        self
    }

    /// Set the `ORDER BY` columns.
    ///
    /// When both `primary_key` and `order_by` are set, the primary key must
    /// be a prefix of the order-by key. When only `order_by` is set,
    /// ClickHouse uses it as the primary key as well.
    pub fn order_by(&mut self, keys: impl IntoIterator<Item = impl Into<String>>) -> &mut Self {
        self.order_by = keys.into_iter().map(Into::into).collect();
        self
    }

    /// Set the `PARTITION BY` expression.
    pub fn partition_by(&mut self, expr: impl Into<String>) -> &mut Self {
        self.partition_by = Some(expr.into());
        self
    }

    /// Shorthand for `SETTINGS index_granularity = n`.
    pub fn index_granularity(&mut self, n: u32) -> &mut Self {
        self.setting("index_granularity", n.to_string())
    }

    /// Add a `SETTINGS` key-value pair.
    pub fn setting(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.settings.push((key.into(), value.into()));
        self
    }

    /// Add `IF NOT EXISTS` to the statement.
    pub fn if_not_exists(&mut self) -> &mut Self {
        self.if_not_exists = true;
        self
    }

    /// Append arbitrary SQL after the engine and settings clauses.
    pub fn extra(&mut self, sql: impl Into<String>) -> &mut Self {
        self.extra = Some(sql.into());
        self
    }

    /// Consume `self` via [`std::mem::take`], leaving `Default` in its place.
    pub fn take(&mut self) -> Self {
        std::mem::take(self)
    }
}

impl SchemaColumn {
    pub fn new(name: impl Into<String>, data_type: DataTypeNode) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: false,
            low_cardinality: false,
        }
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    pub fn low_cardinality(mut self) -> Self {
        self.low_cardinality = true;
        self
    }

    pub fn set_nullable(&mut self, val: bool) -> &mut Self {
        self.nullable = val;
        self
    }

    pub fn set_low_cardinality(&mut self, val: bool) -> &mut Self {
        self.low_cardinality = val;
        self
    }

    /// Renders the full data type, applying `Nullable` and `LowCardinality`
    /// wrappers in the order ClickHouse expects: `LowCardinality(Nullable(T))`.
    fn rendered_type(&self) -> String {
        let base = self.data_type.to_string();
        match (self.low_cardinality, self.nullable) {
            (true, true) => format!("LowCardinality(Nullable({base}))"),
            (true, false) => format!("LowCardinality({base})"),
            (false, true) => format!("Nullable({base})"),
            (false, false) => base,
        }
    }
}

impl From<Column> for SchemaColumn {
    fn from(col: Column) -> Self {
        Self::new(col.name, col.data_type)
    }
}

impl Display for ClickHouseSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CREATE TABLE ")?;
        if self.if_not_exists {
            f.write_str("IF NOT EXISTS ")?;
        }
        f.write_str(&self.table_name)?;
        f.write_str(" (\n")?;

        for (i, col) in self.columns.iter().enumerate() {
            write!(f, "    {} {}", col.name, col.rendered_type())?;
            if i + 1 < self.columns.len() {
                f.write_char(',')?;
            }
            f.write_char('\n')?;
        }

        write!(f, ") ENGINE = {}", self.engine)?;

        if self.engine.is_merge_tree_family() {
            let has_pk = !self.primary_key.is_empty();
            let has_ob = !self.order_by.is_empty();
            match (has_pk, has_ob) {
                (true, true) => {
                    write!(f, "\nORDER BY ({})", self.order_by.join(", "))?;
                    write!(f, "\nPRIMARY KEY ({})", self.primary_key.join(", "))?;
                }
                (true, false) => {
                    write!(f, "\nPRIMARY KEY ({})", self.primary_key.join(", "))?;
                }
                (false, true) => {
                    write!(f, "\nORDER BY ({})", self.order_by.join(", "))?;
                }
                (false, false) => {
                    f.write_str("\nORDER BY tuple()")?;
                }
            }
        }

        if let Some(ref partition) = self.partition_by {
            write!(f, "\nPARTITION BY {partition}")?;
        }

        if !self.settings.is_empty() {
            f.write_str("\nSETTINGS ")?;
            for (i, (key, value)) in self.settings.iter().enumerate() {
                if i > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{key} = {value}")?;
            }
        }

        if let Some(ref extra) = self.extra {
            write!(f, "\n{extra}")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clickhouse_types::DataTypeNode;
    use clickhouse_types::data_types::DecimalType;

    /// Strips the common leading whitespace from every non-empty line,
    /// and trims the first/last empty lines. Lets multiline `r#"..."#`
    /// strings stay indented with the surrounding test code.
    fn trim_indent(s: &str) -> String {
        let lines: Vec<&str> = s.lines().collect();
        let min_indent = lines
            .iter()
            .filter(|l| !l.trim().is_empty())
            .map(|l| l.len() - l.trim_start().len())
            .min()
            .unwrap_or(0);
        let trimmed: Vec<&str> = lines
            .iter()
            .map(|l| {
                if l.len() >= min_indent {
                    &l[min_indent..]
                } else {
                    l.trim()
                }
            })
            .collect();
        let s = trimmed.join("\n");
        s.trim_matches('\n').to_string()
    }

    #[test]
    fn test_basic_merge_tree_ddl() {
        let ddl = ClickHouseSchema::new([
            SchemaColumn::new("id", DataTypeNode::UInt64),
            SchemaColumn::new("name", DataTypeNode::String),
            SchemaColumn::new("value", DataTypeNode::Float64),
        ])
        .table_name("events")
        .primary_key(["id"])
        .to_string();

        assert_eq!(
            ddl,
            trim_indent(
                r#"
                CREATE TABLE events (
                    id UInt64,
                    name String,
                    value Float64
                ) ENGINE = MergeTree()
                PRIMARY KEY (id)
            "#
            )
        );
    }

    #[test]
    fn test_if_not_exists() {
        let ddl = ClickHouseSchema::new([SchemaColumn::new("id", DataTypeNode::UInt32)])
            .table_name("t")
            .if_not_exists()
            .to_string();

        assert!(ddl.starts_with("CREATE TABLE IF NOT EXISTS t"));
    }

    #[test]
    fn test_replacing_merge_tree_with_settings() {
        let ddl = ClickHouseSchema::new([
            SchemaColumn::new("ts", DataTypeNode::DateTime(None)),
            SchemaColumn::new("sensor_id", DataTypeNode::Int32),
            SchemaColumn::new("temp", DataTypeNode::Float64),
        ])
        .table_name("sensor_data")
        .engine(Engine::ReplacingMergeTree)
        .primary_key(["ts", "sensor_id"])
        .index_granularity(8192)
        .to_string();

        assert_eq!(
            ddl,
            trim_indent(
                r#"
                CREATE TABLE sensor_data (
                    ts DateTime,
                    sensor_id Int32,
                    temp Float64
                ) ENGINE = ReplacingMergeTree()
                PRIMARY KEY (ts, sensor_id)
                SETTINGS index_granularity = 8192
            "#
            )
        );
    }

    #[test]
    fn test_partition_by() {
        let ddl = ClickHouseSchema::new([
            SchemaColumn::new("id", DataTypeNode::UInt64),
            SchemaColumn::new("date", DataTypeNode::Date),
        ])
        .table_name("logs")
        .primary_key(["id"])
        .partition_by("toYYYYMM(date)")
        .to_string();

        assert!(ddl.contains("PARTITION BY toYYYYMM(date)"));
    }

    #[test]
    fn test_memory_engine_no_order_by() {
        let ddl = ClickHouseSchema::new([SchemaColumn::new("x", DataTypeNode::Int32)])
            .table_name("tmp")
            .engine(Engine::Memory)
            .to_string();

        assert_eq!(
            ddl,
            trim_indent(
                r#"
                CREATE TABLE tmp (
                    x Int32
                ) ENGINE = Memory()
            "#
            )
        );
        assert!(!ddl.contains("ORDER BY"));
    }

    #[test]
    fn test_empty_order_by_uses_tuple() {
        let ddl = ClickHouseSchema::new([SchemaColumn::new("x", DataTypeNode::Int32)])
            .table_name("t")
            .engine(Engine::MergeTree)
            .to_string();

        assert!(ddl.contains("ORDER BY tuple()"));
    }

    #[test]
    fn test_nullable_column() {
        let ddl =
            ClickHouseSchema::new([SchemaColumn::new("val", DataTypeNode::String).nullable()])
                .table_name("t")
                .engine(Engine::Memory)
                .to_string();

        assert!(ddl.contains("val Nullable(String)"));
    }

    #[test]
    fn test_collapsing_merge_tree() {
        let ddl = ClickHouseSchema::new([
            SchemaColumn::new("id", DataTypeNode::UInt64),
            SchemaColumn::new("sign", DataTypeNode::Int8),
        ])
        .table_name("t")
        .engine(Engine::CollapsingMergeTree("sign".into()))
        .primary_key(["id"])
        .to_string();

        assert!(ddl.contains("ENGINE = CollapsingMergeTree(sign)"));
    }

    #[test]
    fn test_extra_clause() {
        let ddl = ClickHouseSchema::new([
            SchemaColumn::new("id", DataTypeNode::UInt64),
            SchemaColumn::new("ts", DataTypeNode::DateTime(None)),
        ])
        .table_name("t")
        .primary_key(["id"])
        .extra("TTL ts + INTERVAL 1 MONTH")
        .to_string();

        assert!(ddl.contains("TTL ts + INTERVAL 1 MONTH"));
    }

    #[test]
    fn test_multiple_settings() {
        let ddl = ClickHouseSchema::new([SchemaColumn::new("id", DataTypeNode::UInt64)])
            .table_name("t")
            .primary_key(["id"])
            .index_granularity(4096)
            .setting("min_bytes_for_wide_part", "0")
            .to_string();

        assert!(ddl.contains("SETTINGS index_granularity = 4096, min_bytes_for_wide_part = 0"));
    }

    #[test]
    fn test_take() {
        let mut builder = ClickHouseSchema::new([SchemaColumn::new("id", DataTypeNode::UInt64)]);
        builder.table_name("t").engine(Engine::Memory);
        let owned = builder.take();
        assert_eq!(owned.table_name, "t");
        assert_eq!(owned.engine, Engine::Memory);
        assert!(builder.table_name.is_empty());
    }

    #[test]
    fn test_decimal_column() {
        let ddl = ClickHouseSchema::new([SchemaColumn::new(
            "price",
            DataTypeNode::Decimal(18, 4, DecimalType::Decimal64),
        )])
        .table_name("t")
        .engine(Engine::Memory)
        .to_string();

        assert!(ddl.contains("price Decimal(18, 4)"));
    }

    #[test]
    fn test_low_cardinality_nullable() {
        let c = SchemaColumn::new("abc", DataTypeNode::String)
            .low_cardinality()
            .nullable();
        assert!(c.low_cardinality);
        assert!(c.nullable);
        assert_eq!(c.rendered_type(), "LowCardinality(Nullable(String))");
    }

    #[test]
    fn test_nullable_low_cardinality() {
        let c = SchemaColumn::new("abc", DataTypeNode::String)
            .nullable()
            .low_cardinality();
        assert!(c.low_cardinality);
        assert!(c.nullable);
        assert_eq!(c.rendered_type(), "LowCardinality(Nullable(String))");
    }

    #[test]
    fn test_low_cardinality_in_ddl() {
        let ddl = ClickHouseSchema::new([
            SchemaColumn::new("id", DataTypeNode::UInt64),
            SchemaColumn::new("status", DataTypeNode::String).low_cardinality(),
            SchemaColumn::new("tag", DataTypeNode::String)
                .low_cardinality()
                .nullable(),
        ])
        .table_name("events")
        .primary_key(["id"])
        .to_string();

        assert_eq!(
            ddl,
            trim_indent(
                r#"
                CREATE TABLE events (
                    id UInt64,
                    status LowCardinality(String),
                    tag LowCardinality(Nullable(String))
                ) ENGINE = MergeTree()
                PRIMARY KEY (id)
            "#
            )
        );
    }

    #[test]
    fn test_from_row_batch() {
        use crate::data_row::RowBatch;
        use std::sync::Arc;

        let batch = RowBatch {
            column_names: Arc::from([Arc::from("id"), Arc::from("tag"), Arc::from("value")]),
            column_types: Arc::from([
                DataTypeNode::UInt64,
                DataTypeNode::LowCardinality(Box::new(DataTypeNode::Nullable(Box::new(
                    DataTypeNode::String,
                )))),
                DataTypeNode::Nullable(Box::new(DataTypeNode::Float64)),
            ]),
            column_data: vec![],
            num_rows: 0,
        };

        let ddl = ClickHouseSchema::from_row_batch(&batch)
            .table_name("events")
            .primary_key(["id"])
            .to_string();

        assert_eq!(
            ddl,
            trim_indent(
                r#"
                CREATE TABLE events (
                    id UInt64,
                    tag LowCardinality(Nullable(String)),
                    value Nullable(Float64)
                ) ENGINE = MergeTree()
                PRIMARY KEY (id)
            "#
            )
        );
    }

    #[test]
    fn test_from_data_row() {
        use crate::data_row::DataRow;
        use std::sync::Arc;

        let row = DataRow {
            column_names: Arc::from([Arc::from("ts"), Arc::from("sensor_id")]),
            column_types: Arc::from([DataTypeNode::DateTime(None), DataTypeNode::Int32]),
            values: vec![],
        };

        let schema = ClickHouseSchema::from_data_row(&row);
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "ts");
        assert_eq!(schema.columns[0].data_type, DataTypeNode::DateTime(None));
        assert!(!schema.columns[0].nullable);
        assert_eq!(schema.columns[1].name, "sensor_id");
        assert_eq!(schema.columns[1].data_type, DataTypeNode::Int32);
    }
}
