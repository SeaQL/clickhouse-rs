# ClickHouse for SeaQL

A ClickHouse client that integrates with the [SeaQL](https://github.com/SeaQL/sea-orm/) ecosystem.
Query results are decoded into `sea_query::Value`, giving you first-class support for
`DateTime`, `Decimal`, `BigDecimal`, `Json` arrays,
and more - without defining any schema structs.

[Apache Arrow](https://docs.rs/arrow/latest/arrow/) is also supported: stream query results directly into `RecordBatch`es, or insert
Arrow batches back into ClickHouse.

This is a soft fork of [clickhouse.rs](https://github.com/ClickHouse/clickhouse-rs), 100% compatible with all upstream features, and continually rebased on upstream.

## Features

- **Dynamic rows** - fetch results as `Vec<DataRow>` with no compile-time schema
- **SeaQuery values** - every column maps to a typed `sea_query::Value` variant
- **Rich types** - `Date`, `Time`, `DateTime`, `Decimal`, `BigDecimal`, `Json`
- **Column-oriented batches** - `next_batch(n)` streams rows in column-major `RowBatch`es
- **Apache Arrow** - stream query results as `RecordBatch`es; insert Arrow batches directly

## Setup

```toml
[dependencies]
# Dynamic DataRow + SeaQuery value support
sea-clickhouse = { version = "0.14", features = ["sea-ql"] }

# Apache Arrow support (includes sea-ql)
sea-clickhouse = { version = "0.14", features = ["arrow"] }
```

## Dynamic DataRow

`fetch_rows()` decodes every column into the matching `sea_query::Value` variant:
integers, floats, strings, booleans, dates, decimals, arrays - all without a schema struct.

```rust,ignore
use clickhouse::{Client, DataRow, error::Result};
use sea_query::Value;
use sea_query::value::prelude::{BigDecimal, Decimal, NaiveDate, NaiveDateTime, NaiveTime};

let mut cursor = client
    .query(
        "SELECT
            1::UInt8                                 AS u8_col,
            3.14::Float64                            AS f64_col,
            'hello'::String                          AS str_col,
            toDate('2026-01-15')                     AS date_col,
            toDateTime('2026-01-15 12:34:56')        AS dt_col,
            toDecimal64(123.45, 2)                   AS dec64_col,
            toDecimal256('3.14159265358979', 14)     AS dec256_col,
            NULL::Nullable(Int32)                    AS null_col,
            ['a', 'b', 'c']::Array(String)           AS arr_col
        ",
    )
    .fetch_rows()?;

let row = cursor.next().await?.unwrap();
let DataRow { columns, values } = &row;

assert_eq!(values[0], Value::TinyUnsigned(Some(1)));
assert_eq!(values[1], Value::Double(Some(3.14)));
assert_eq!(values[2], Value::String(Some("hello".into())));
assert_eq!(
    values[3],
    Value::ChronoDate(Some(NaiveDate::from_ymd_opt(2026, 1, 15).unwrap()))
);
assert_eq!(
    values[4],
    Value::ChronoDateTime(Some(NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2026, 1, 15).unwrap(),
        NaiveTime::from_hms_opt(12, 34, 56).unwrap(),
    )))
);
// Decimal32 / Decimal64 / Decimal128  ->  Value::Decimal
assert_eq!(values[5], Value::Decimal(Some(Decimal::from_i128_with_scale(12345, 2))));
// Decimal128 / Decimal256  ->  Value::BigDecimal
assert_eq!(
    values[6],
    Value::BigDecimal(Some(Box::new("3.14159265358979".parse::<BigDecimal>().unwrap())))
);
assert_eq!(values[7], Value::Int(None)); // Nullable value -> typed None
// Array(String) -> Json array
let expected_arr = serde_json::json!(["a", "b", "c"]);
assert_eq!(values[8], Value::Json(Some(Box::new(expected_arr))));
```

## No-fuzz Dynamic Type

No need to guess the resulting type of a SQL expression, it can be converted to the desired type on runtime:

```rust,ignore
let mut cursor = client
    .query("SELECT 1::UInt32 + 1::Float32 AS value") // what's the output type?
    .fetch_rows()?;

let row = cursor.next().await?.expect("expected one row");

// UInt32 + Float32 -> Float64
assert_eq!(row.try_get::<f64, _>(0)?, 2.0); // designated type
assert_eq!(row.try_get::<f32, _>(0)?, 2.0); // get by index, also works
assert_eq!(row.try_get::<Decimal, _>("value")?, 2.into()); // get by column name, also works
```

## Insert DataRows

Build `DataRow`s with a shared column list and insert them in a single streaming request.

```rust,ignore
use std::sync::Arc;
use clickhouse::{Client, DataRow};
use sea_query::Value;

let columns: Arc<[Arc<str>]> = Arc::from(["id".into(), "name".into(), "score".into()]);

let rows: Vec<DataRow> = (0u32..5)
    .map(|i| DataRow {
        columns: columns.clone(),
        values: vec![
            Value::Unsigned(Some(i)),
            Value::String(Some("original".into())),
            Value::Double(Some(i as f64 * 1.5)),
        ],
    })
    .collect();

// schema derived from first row
let mut insert = client.insert_data_row("my_table", &rows[0]).await?;
for row in &rows {
    insert.write_row(row).await?;
}
insert.end().await?;
```

## Column-oriented batches

`next_batch(max_rows)` accumulates rows column-by-column into a `RowBatch`:
one `Vec<Value>` per column, making it a natural bridge toward Apache Arrow.

```rust,ignore
let mut cursor = client
    .query("SELECT number::UInt64 AS n, number * 2 AS doubled FROM system.numbers LIMIT 1000")
    .fetch_rows()?;

while let Some(batch) = cursor.next_batch(256).await? {
    // batch.columns[i]       - column name
    // batch.column_data[i]   - Vec<Value> for column i
    // batch.num_rows
}
```

## Apache Arrow

`next_arrow_batch(chunk_size)` streams ClickHouse results as `arrow::RecordBatch`es -
ready for DataFusion, Polars, Parquet export, or any Arrow consumer.

```rust,ignore
use sea_orm_arrow::arrow::util::pretty;

let mut cursor = client.query("SELECT * FROM sensor_data").fetch_rows()?;

while let Some(batch) = cursor.next_arrow_batch(1000).await? {
    pretty::print_batches(&[batch]).unwrap();
}
```

```ignore
$ cargo run --example arrow_sensor_data --features=arrow,chrono,rust_decimal
+----+-------------------------+-----------+----------------------+---------+
| id | recorded_at             | sensor_id | temperature          | voltage |
+----+-------------------------+-----------+----------------------+---------+
| 1  | 2026-01-01T13:35:36.736 | 106       | 36.345616831016436   | 3.2736  |
| 2  | 2026-01-01T10:07:38.458 | 108       | 10.122001773336567   | 3.3458  |
| 3  | 2026-01-01T01:15:18.518 | 108       | 35.21406789966149    | 3.1518  |
| 4  | 2026-01-01T05:36:57.017 | 107       | 22.92828141235666    | 3.2016  |
| 5  | 2026-01-01T13:17:36.056 | 106       | -2.082591477369223   | 3.0056  |
| 6  | 2026-01-01T02:08:08.688 | 108       | 18.693990809409808   | 3.1688  |
| 7  | 2026-01-01T23:09:28.768 | 108       | 30.205472457922546   | 3.0768  |
| 8  | 2026-01-01T15:14:07.247 | 107       | 1.8525432800697583   | 3.0247  |
| 9  | 2026-01-01T05:15:53.753 | 103       | 21.397067736011795   | 3.0753  |
| 10 | 2026-01-01T00:02:49.769 | 109       | 17.550203554882934   | 3.0769  |
+----+-------------------------+-----------+----------------------+---------+
```

### SeaORM -> ClickHouse

Build an Arrow `RecordBatch` [using SeaORM](https://github.com/SeaQL/sea-orm/blob/master/examples/parquet_example/src/main.rs) and insert it directly into ClickHouse.
Full working example: [`sea-orm-arrow-example`](sea-orm-arrow-example/src/main.rs).

```rust,ignore
use sea_orm::prelude::*;
use sea_orm::{ArrowSchema, Set};

mod measurement {
    use sea_orm::entity::prelude::*;

    #[sea_orm::model]
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "measurement", arrow_schema)]
    pub struct Model {
        #[sea_orm(primary_key)]
        pub id: i32,
        pub recorded_at: ChronoDateTime,
        pub sensor_id: i32,
        pub temperature: f64,
        #[sea_orm(column_type = "Decimal(Some((38, 4)))")]
        pub voltage: Decimal,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

let base_ts = chrono::NaiveDate::from_ymd_opt(2026, 6, 15)
    .unwrap()
    .and_hms_milli_opt(8, 0, 0, 0)
    .unwrap();

let models: Vec<measurement::ActiveModel> = (1..=10)
    .map(|i| {
        let millis = i as u64 * 60_000 + ((i as u64 * 137 + 42) % 1000);
        measurement::ActiveModel {
        id: Set(i),
        recorded_at: Set(base_ts + std::time::Duration::from_millis(millis)),
        sensor_id: Set(100 + (i % 3)),
        temperature: Set(20.0 + i as f64 * 0.5),
        voltage: Set(Decimal::new(30000 + i as i64 * 100, 4)),
    }})
    .collect();

let schema = measurement::Entity::arrow_schema();
let batch = measurement::ActiveModel::to_arrow(&models, &schema)?;

let mut insert = client.insert_arrow("measurement", &batch).await?;
insert.write_batch(&batch).await?;
insert.end().await?;
```

### Arrow Schema to ClickHouse Table

`ClickHouseSchema::from_arrow` derives a full `CREATE TABLE` DDL from an Arrow schema,
so you can go from query result to table definition without writing any DDL by hand.

```rust,ignore
use clickhouse::schema::{ClickHouseSchema, Engine};
use sea_orm_arrow::arrow::array::RecordBatch;

// 1. Stream any query as Arrow batches
let mut cursor = client.query("SELECT ...").fetch_rows()?;
let mut batches: Vec<RecordBatch> = Vec::new();
while let Some(batch) = cursor.next_arrow_batch(1000).await? {
    batches.push(batch);
}

// 2. Derive the CREATE TABLE DDL from the Arrow schema
let mut schema = ClickHouseSchema::from_arrow(&batches[0].schema());
schema
    .table_name("my_table")
    .engine(Engine::ReplacingMergeTree)
    .primary_key(["recorded_at", "device"]);
schema.find_column_mut("device").set_low_cardinality(true);

let ddl = schema.to_string();
assert_eq!(ddl, r#"
CREATE TABLE my_table (
    id UInt64,
    recorded_at DateTime64(6),
    device LowCardinality(String),
    temperature Nullable(Float64),
    voltage Decimal(38, 4)
) ENGINE = ReplacingMergeTree()
PRIMARY KEY (recorded_at, device)"#);

client.query(&ddl).execute().await?;

// 3. Insert the batches
let mut insert = client.insert_arrow("my_table", &batches[0]).await?;
for batch in &batches {
    insert.write_batch(batch).await?;
}
insert.end().await?;
```

The same workflow works with `DataRow` via `ClickHouseSchema::from_data_row` too.

## Type mapping

| ClickHouse type | `sea_query::Value` variant |
|---|---|
| `Bool` | `Value::Bool` |
| `Int8` / `Int16` / `Int32` / `Int64` | `Value::TinyInt` / `SmallInt` / `Int` / `BigInt` |
| `UInt8` / `UInt16` / `UInt32` / `UInt64` | `Value::TinyUnsigned` / `SmallUnsigned` / `Unsigned` / `BigUnsigned` |
| `Int128` / `Int256` / `UInt128` / `UInt256` | `Value::BigDecimal` (scale 0) |
| `Float32` | `Value::Float` |
| `Float64` | `Value::Double` |
| `String` | `Value::String` |
| `FixedString(n)` | `Value::Bytes` |
| `UUID` | `Value::Uuid` |
| `Date` / `Date32` | `Value::ChronoDate` |
| `DateTime` / `DateTime64` | `Value::ChronoDateTime` |
| `Time` / `Time64` | `Value::ChronoTime` |
| `Decimal32` / `Decimal64` | `Value::Decimal` (`rust_decimal`) |
| `Decimal128` | `Value::Decimal`, or `Value::BigDecimal` if scale > 28 |
| `Decimal256` | `Value::BigDecimal` (`bigdecimal`) |
| `IPv4` / `IPv6` | `Value::String` |
| `Enum8` / `Enum16` | `Value::String` |
| `Array(T)` / `Tuple(…)` / `Map(K,V)` | `Value::Json` |
| `Nullable(T)` null | typed `None` variant |

## Examples

| Example | Feature | Description |
|---|---|---|
| [`data_rows`](examples/data_rows.rs) | `sea-ql` | Fetch rows; assert type mappings for all major types |
| [`data_row_insert`](examples/data_row_insert.rs) | `sea-ql` | Insert, mutate in place, re-insert (ReplacingMergeTree pattern) |
| [`data_row_schema`](examples/data_row_schema.rs) | `sea-ql` | Derive `CREATE TABLE` DDL from a `DataRow` |
| [`row_batch`](examples/row_batch.rs) | `sea-ql` | Column-oriented batch streaming |
| [`arrow_batch`](examples/arrow_batch.rs) | `arrow` | Stream query results as `RecordBatch`es |
| [`arrow_batch_schema`](examples/arrow_batch_schema.rs) | `arrow` | Derive `CREATE TABLE` DDL from an Arrow `RecordBatch` |
| [`arrow_insert`](examples/arrow_insert.rs) | `arrow` | Arrow `RecordBatch` insert round-trip with Decimal128 / Decimal256 |
| [`arrow_sensor_data`](examples/arrow_sensor_data.rs) | `arrow` | Practical example of sensor data processing via Arrow |
| [`sea-orm-arrow-example`](sea-orm-arrow-example/src/main.rs) | `arrow` | SeaORM entity -> Arrow RecordBatch -> ClickHouse insert |

```sh
cargo run --example data_rows          --features sea-ql
cargo run --example data_row_insert    --features sea-ql
cargo run --example data_row_schema    --features=sea-ql,chrono,rust_decimal
cargo run --example row_batch          --features=sea-ql
cargo run --example arrow_batch        --features=arrow
cargo run --example arrow_batch_schema --features arrow,chrono,rust_decimal
cargo run --example arrow_insert       --features=arrow,rust_decimal,bigdecimal
cargo run --example arrow_sensor_data  --features=arrow,chrono,rust_decimal
cargo run -p sea-orm-arrow-example
```

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

We invite you to participate, contribute and together help build Rust's future.

## Mascot

A friend of Ferris, Terres the hermit crab is the official mascot of SeaORM. His hobby is collecting shells.

<img alt="Terres" src="https://www.sea-ql.org/SeaORM/img/Terres.png" width="400"/>
