# Clickhouse for SeaQL + Arrow

A ClickHouse client that integrates with the [SeaQL](https://github.com/SeaQL/sea-orm/) ecosystem.
Query results are decoded into `sea_query::Value`, giving you first-class support for
`chrono` datetimes, `rust_decimal::Decimal`, `bigdecimal::BigDecimal`, UUIDs, JSON arrays,
and more - without defining any schema structs.

Apache Arrow is also supported: stream query results directly into `RecordBatch`es, or insert
Arrow batches back into ClickHouse.

This is a soft fork of [clickhouse.rs](https://github.com/ClickHouse/clickhouse-rs), 100% compatible with all upstream features, and continually rebased on upstream.

## Features

- **Dynamic rows** - fetch results as `Vec<DataRow>` with no compile-time schema
- **SeaQuery values** - every column maps to a typed `sea_query::Value` variant
- **Rich types** - `chrono` dates/datetimes, `Decimal`, `BigDecimal`, `Uuid`, JSON
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

```rust
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

```rust
let mut cursor = client
    .query("SELECT 1::UInt32 + 1::Float32 AS value") // what's the output type?
    .fetch_rows()?;

let row = cursor.next().await?.expect("expected one row");

// UInt32 + Float32 -> Float64
assert_eq!(row.try_get::<f64, _>(0)?, 2.0); // designated type
assert_eq!(row.try_get::<f32, _>(0)?, 2.0); // also works
assert_eq!(row.try_get::<Decimal, _>("value")?, 2.into()); // also works
```

## Insert DataRows

Build `DataRow`s with a shared column list and insert them in a single streaming request.

```rust
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

```rust
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

```rust
use sea_orm_arrow::arrow::util::pretty;

let mut cursor = client.query("SELECT * FROM sensor_data").fetch_rows()?;

while let Some(batch) = cursor.next_arrow_batch(1000).await? {
    pretty::print_batches(&[batch]).unwrap();
}
```

```
$ cargo run --example arrow_sensor_data --features arrow
+----+---------------------+-----------+---------------------+---------+
| id | recorded_at         | sensor_id | temperature         | voltage |
+----+---------------------+-----------+---------------------+---------+
| 1  | 2026-01-01T23:42:41 | 101       | -5.852258704104257  | 3.3961  |
| 2  | 2026-01-01T03:38:50 | 100       | 11.739516305712755  | 3.1730  |
| 3  | 2026-01-01T07:04:50 | 100       | 39.84085020857278   | 3.2090  |
| 4  | 2026-01-01T05:16:12 | 102       | -2.9822916019279226 | 3.4772  |
| 5  | 2026-01-01T04:53:54 | 104       | 22.093354543965127  | 3.3434  |
| 6  | 2026-01-01T06:23:54 | 104       | 1.6905997847872278  | 3.1833  |
| 7  | 2026-01-01T01:24:23 | 103       | -6.1476672197139095 | 3.1663  |
| 8  | 2026-01-01T12:14:08 | 108       | -6.293130672459389  | 3.1048  |
| 9  | 2026-01-01T04:16:57 | 107       | 11.086130037421675  | 3.2016  |
| 10 | 2026-01-01T02:05:57 | 107       | 30.103786861677598  | 3.2357  |
+----+---------------------+-----------+---------------------+---------+
```

### Insert Arrow batches

Build an Arrow `RecordBatch` [using SeaORM](https://github.com/SeaQL/sea-orm/blob/master/examples/parquet_example/src/main.rs) and insert it directly into ClickHouse.

```rust
use sea_orm::ArrowSchema;

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
        #[sea_orm(column_type = "Decimal(Some((10, 4)))")]
        pub voltage: Decimal,
    }

    impl ActiveModelBehavior for ActiveModel {}
}

let schema = measurement::Entity::arrow_schema();

let models: Vec<measurement::ActiveModel> = (1..=100)
    .map(|i| {
        measurement::ActiveModel {
            id: Set(i),
            recorded_at: Set(timestamp),
            sensor_id: Set(sensor_id),
            temperature: Set(temperature),
            voltage: Set(voltage),
        }
    })
    .collect();

let batch = measurement::ActiveModel::to_arrow(&models, &schema)?;

let mut insert = client.insert_arrow("measurement", &batch).await?;
insert.write_batch(&batch).await?;
insert.end().await?;
```

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
| [`row_batch`](examples/row_batch.rs) | `sea-ql` | Column-oriented batch streaming |
| [`arrow_batch`](examples/arrow_batch.rs) | `arrow` | Stream query results as `RecordBatch`es |
| [`arrow_insert`](examples/arrow_insert.rs) | `arrow` | Arrow `RecordBatch` insert round-trip with Decimal128 / Decimal256 |
| [`arrow_sensor_data`](examples/arrow_sensor_data.rs) | `arrow` | Practical example of sensor data processing via Arrow |

```sh
cargo run --example data_rows         --features sea-ql
cargo run --example data_row_insert   --features sea-ql
cargo run --example row_batch         --features sea-ql
cargo run --example arrow_batch       --features arrow
cargo run --example arrow_insert      --features arrow
cargo run --example arrow_sensor_data --features arrow
```

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

We invite you to participate, contribute and together help build Rust's future.

## Mascot

A friend of Ferris, Terres the hermit crab is the official mascot of SeaORM. His hobby is collecting shells.

<img alt="Terres" src="https://www.sea-ql.org/SeaORM/img/Terres.png" width="400"/>
