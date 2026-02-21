set -e
cargo run --example data_rows --features=sea-ql
cargo run --example data_row_insert --features sea-ql
cargo run --example row_batch --features=sea-ql
cargo run --example arrow_batch --features=arrow
cargo run --example arrow_insert --features=arrow
cargo run --example arrow_sensor_data --features=arrow