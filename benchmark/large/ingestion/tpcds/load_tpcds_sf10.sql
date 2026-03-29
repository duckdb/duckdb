CALL dsdgen(sf = 10);
EXPORT DATABASE 'duckdb_benchmark_data/tpcds_parquet_sf10'(FORMAT PARQUET);
EXPORT DATABASE 'duckdb_benchmark_data/tpcds_csv_sf10'(FORMAT CSV);

CREATE OR REPLACE VIEW store_sales_csv AS
SELECT *
FROM read_csv('duckdb_benchmark_data/tpcds_csv_sf10/store_sales.csv');
CREATE OR REPLACE VIEW store_sales_parquet AS
SELECT *
FROM read_parquet('duckdb_benchmark_data/tpcds_parquet_sf10/store_sales.parquet');
CREATE OR REPLACE VIEW store_sales_native AS
SELECT *
FROM store_sales;

CREATE OR REPLACE VIEW inventory_csv AS
SELECT *
FROM read_csv('duckdb_benchmark_data/tpcds_csv_sf10/inventory.csv');
CREATE OR REPLACE VIEW inventory_parquet AS
SELECT *
FROM read_parquet('duckdb_benchmark_data/tpcds_parquet_sf10/inventory.parquet');
CREATE OR REPLACE VIEW inventory_native AS
SELECT *
FROM inventory;
