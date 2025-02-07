CALL dsdgen(sf=10);
EXPORT DATABASE 'duckdb_benchmark_data/tpcds_parquet_sf10' (FORMAT PARQUET);
EXPORT DATABASE 'duckdb_benchmark_data/tpcds_csv_sf10' (FORMAT CSV);

create or replace view store_sales_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf10/store_sales.csv');
create or replace view store_sales_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf10/store_sales.parquet');
create or replace view store_sales_native as select * from store_sales;

create or replace view inventory_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf10/inventory.csv');
create or replace view inventory_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf10/inventory.parquet');
create or replace view inventory_native as select * from inventory;
