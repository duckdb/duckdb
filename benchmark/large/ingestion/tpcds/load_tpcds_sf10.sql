CALL dsdgen(sf=100);
EXPORT DATABASE 'duckdb_benchmark_data/tpcds_parquet_sf10' (FORMAT PARQUET);
EXPORT DATABASE 'duckdb_benchmark_data/tpcds_csv_sf10' (FORMAT CSV);

create or replace view catalog_sales_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf10/catalog_sales.csv');
create or replace view catalog_sales_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf10/catalog_sales.parquet');
create or replace view catalog_sales_native as select * from catalog_sales;

create or replace view store_returns_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf10/store_returns.csv');
create or replace view store_returns_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf10/store_returns.parquet');
create or replace view store_returns_native as select * from store_returns;