CALL dsdgen(sf=100);
EXPORT DATABASE 'duckdb_benchmark_data/tpcds_parquet_sf100' (FORMAT PARQUET);
EXPORT DATABASE 'duckdb_benchmark_data/tpcds_csv_sf100' (FORMAT CSV);

create view store_sales_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/store_sales.csv');
create view store_sales_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/store_sales.parquet');
create view store_sales_native as select * from store_sales;

create view inventory_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/inventory.csv');
create view inventory_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/inventory.parquet');
create view inventory_native as select * from inventory;