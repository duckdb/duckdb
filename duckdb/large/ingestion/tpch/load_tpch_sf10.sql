CALL dbgen(sf=10);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_parquet_sf10' (FORMAT PARQUET);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_csv_sf10' (FORMAT CSV);

create view lineitem_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf10/lineitem.csv');
create view lineitem_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf10/lineitem.parquet');
create view lineitem_native as select * from lineitem;

create view orders_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf10/orders.csv');
create view orders_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf10/orders.parquet');
create view orders_native as select * from orders;
