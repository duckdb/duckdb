CALL dbgen(sf=100);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_parquet_sf100' (FORMAT PARQUET);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_csv_sf100' (FORMAT CSV);

create view customer as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf100/customer.csv');
create view customer as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf100/customer.parquet');
create view customer as select * from customer;

create view orders_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf100/orders.csv');
create view orders_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf100/orders.parquet');
create view orders_native as select * from orders;