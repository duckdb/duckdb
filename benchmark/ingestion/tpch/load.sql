CALL dbgen(sf=5);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_parquet_sf5' (FORMAT PARQUET);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_csv_sf5' (FORMAT CSV);

create view customer_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf5/customer.csv');
create view customer_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf5/customer.parquet');
create view customer_native as select * from customer;

create view lineitem_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf5/lineitem.csv');
create view lineitem_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf5/lineitem.parquet');
create view lineitem_native as select * from lineitem;

create view nation_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf5/nation.csv');
create view nation_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf5/nation.parquet');
create view nation_native as select * from nation;

create view orders_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf5/orders.csv');
create view orders_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf5/orders.parquet');
create view orders_native as select * from orders;

create view part_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf5/part.csv');
create view part_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf5/part.parquet');
create view part_native as select * from part;

create view partsupp_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf5/partsupp.csv');
create view partsupp_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf5/partsupp.parquet');
create view partsupp_native as select * from partsupp;

create view region_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf5/region.csv');
create view region_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf5/region.parquet');
create view region_native as select * from region;

create view supplier_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf5/supplier.csv');
create view supplier_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf5/supplier.parquet');
create view supplier_native as select * from supplier