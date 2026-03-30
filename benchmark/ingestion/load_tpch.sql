CALL dbgen(sf=1);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_parquet' (FORMAT PARQUET);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_csv' (FORMAT CSV);

create view customer_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv/customer.csv');
create view customer_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet/customer.parquet');
create view customer_native as select * from customer;

create view lineitem_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv/lineitem.csv');
create view lineitem_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet/lineitem.parquet');
create view lineitem_native as select * from lineitem;

create view nation_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv/nation.csv');
create view nation_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet/nation.parquet');
create view nation_native as select * from nation;

create view orders_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv/orders.csv');
create view orders_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet/orders.parquet');
create view orders_native as select * from orders;

create view part_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv/part.csv');
create view part_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet/part.parquet');
create view part_native as select * from part;

create view partsupp_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv/partsupp.csv');
create view partsupp_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet/partsupp.parquet');
create view partsupp_native as select * from partsupp;

create view region_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv/region.csv');
create view region_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet/region.parquet');
create view region_native as select * from region;

create view supplier_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv/supplier.csv');
create view supplier_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet/supplier.parquet');
create view supplier_native as select * from supplier;