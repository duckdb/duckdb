CALL dbgen(sf=100);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_parquet_sf100' (FORMAT PARQUET);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_csv_sf100' (FORMAT CSV);

create view lineitem_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf100/lineitem.csv');
create view lineitem_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf100/lineitem.parquet');
create view lineitem_native as select * from lineitem;

create view part_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf100/part.csv');
create view part_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf100/part.parquet');
create view part_native as select * from part;