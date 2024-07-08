CALL dbgen(sf=10);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_parquet_sf10' (FORMAT PARQUET);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_csv_sf10' (FORMAT CSV);

create view partsupp_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf10/partsupp.csv');
create view partsupp_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf10/partsupp.parquet');
create view partsupp_native as select * from partsupp;

create view part_csv as select * from read_csv('duckdb_benchmark_data/tpch_csv_sf10/part.csv');
create view part_parquet as select * from read_parquet('duckdb_benchmark_data/tpch_parquet_sf10/part.parquet');
create view part_native as select * from part;