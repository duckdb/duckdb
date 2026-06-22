create table bluesky_raw(j VARIANT);
insert into bluesky_raw from read_json_objects('https://github.com/duckdb/duckdb-data/releases/download/v1.0/jsonbench_file_0001.json.gz');
copy bluesky_raw to 'duckdb_benchmark_data/jsonbench_shredded.parquet';
drop table bluesky_raw;
create view bluesky as select j from 'duckdb_benchmark_data/jsonbench_shredded.parquet';
