create table bluesky(j VARIANT);
insert into bluesky from read_json_objects('https://github.com/duckdb/duckdb-data/releases/download/v1.0/jsonbench_file_0001.json.gz');
