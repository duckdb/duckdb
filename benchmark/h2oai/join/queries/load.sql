CREATE TABLE IF NOT EXISTS x AS SELECT * FROM read_csv_auto('https://github.com/duckdb/duckdb-data/releases/download/v1.0/J1_1e7_NA_0_0.csv.gz');
CREATE TABLE IF NOT EXISTS small AS SELECT * FROM read_csv_auto('https://github.com/duckdb/duckdb-data/releases/download/v1.0/J1_1e7_1e1_0_0.csv.gz');
CREATE TABLE IF NOT EXISTS medium AS SELECT * FROM read_csv_auto('https://github.com/duckdb/duckdb-data/releases/download/v1.0/J1_1e7_1e4_0_0.csv.gz');
CREATE TABLE IF NOT EXISTS big AS SELECT * FROM read_csv_auto('https://github.com/duckdb/duckdb-data/releases/download/v1.0/J1_1e7_1e7_0_0.csv.gz');
