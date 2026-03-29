CALL dbgen(sf = 10);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_parquet_sf10'(FORMAT PARQUET);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_csv_sf10'(FORMAT CSV);
CREATE VIEW lineitem_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpch_csv_sf10/lineitem.csv');
CREATE VIEW lineitem_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpch_parquet_sf10/lineitem.parquet');
CREATE VIEW lineitem_native AS
SELECT *
FROM lineitem;
CREATE VIEW orders_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpch_csv_sf10/orders.csv');
CREATE VIEW orders_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpch_parquet_sf10/orders.parquet');
CREATE VIEW orders_native AS
SELECT *
FROM orders;
