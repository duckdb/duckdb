CALL dbgen(sf = 1);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_parquet'(FORMAT PARQUET);
EXPORT DATABASE 'duckdb_benchmark_data/tpch_csv'(FORMAT CSV);
CREATE VIEW customer_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpch_csv/customer.csv');
CREATE VIEW customer_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpch_parquet/customer.parquet');
CREATE VIEW customer_native AS
SELECT *
FROM customer;
CREATE VIEW lineitem_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpch_csv/lineitem.csv');
CREATE VIEW lineitem_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpch_parquet/lineitem.parquet');
CREATE VIEW lineitem_native AS
SELECT *
FROM lineitem;
CREATE VIEW nation_csv AS
SELECT *
FROM read_csv('duckdb_benchmark_data/tpch_csv/nation.csv');
CREATE VIEW nation_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpch_parquet/nation.parquet');
CREATE VIEW nation_native AS
SELECT *
FROM nation;
CREATE VIEW orders_csv AS
SELECT *
FROM read_csv('duckdb_benchmark_data/tpch_csv/orders.csv');
CREATE VIEW orders_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpch_parquet/orders.parquet');
CREATE VIEW orders_native AS
SELECT *
FROM orders;
CREATE VIEW part_csv AS
SELECT *
FROM read_csv('duckdb_benchmark_data/tpch_csv/part.csv');
CREATE VIEW part_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpch_parquet/part.parquet');
CREATE VIEW part_native AS
SELECT *
FROM part;
CREATE VIEW partsupp_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpch_csv/partsupp.csv');
CREATE VIEW partsupp_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpch_parquet/partsupp.parquet');
CREATE VIEW partsupp_native AS
SELECT *
FROM partsupp;
CREATE VIEW region_csv AS
SELECT *
FROM read_csv('duckdb_benchmark_data/tpch_csv/region.csv');
CREATE VIEW region_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpch_parquet/region.parquet');
CREATE VIEW region_native AS
SELECT *
FROM region;
CREATE VIEW supplier_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpch_csv/supplier.csv');
CREATE VIEW supplier_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpch_parquet/supplier.parquet');
CREATE VIEW supplier_native AS
SELECT *
FROM supplier;
