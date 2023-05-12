SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    'duckdb_benchmark_data/tpch_sf1_parquet/customer.parquet',
    'duckdb_benchmark_data/tpch_sf1_parquet/orders.parquet',
    'duckdb_benchmark_data/tpch_sf1_parquet/lineitem.parquet',
    'duckdb_benchmark_data/tpch_sf1_parquet/supplier.parquet',
    'duckdb_benchmark_data/tpch_sf1_parquet/nation.parquet',
    'duckdb_benchmark_data/tpch_sf1_parquet/region.parquet'
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= CAST('1994-01-01' AS date)
    AND o_orderdate < CAST('1995-01-01' AS date)
GROUP BY
    n_name
ORDER BY
    revenue DESC;
