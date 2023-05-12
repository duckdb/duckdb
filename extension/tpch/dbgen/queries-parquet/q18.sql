SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
FROM
    'duckdb_benchmark_data/tpch_sf1_parquet/customer.parquet',
    'duckdb_benchmark_data/tpch_sf1_parquet/orders.parquet',
    'duckdb_benchmark_data/tpch_sf1_parquet/lineitem.parquet'
WHERE
    o_orderkey IN (
        SELECT
            l_orderkey
        FROM
            'duckdb_benchmark_data/tpch_sf1_parquet/lineitem.parquet'
        GROUP BY
            l_orderkey
        HAVING
            sum(l_quantity) > 300)
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100;
