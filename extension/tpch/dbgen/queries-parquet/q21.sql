SELECT
    s_name,
    count(*) AS numwait
FROM
    'duckdb_benchmark_data/tpch_sf1_parquet/supplier.parquet',
    'duckdb_benchmark_data/tpch_sf1_parquet/lineitem.parquet' l1,
    'duckdb_benchmark_data/tpch_sf1_parquet/orders.parquet',
    'duckdb_benchmark_data/tpch_sf1_parquet/nation.parquet'
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT
            *
        FROM
            'duckdb_benchmark_data/tpch_sf1_parquet/lineitem.parquet' l2
        WHERE
            l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey)
    AND NOT EXISTS (
        SELECT
            *
        FROM
            'duckdb_benchmark_data/tpch_sf1_parquet/lineitem.parquet' l3
        WHERE
            l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate)
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100;
