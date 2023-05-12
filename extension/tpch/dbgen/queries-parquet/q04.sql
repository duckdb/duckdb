SELECT
    o_orderpriority,
    count(*) AS order_count
FROM
    'duckdb_benchmark_data/tpch_sf1_parquet/orders.parquet'
WHERE
    o_orderdate >= CAST('1993-07-01' AS date)
    AND o_orderdate < CAST('1993-10-01' AS date)
    AND EXISTS (
        SELECT
            *
        FROM
            'duckdb_benchmark_data/tpch_sf1_parquet/lineitem.parquet'
        WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate)
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;
