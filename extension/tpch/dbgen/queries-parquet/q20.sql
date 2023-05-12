SELECT
    s_name,
    s_address
FROM
    'duckdb_benchmark_data/tpch_sf1_parquet/supplier.parquet',
    'duckdb_benchmark_data/tpch_sf1_parquet/nation.parquet'
WHERE
    s_suppkey IN (
        SELECT
            ps_suppkey
        FROM
            'duckdb_benchmark_data/tpch_sf1_parquet/partsupp.parquet'
        WHERE
            ps_partkey IN (
                SELECT
                    p_partkey
                FROM
                    'duckdb_benchmark_data/tpch_sf1_parquet/part.parquet'
                WHERE
                    p_name LIKE 'forest%')
                AND ps_availqty > (
                    SELECT
                        0.5 * sum(l_quantity)
                    FROM
                        'duckdb_benchmark_data/tpch_sf1_parquet/lineitem.parquet'
                    WHERE
                        l_partkey = ps_partkey
                        AND l_suppkey = ps_suppkey
                        AND l_shipdate >= CAST('1994-01-01' AS date)
                        AND l_shipdate < CAST('1995-01-01' AS date)))
            AND s_nationkey = n_nationkey
            AND n_name = 'CANADA'
        ORDER BY
            s_name;
