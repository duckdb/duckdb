SELECT
    (
        SELECT
            l_partkey
        FROM
            main.lineitem
        LIMIT 1 offset 6) AS c0,
    subq_0.c0 AS c1,
    CAST(nullif (subq_0.c0, subq_0.c0) AS VARCHAR) AS c2,
    subq_0.c0 AS c3,
    subq_0.c0 AS c4,
    subq_0.c0 AS c5,
    (
        SELECT
            c_custkey
        FROM
            main.customer
        LIMIT 1 offset 5) AS c6,
    subq_0.c0 AS c7,
    58 AS c8,
    subq_0.c0 AS c9,
    (
        SELECT
            o_shippriority
        FROM
            main.orders
        LIMIT 1 offset 4) AS c10,
    subq_0.c0 AS c11,
    subq_0.c0 AS c12
FROM (
    SELECT
        ref_1.s_phone AS c0
    FROM
        main.orders AS ref_0
    RIGHT JOIN main.supplier AS ref_1 ON ((ref_0.o_custkey IS NOT NULL)
            OR (1))
    RIGHT JOIN main.customer AS ref_2
    RIGHT JOIN main.lineitem AS ref_3 ON (ref_2.c_name IS NOT NULL) ON (ref_0.o_comment = ref_2.c_name)
WHERE (ref_0.o_orderstatus IS NOT NULL)
OR ((EXISTS (
            SELECT
                ref_3.l_shipdate AS c0, ref_0.o_shippriority AS c1, ref_1.s_nationkey AS c2, ref_4.n_nationkey AS c3
            FROM
                main.nation AS ref_4
            WHERE ((0)
                AND ((0)
                    AND (ref_2.c_custkey IS NOT NULL)))
            OR (ref_2.c_mktsegment IS NULL)))
    AND (ref_1.s_nationkey IS NOT NULL))) AS subq_0
WHERE
    1
