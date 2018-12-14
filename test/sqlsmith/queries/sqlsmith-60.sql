WITH jennifer_0 AS (
    SELECT
        ref_0.p_type AS c0,
        ref_0.p_partkey AS c1,
        ref_0.p_name AS c2,
        ref_0.p_retailprice AS c3,
        ref_0.p_name AS c4,
        ref_0.p_name AS c5
    FROM
        main.part AS ref_0
    WHERE ((1)
        OR (((
                    SELECT
                        o_clerk
                    FROM
                        main.orders
                    LIMIT 1 offset 3)
                IS NOT NULL)
            AND (ref_0.p_container IS NULL)))
    AND (ref_0.p_mfgr IS NULL)
)
SELECT
    subq_0.c0 AS c0,
    subq_0.c0 AS c1,
    59 AS c2,
    43 AS c3,
    subq_0.c0 AS c4,
    subq_0.c0 AS c5
FROM (
    SELECT
        ref_2.ps_supplycost AS c0
    FROM
        main.partsupp AS ref_1
        INNER JOIN main.partsupp AS ref_2
        LEFT JOIN main.nation AS ref_3
        LEFT JOIN main.customer AS ref_4 ON ((0)
                OR (ref_3.n_nationkey IS NOT NULL)) ON (ref_2.ps_partkey = ref_3.n_nationkey) ON (ref_4.c_address IS NULL)
    WHERE
        EXISTS (
            SELECT
                ref_2.ps_comment AS c0, ref_3.n_name AS c1, ref_5.c_name AS c2, ref_3.n_name AS c3, ref_2.ps_suppkey AS c4, ref_4.c_phone AS c5, ref_3.n_regionkey AS c6, ref_5.c_phone AS c7, ref_2.ps_availqty AS c8, ref_4.c_address AS c9, ref_2.ps_comment AS c10, ref_2.ps_suppkey AS c11, ref_1.ps_suppkey AS c12, ref_5.c_mktsegment AS c13, ref_3.n_nationkey AS c14, ref_3.n_comment AS c15, ref_1.ps_supplycost AS c16, ref_5.c_phone AS c17, ref_4.c_comment AS c18
            FROM
                main.customer AS ref_5
            WHERE (ref_1.ps_supplycost IS NOT NULL)
            OR (ref_5.c_address IS NOT NULL))
    LIMIT 146) AS subq_0
WHERE ((1)
    OR (((subq_0.c0 IS NOT NULL)
            AND (0))
        AND (EXISTS (
                SELECT
                    subq_0.c0 AS c0, ref_6.o_shippriority AS c1, subq_0.c0 AS c2, subq_0.c0 AS c3, subq_0.c0 AS c4, subq_0.c0 AS c5, ref_6.o_orderpriority AS c6, ref_6.o_totalprice AS c7, ref_6.o_custkey AS c8
                FROM
                    main.orders AS ref_6
                WHERE
                    ref_6.o_clerk IS NOT NULL
                LIMIT 44))))
OR (subq_0.c0 IS NULL)
LIMIT 47
