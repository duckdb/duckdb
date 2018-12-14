SELECT
    subq_1.c1 AS c0,
    subq_1.c0 AS c1,
    subq_1.c0 AS c2,
    CAST(nullif (CAST(nullif (CAST(nullif (subq_1.c1, subq_1.c1) AS VARCHAR), subq_1.c1) AS VARCHAR), subq_1.c1) AS VARCHAR) AS c3
FROM (
    SELECT
        subq_0.c9 AS c0,
        subq_0.c4 AS c1
    FROM (
        SELECT
            ref_0.c_address AS c0,
            ref_0.c_acctbal AS c1,
            ref_2.l_extendedprice AS c2,
            ref_0.c_phone AS c3,
            ref_0.c_phone AS c4,
            16 AS c5,
            ref_1.o_custkey AS c6,
            ref_2.l_commitdate AS c7,
            ref_1.o_orderkey AS c8,
            ref_2.l_suppkey AS c9,
            ref_1.o_clerk AS c10,
            96 AS c11,
            ref_0.c_custkey AS c12,
            ref_0.c_comment AS c13
        FROM
            main.customer AS ref_0
        LEFT JOIN main.orders AS ref_1
        INNER JOIN main.lineitem AS ref_2 ON (ref_2.l_shipinstruct IS NOT NULL) ON (ref_0.c_comment = ref_1.o_orderstatus)
    WHERE (ref_0.c_nationkey IS NULL)
    AND (((ref_1.o_custkey IS NOT NULL)
            OR (ref_0.c_phone IS NOT NULL))
        OR (0))
LIMIT 111) AS subq_0
WHERE (EXISTS (
        SELECT
            ref_3.s_address AS c0
        FROM
            main.supplier AS ref_3
        WHERE
            ref_3.s_nationkey IS NOT NULL))
    OR ((1)
        OR (0))
LIMIT 151) AS subq_1
WHERE (((EXISTS (
                SELECT
                    ref_5.n_comment AS c0, ref_4.o_comment AS c1, subq_1.c0 AS c2, ref_5.n_regionkey AS c3, 35 AS c4, ref_4.o_comment AS c5, ref_5.n_regionkey AS c6
                FROM
                    main.orders AS ref_4
                RIGHT JOIN main.nation AS ref_5 ON ((0)
                        OR (1))
            WHERE ((1)
                OR ((0)
                    OR (ref_5.n_nationkey IS NOT NULL)))
            AND (1)
        LIMIT 62))
OR ((((subq_1.c1 IS NOT NULL)
            OR (((subq_1.c1 IS NULL)
                    AND (subq_1.c1 IS NOT NULL))
                OR (subq_1.c1 IS NOT NULL)))
        OR ((subq_1.c1 IS NOT NULL)
            OR ((1)
                OR ((0)
                    OR (0)))))
    OR (subq_1.c0 IS NOT NULL)))
OR (subq_1.c1 IS NULL))
OR (subq_1.c1 IS NOT NULL)
LIMIT 83
