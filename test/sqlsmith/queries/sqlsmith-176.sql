SELECT
    subq_0.c4 AS c0,
    subq_0.c12 AS c1,
    subq_0.c0 AS c2
FROM (
    SELECT
        ref_2.ps_comment AS c0,
        ref_0.s_suppkey AS c1,
        ref_1.c_acctbal AS c2,
        ref_2.ps_partkey AS c3,
        (
            SELECT
                ps_suppkey
            FROM
                main.partsupp
            LIMIT 1 offset 5) AS c4,
        ref_1.c_phone AS c5,
        ref_0.s_phone AS c6,
        ref_0.s_suppkey AS c7,
        ref_3.p_type AS c8,
        ref_2.ps_suppkey AS c9,
        ref_3.p_brand AS c10,
        ref_2.ps_supplycost AS c11,
        ref_1.c_name AS c12,
        ref_1.c_phone AS c13
    FROM
        main.supplier AS ref_0
        INNER JOIN main.customer AS ref_1
        LEFT JOIN main.partsupp AS ref_2
        LEFT JOIN main.part AS ref_3 ON (((0)
                    OR (1))
                OR (0)) ON (ref_1.c_mktsegment = ref_2.ps_comment) ON (ref_0.s_acctbal = ref_2.ps_supplycost)
    WHERE
        ref_2.ps_comment IS NOT NULL
    LIMIT 142) AS subq_0
WHERE (((EXISTS (
                SELECT
                    ref_4.l_shipinstruct AS c0, ref_4.l_receiptdate AS c1, subq_0.c11 AS c2, subq_0.c12 AS c3, subq_0.c11 AS c4, subq_0.c5 AS c5, ref_4.l_receiptdate AS c6, subq_0.c3 AS c7, ref_4.l_commitdate AS c8, ref_4.l_suppkey AS c9
                FROM
                    main.lineitem AS ref_4
                WHERE
                    subq_0.c11 IS NOT NULL))
            AND (0))
        OR (subq_0.c8 IS NOT NULL))
    AND ((subq_0.c1 IS NOT NULL)
        AND (subq_0.c1 IS NOT NULL))
