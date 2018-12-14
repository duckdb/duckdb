SELECT
    subq_1.c9 AS c0,
    62 AS c1,
    subq_1.c6 AS c2
FROM (
    SELECT
        ref_1.s_address AS c0,
        ref_2.c_comment AS c1,
        subq_0.c0 AS c2,
        ref_2.c_custkey AS c3,
        ref_1.s_phone AS c4,
        subq_0.c0 AS c5,
        ref_2.c_mktsegment AS c6,
        ref_1.s_suppkey AS c7,
        ref_1.s_acctbal AS c8,
        subq_0.c0 AS c9,
        ref_1.s_acctbal AS c10,
        ref_2.c_phone AS c11,
        ref_2.c_custkey AS c12,
        ref_1.s_suppkey AS c13
    FROM (
        SELECT
            ref_0.n_comment AS c0
        FROM
            main.nation AS ref_0
        WHERE
            ref_0.n_regionkey IS NOT NULL) AS subq_0
        INNER JOIN main.supplier AS ref_1
        LEFT JOIN main.customer AS ref_2 ON ((ref_1.s_acctbal IS NOT NULL)
                AND (ref_1.s_phone IS NOT NULL)) ON (subq_0.c0 = ref_1.s_name)
    WHERE (ref_2.c_mktsegment IS NULL)
    OR ((EXISTS (
                SELECT
                    ref_2.c_comment AS c0, ref_2.c_name AS c1, ref_1.s_name AS c2, ref_2.c_address AS c3, ref_1.s_address AS c4, ref_2.c_acctbal AS c5
                FROM
                    main.lineitem AS ref_3
                WHERE
                    1
                LIMIT 63))
        AND (ref_1.s_phone IS NULL))) AS subq_1
WHERE
    1
LIMIT 102
