SELECT
    subq_0.c2 AS c0,
    subq_0.c0 AS c1,
    subq_0.c1 AS c2,
    subq_0.c2 AS c3,
    subq_0.c1 AS c4,
    subq_0.c2 AS c5,
    CASE WHEN 0 THEN
        subq_0.c2
    ELSE
        subq_0.c2
    END AS c6,
    CASE WHEN 0 THEN
        subq_0.c2
    ELSE
        subq_0.c2
    END AS c7,
    subq_0.c2 AS c8,
    subq_0.c1 AS c9,
    subq_0.c2 AS c10,
    subq_0.c0 AS c11,
    subq_0.c1 AS c12,
    subq_0.c0 AS c13,
    subq_0.c0 AS c14,
    subq_0.c2 AS c15,
    subq_0.c2 AS c16
FROM (
    SELECT
        ref_0.l_orderkey AS c0,
        ref_2.s_nationkey AS c1,
        ref_2.s_phone AS c2
    FROM
        main.lineitem AS ref_0
        INNER JOIN main.supplier AS ref_1
        INNER JOIN main.supplier AS ref_2 ON (ref_1.s_comment IS NOT NULL) ON (ref_0.l_extendedprice = ref_2.s_acctbal)
    WHERE
        ref_1.s_acctbal IS NOT NULL
    LIMIT 46) AS subq_0
WHERE
    subq_0.c1 IS NOT NULL
LIMIT 167
