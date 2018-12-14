SELECT
    CASE WHEN EXISTS (
            SELECT
                subq_0.c0 AS c0,
                ref_3.s_address AS c1,
                ref_2.l_extendedprice AS c2,
                ref_3.s_nationkey AS c3,
                subq_0.c0 AS c4,
                ref_3.s_name AS c5,
                ref_3.s_suppkey AS c6,
                12 AS c7
            FROM
                main.lineitem AS ref_1
            LEFT JOIN main.lineitem AS ref_2
            RIGHT JOIN main.supplier AS ref_3 ON ((ref_2.l_tax IS NOT NULL)
                    AND (1)) ON (ref_1.l_shipinstruct = ref_2.l_returnflag)
        WHERE
            subq_0.c0 IS NULL
        LIMIT 157) THEN
    subq_0.c0
ELSE
    subq_0.c0
END AS c0,
subq_0.c0 AS c1,
subq_0.c0 AS c2,
subq_0.c0 AS c3,
subq_0.c0 AS c4,
subq_0.c0 AS c5,
subq_0.c0 AS c6,
subq_0.c0 AS c7,
subq_0.c0 AS c8,
subq_0.c0 AS c9,
subq_0.c0 AS c10,
subq_0.c0 AS c11,
subq_0.c0 AS c12,
CASE WHEN subq_0.c0 IS NOT NULL THEN
    subq_0.c0
ELSE
    subq_0.c0
END AS c13,
subq_0.c0 AS c14
FROM (
    SELECT
        ref_0.r_comment AS c0
    FROM
        main.region AS ref_0
    WHERE
        ref_0.r_regionkey IS NOT NULL
    LIMIT 143) AS subq_0
WHERE
    1
LIMIT 136
