SELECT
    subq_0.c0 AS c0,
    subq_0.c0 AS c1,
    subq_1.c3 AS c2,
    81 AS c3,
    subq_1.c1 AS c4,
    ref_1.l_linestatus AS c5,
    ref_1.l_comment AS c6,
    subq_0.c0 AS c7,
    subq_1.c3 AS c8,
    subq_1.c2 AS c9,
    subq_1.c0 AS c10,
    subq_0.c0 AS c11,
    subq_0.c0 AS c12
FROM (
    SELECT
        ref_0.o_comment AS c0
    FROM
        main.orders AS ref_0
    WHERE ((ref_0.o_clerk IS NULL)
        OR (ref_0.o_comment IS NOT NULL))
    OR (ref_0.o_totalprice IS NOT NULL)
LIMIT 79) AS subq_0
    LEFT JOIN main.lineitem AS ref_1
    INNER JOIN (
        SELECT
            ref_2.r_regionkey AS c0,
            ref_2.r_comment AS c1,
            ref_2.r_regionkey AS c2,
            ref_2.r_name AS c3
        FROM
            main.region AS ref_2
        WHERE (1)
        AND (1)) AS subq_1 ON (ref_1.l_suppkey IS NOT NULL) ON (subq_0.c0 = ref_1.l_returnflag)
WHERE (subq_0.c0 IS NOT NULL)
OR (ref_1.l_shipdate IS NOT NULL)
