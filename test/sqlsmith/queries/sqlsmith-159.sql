SELECT
    34 AS c0,
    12 AS c1,
    subq_0.c0 AS c2,
    subq_0.c1 AS c3,
    CASE WHEN (((subq_0.c1 IS NULL)
                AND (1))
            OR (subq_0.c0 IS NULL))
        AND (1) THEN
        subq_0.c0
    ELSE
        subq_0.c0
    END AS c4
FROM (
    SELECT
        ref_4.c_nationkey AS c0,
        ref_3.c_comment AS c1,
        ref_2.o_shippriority AS c2
    FROM
        main.orders AS ref_0
        INNER JOIN main.customer AS ref_1 ON (ref_0.o_comment IS NULL)
        INNER JOIN main.orders AS ref_2
        LEFT JOIN main.customer AS ref_3 ON (ref_2.o_custkey = ref_3.c_custkey)
        INNER JOIN main.customer AS ref_4 ON (ref_4.c_nationkey IS NOT NULL) ON (ref_0.o_comment = ref_2.o_orderstatus)
    WHERE (0)
    OR ((1)
        OR (ref_1.c_acctbal IS NOT NULL))
LIMIT 68) AS subq_0
WHERE
    subq_0.c2 IS NOT NULL
LIMIT 156
