SELECT
    subq_1.c1 AS c0,
    69 AS c1
FROM (
    SELECT
        ref_3.r_comment AS c0,
        ref_0.s_phone AS c1,
        ref_1.l_linestatus AS c2,
        ref_0.s_comment AS c3
    FROM
        main.supplier AS ref_0
    RIGHT JOIN main.lineitem AS ref_1
    RIGHT JOIN main.region AS ref_2 ON ((0)
            OR (1))
    RIGHT JOIN main.region AS ref_3
    INNER JOIN main.orders AS ref_4 ON (ref_4.o_shippriority IS NOT NULL) ON (ref_2.r_name IS NOT NULL) ON (ref_0.s_address = ref_2.r_name)
    INNER JOIN (
        SELECT
            ref_5.n_comment AS c0,
            ref_5.n_nationkey AS c1,
            ref_5.n_comment AS c2
        FROM
            main.nation AS ref_5
        WHERE (1)
        AND (ref_5.n_name IS NOT NULL)) AS subq_0 ON (subq_0.c2 IS NULL)
WHERE (ref_3.r_comment IS NOT NULL)
OR (0)
LIMIT 64) AS subq_1
WHERE (CAST(coalesce(subq_1.c0, subq_1.c3) AS VARCHAR)
    IS NULL)
AND (40 IS NULL)
