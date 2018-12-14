SELECT
    subq_1.c6 AS c0,
    subq_1.c0 AS c1,
    subq_1.c0 AS c2,
    subq_1.c1 AS c3,
    subq_1.c4 AS c4
FROM (
    SELECT
        subq_0.c0 AS c0,
        subq_0.c2 AS c1,
        subq_0.c3 AS c2,
        subq_0.c3 AS c3,
        subq_0.c3 AS c4,
        subq_0.c3 AS c5,
        subq_0.c3 AS c6,
        subq_0.c0 AS c7,
        subq_0.c2 AS c8,
        subq_0.c3 AS c9
    FROM (
        SELECT
            ref_2.p_brand AS c0,
            ref_0.ps_partkey AS c1,
            ref_2.p_comment AS c2,
            ref_0.ps_comment AS c3
        FROM
            main.partsupp AS ref_0
        LEFT JOIN main.nation AS ref_1
        RIGHT JOIN main.part AS ref_2 ON (39 IS NOT NULL) ON (ref_0.ps_comment = ref_1.n_name)
    WHERE (ref_2.p_type IS NOT NULL)
    AND ((ref_1.n_comment IS NULL)
        OR ((1)
            OR (1)))) AS subq_0
WHERE (EXISTS (
        SELECT
            ref_3.ps_supplycost AS c0
        FROM
            main.partsupp AS ref_3
        WHERE
            ref_3.ps_partkey IS NULL
        LIMIT 57))
AND ((((1)
            OR ((0)
                OR (subq_0.c2 IS NULL)))
        OR ((0)
            OR (1)))
    OR (subq_0.c3 IS NULL))) AS subq_1
WHERE (0)
OR ((0)
    AND (0))
LIMIT 94
