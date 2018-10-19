SELECT
    CASE WHEN 1 THEN
        CAST(coalesce(subq_0.c1, subq_0.c2) AS INTEGER)
    ELSE
        CAST(coalesce(subq_0.c1, subq_0.c2) AS INTEGER)
    END AS c0,
    subq_0.c1 AS c1
FROM (
    SELECT
        ref_0.s_comment AS c0,
        ref_1.ps_suppkey AS c1,
        ref_2.n_nationkey AS c2
    FROM
        main.supplier AS ref_0
    LEFT JOIN main.partsupp AS ref_1
    INNER JOIN main.nation AS ref_2
    INNER JOIN main.nation AS ref_3 ON (ref_2.n_nationkey = ref_3.n_nationkey)
    ON (ref_1.ps_suppkey = ref_2.n_nationkey)
    ON ((ref_0.s_suppkey IS NULL)
            OR (ref_1.ps_comment IS NULL))
    WHERE (((0)
            AND (ref_2.n_comment IS NULL))
        AND ((ref_3.n_nationkey IS NULL)
            OR (ref_2.n_regionkey IS NULL)))
    OR (1)
LIMIT 48) AS subq_0
WHERE
    subq_0.c0 IS NOT NULL
LIMIT 161
