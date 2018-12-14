SELECT
    CASE WHEN (subq_0.c1 IS NOT NULL)
        AND (1) THEN
        CAST(coalesce(subq_0.c2, subq_0.c0) AS VARCHAR)
    ELSE
        CAST(coalesce(subq_0.c2, subq_0.c0) AS VARCHAR)
    END AS c0,
    subq_0.c1 AS c1,
    subq_0.c2 AS c2
FROM (
    SELECT
        ref_0.p_name AS c0,
        ref_3.c_nationkey AS c1,
        ref_1.s_address AS c2
    FROM
        main.part AS ref_0
        INNER JOIN main.supplier AS ref_1
        INNER JOIN main.partsupp AS ref_2
        RIGHT JOIN main.customer AS ref_3 ON (ref_2.ps_partkey IS NOT NULL)
        INNER JOIN main.region AS ref_4 ON (ref_3.c_phone IS NOT NULL) ON (ref_3.c_name IS NOT NULL) ON (ref_0.p_brand = ref_1.s_name)
    WHERE
        EXISTS (
            SELECT
                ref_2.ps_comment AS c0, ref_4.r_regionkey AS c1, ref_2.ps_partkey AS c2
            FROM
                main.region AS ref_5
            WHERE
                1
            LIMIT 175)
    LIMIT 147) AS subq_0
WHERE (0)
OR (((1)
        AND (0))
    OR (1))
LIMIT 98
