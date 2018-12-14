SELECT
    subq_0.c0 AS c0,
    subq_0.c1 AS c1,
    subq_0.c0 AS c2,
    subq_0.c0 AS c3
FROM (
    SELECT
        ref_0.n_nationkey AS c0,
        ref_2.n_comment AS c1
    FROM
        main.nation AS ref_0
        INNER JOIN main.customer AS ref_1
        INNER JOIN main.nation AS ref_2
        INNER JOIN main.nation AS ref_3 ON (ref_2.n_nationkey = ref_3.n_nationkey) ON (((0)
                    AND (1))
                OR ((0)
                    OR (ref_1.c_mktsegment IS NOT NULL)))
            INNER JOIN main.nation AS ref_4 ON ((1)
                    AND (ref_2.n_name IS NOT NULL)) ON (ref_0.n_name = ref_1.c_name)
        WHERE (1)
        OR (ref_0.n_comment IS NULL)) AS subq_0
WHERE
    EXISTS (
        SELECT
            ref_5.s_comment AS c0, subq_0.c1 AS c1, CASE WHEN (ref_5.s_nationkey IS NULL)
                AND ((ref_5.s_address IS NULL)
                    AND (0)) THEN
                subq_0.c0
            ELSE
                subq_0.c0
            END AS c2, subq_0.c1 AS c3, ref_5.s_name AS c4
        FROM
            main.supplier AS ref_5
        WHERE
            1
        LIMIT 101)
