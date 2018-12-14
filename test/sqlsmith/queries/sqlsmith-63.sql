SELECT
    subq_0.c0 AS c0
FROM (
    SELECT
        ref_0.l_commitdate AS c0
    FROM
        main.lineitem AS ref_0
    WHERE
        EXISTS (
            SELECT
                ref_0.l_shipmode AS c0, ref_2.s_name AS c1, ref_3.n_regionkey AS c2, ref_0.l_suppkey AS c3, ref_3.n_name AS c4, ref_2.s_phone AS c5, ref_0.l_suppkey AS c6, ref_0.l_linestatus AS c7
            FROM
                main.nation AS ref_1
                INNER JOIN main.supplier AS ref_2
                INNER JOIN main.nation AS ref_3 ON (((1)
                            AND (1))
                        AND (1)) ON (ref_1.n_regionkey = ref_2.s_suppkey)
            WHERE (ref_0.l_linenumber IS NULL)
            OR ((ref_1.n_nationkey IS NOT NULL)
                OR (((0)
                        AND (ref_3.n_regionkey IS NOT NULL))
                    OR (1)))
        LIMIT 101)) AS subq_0
WHERE
    subq_0.c0 IS NOT NULL
LIMIT 62
