SELECT
    (
        SELECT
            s_address
        FROM
            main.supplier
        LIMIT 1 offset 6) AS c0,
    subq_0.c2 AS c1,
    subq_0.c1 AS c2,
    subq_0.c6 AS c3,
    subq_0.c0 AS c4
FROM (
    SELECT
        ref_2.n_nationkey AS c0,
        66 AS c1,
        ref_0.r_comment AS c2,
        ref_0.r_name AS c3,
        ref_2.n_regionkey AS c4,
        ref_1.l_tax AS c5,
        ref_2.n_name AS c6
    FROM
        main.region AS ref_0
        INNER JOIN main.lineitem AS ref_1
        RIGHT JOIN main.nation AS ref_2 ON (ref_2.n_nationkey IS NOT NULL) ON (ref_0.r_name = ref_1.l_returnflag)
    WHERE
        EXISTS (
            SELECT
                ref_3.c_acctbal AS c0, ref_3.c_comment AS c1
            FROM
                main.customer AS ref_3
            WHERE
                0
            LIMIT 142)) AS subq_0
WHERE ((1)
    OR (0))
AND (
    CASE WHEN 0 THEN
        subq_0.c1
    ELSE
        subq_0.c1
    END IS NULL)
LIMIT 134
