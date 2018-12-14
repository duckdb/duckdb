SELECT
    subq_0.c4 AS c0
FROM (
    SELECT
        ref_3.r_comment AS c0,
        ref_2.n_name AS c1,
        ref_0.o_clerk AS c2,
        ref_1.n_comment AS c3,
        ref_3.r_name AS c4,
        ref_3.r_name AS c5
    FROM
        main.orders AS ref_0
        INNER JOIN main.nation AS ref_1
        INNER JOIN main.nation AS ref_2
        RIGHT JOIN main.region AS ref_3 ON (ref_3.r_regionkey IS NOT NULL) ON (ref_1.n_nationkey = ref_3.r_regionkey) ON (ref_0.o_orderpriority = ref_2.n_name)
    WHERE
        EXISTS (
            SELECT
                ref_2.n_comment AS c0, ref_1.n_regionkey AS c1, ref_4.n_regionkey AS c2, 92 AS c3, ref_2.n_nationkey AS c4, ref_2.n_regionkey AS c5, ref_2.n_name AS c6
            FROM
                main.nation AS ref_4
            WHERE (ref_1.n_name IS NOT NULL)
            OR ((ref_3.r_regionkey IS NOT NULL)
                OR (ref_2.n_regionkey IS NULL))
        LIMIT 173)) AS subq_0
WHERE
    0
