SELECT
    subq_0.c0 AS c0,
    subq_0.c0 AS c1,
    subq_0.c0 AS c2,
    subq_0.c0 AS c3,
    subq_0.c0 AS c4,
    subq_0.c0 AS c5,
    subq_0.c0 AS c6,
    subq_0.c0 AS c7,
    subq_0.c0 AS c8,
    subq_0.c0 AS c9,
    subq_0.c0 AS c10,
    subq_0.c0 AS c11
FROM (
    SELECT
        ref_0.l_receiptdate AS c0
    FROM
        main.lineitem AS ref_0
    WHERE
        ref_0.l_discount IS NOT NULL
    LIMIT 152) AS subq_0
WHERE
    EXISTS (
        SELECT
            ref_1.l_suppkey AS c0, ref_5.n_nationkey AS c1
        FROM
            main.lineitem AS ref_1
            INNER JOIN main.lineitem AS ref_2
            RIGHT JOIN main.nation AS ref_3 ON ((ref_3.n_comment IS NULL)
                    OR (73 IS NULL)) ON (ref_1.l_shipmode = ref_2.l_returnflag)
                INNER JOIN main.supplier AS ref_4
                LEFT JOIN main.nation AS ref_5 ON (ref_5.n_nationkey IS NOT NULL) ON (ref_1.l_shipdate IS NOT NULL)
            WHERE (ref_5.n_nationkey IS NULL)
            OR ((0)
                OR (ref_5.n_regionkey IS NULL))
        LIMIT 105)
