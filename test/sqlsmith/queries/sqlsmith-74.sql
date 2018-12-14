SELECT
    subq_0.c3 AS c0,
    (
        SELECT
            r_name
        FROM
            main.region
        LIMIT 1 offset 1) AS c1,
    subq_0.c5 AS c2,
    subq_0.c0 AS c3
FROM (
    SELECT
        ref_0.n_regionkey AS c0,
        ref_2.l_shipdate AS c1,
        ref_0.n_regionkey AS c2,
        ref_1.s_phone AS c3,
        ref_0.n_comment AS c4,
        ref_0.n_nationkey AS c5,
        ref_0.n_name AS c6,
        ref_1.s_acctbal AS c7,
        ref_1.s_name AS c8
    FROM
        main.nation AS ref_0
        INNER JOIN main.supplier AS ref_1
        LEFT JOIN main.lineitem AS ref_2 ON (ref_2.l_linestatus IS NOT NULL) ON (ref_0.n_name = ref_1.s_name)
    WHERE
        ref_1.s_phone IS NULL
    LIMIT 133) AS subq_0
WHERE
    EXISTS (
        SELECT
            ref_4.r_regionkey AS c0, subq_0.c7 AS c1, ref_3.n_comment AS c2, subq_0.c5 AS c3, ref_4.r_name AS c4
        FROM
            main.nation AS ref_3
            INNER JOIN main.region AS ref_4 ON ((1)
                    AND (ref_3.n_name IS NULL))
        WHERE
            0
        LIMIT 43)
LIMIT 148
