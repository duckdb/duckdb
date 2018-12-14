SELECT
    subq_1.c1 AS c0,
    8 AS c1,
    subq_1.c7 AS c2,
    subq_0.c2 AS c3,
    subq_0.c1 AS c4,
    subq_0.c3 AS c5,
    81 AS c6,
    subq_0.c3 AS c7,
    subq_1.c5 AS c8
FROM (
    SELECT
        (
            SELECT
                l_suppkey
            FROM
                main.lineitem
            LIMIT 1 offset 5) AS c0,
        ref_0.r_name AS c1,
        ref_1.n_regionkey AS c2,
        ref_0.r_name AS c3
    FROM
        main.region AS ref_0
    LEFT JOIN main.nation AS ref_1 ON ((1)
            AND (ref_1.n_regionkey IS NOT NULL))
WHERE (1)
OR ((0)
    OR (((((1)
                    AND (((ref_1.n_comment IS NULL)
                            AND (0))
                        AND (ref_0.r_comment IS NOT NULL)))
                OR ((0)
                    OR (0)))
            OR ((((((ref_0.r_regionkey IS NOT NULL)
                                OR (ref_0.r_comment IS NULL))
                            AND ((ref_0.r_comment IS NOT NULL)
                                OR ((((1)
                                            AND ((1)
                                                AND (ref_0.r_name IS NOT NULL)))
                                        OR (ref_1.n_regionkey IS NOT NULL))
                                    AND (1))))
                        OR (1))
                    OR (0))
                OR (17 IS NULL)))
        OR (ref_0.r_comment IS NULL)))) AS subq_0
    LEFT JOIN (
        SELECT
            ref_2.n_comment AS c0, ref_3.r_name AS c1, ref_2.n_comment AS c2, ref_2.n_nationkey AS c3, ref_3.r_comment AS c4, ref_2.n_regionkey AS c5, ref_2.n_comment AS c6, ref_2.n_name AS c7, ref_4.ps_supplycost AS c8, ref_5.l_discount AS c9
        FROM
            main.nation AS ref_2
            INNER JOIN main.region AS ref_3
            LEFT JOIN main.partsupp AS ref_4 ON (ref_4.ps_availqty IS NOT NULL) ON (ref_2.n_nationkey = ref_3.r_regionkey)
            INNER JOIN main.lineitem AS ref_5 ON (ref_5.l_suppkey IS NULL)
        WHERE (
            SELECT
                n_name
            FROM
                main.nation
            LIMIT 1 offset 6)
        IS NULL
    LIMIT 87) AS subq_1 ON (subq_0.c3 = subq_1.c0)
WHERE
    subq_1.c8 IS NOT NULL
LIMIT 7
