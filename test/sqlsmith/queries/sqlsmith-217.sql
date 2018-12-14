SELECT
    subq_0.c7 AS c0,
    subq_0.c1 AS c1,
    subq_0.c1 AS c2,
    subq_0.c6 AS c3,
    subq_0.c1 AS c4,
    subq_0.c4 AS c5,
    subq_0.c6 AS c6,
    CASE WHEN 0 THEN
        subq_0.c0
    ELSE
        subq_0.c0
    END AS c7,
    subq_0.c4 AS c8,
    subq_0.c3 AS c9,
    subq_0.c5 AS c10,
    subq_0.c3 AS c11
FROM (
    SELECT
        ref_0.c_name AS c0,
        ref_1.c_comment AS c1,
        ref_2.r_comment AS c2,
        ref_2.r_regionkey AS c3,
        ref_2.r_comment AS c4,
        ref_1.c_comment AS c5,
        ref_1.c_custkey AS c6,
        ref_0.c_name AS c7
    FROM
        main.customer AS ref_0
    LEFT JOIN main.customer AS ref_1
    INNER JOIN main.region AS ref_2 ON (ref_1.c_custkey = ref_2.r_regionkey)
    LEFT JOIN main.supplier AS ref_3 ON (ref_2.r_name = ref_3.s_name) ON (ref_0.c_custkey = ref_1.c_custkey)
WHERE
    ref_3.s_name IS NOT NULL
LIMIT 104) AS subq_0
WHERE ((subq_0.c7 IS NOT NULL)
    AND (((EXISTS (
                    SELECT
                        ref_4.p_size AS c0, subq_0.c1 AS c1, subq_0.c1 AS c2
                    FROM
                        main.part AS ref_4
                    WHERE
                        1
                    LIMIT 156))
            AND ((((subq_0.c4 IS NULL)
                        OR (subq_0.c2 IS NOT NULL))
                    OR (0))
                AND (1)))
        OR ((1)
            OR (1))))
OR ((11 IS NOT NULL)
    AND ((subq_0.c0 IS NOT NULL)
        OR (EXISTS (
                SELECT
                    ref_5.n_name AS c0,
                    subq_0.c0 AS c1,
                    ref_5.n_regionkey AS c2
                FROM
                    main.nation AS ref_5
                WHERE
                    0
                LIMIT 94))))
LIMIT 107
