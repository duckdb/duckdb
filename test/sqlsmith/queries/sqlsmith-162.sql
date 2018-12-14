SELECT
    subq_1.c1 AS c0,
    subq_1.c1 AS c1,
    subq_1.c1 AS c2,
    subq_1.c1 AS c3,
    subq_1.c1 AS c4,
    CASE WHEN ((EXISTS (
                    SELECT
                        ref_3.r_regionkey AS c0,
                        ref_3.r_regionkey AS c1
                    FROM
                        main.region AS ref_3
                    WHERE (((EXISTS (
                                    SELECT
                                        ref_4.r_comment AS c0, ref_3.r_comment AS c1
                                    FROM
                                        main.region AS ref_4
                                    WHERE (0)
                                    AND ((ref_3.r_comment IS NOT NULL)
                                        OR (subq_1.c0 IS NOT NULL))
                                LIMIT 166))
                        AND ((EXISTS (
                                    SELECT
                                        ref_5.s_address AS c0,
                                        (
                                            SELECT
                                                s_acctbal
                                            FROM
                                                main.supplier
                                            LIMIT 1 offset 2) AS c1,
                                        subq_1.c1 AS c2,
                                        (
                                            SELECT
                                                ps_supplycost
                                            FROM
                                                main.partsupp
                                            LIMIT 1 offset 93) AS c3,
                                        subq_1.c2 AS c4,
                                        ref_3.r_name AS c5,
                                        ref_5.s_nationkey AS c6,
                                        ref_5.s_address AS c7,
                                        ref_5.s_suppkey AS c8,
                                        ref_5.s_phone AS c9,
                                        subq_1.c2 AS c10,
                                        ref_5.s_suppkey AS c11
                                    FROM
                                        main.supplier AS ref_5
                                    WHERE (ref_5.s_phone IS NOT NULL)
                                    AND (((1)
                                            OR ((((EXISTS (
                                                                SELECT
                                                                    32 AS c0, ref_5.s_nationkey AS c1, ref_6.c_name AS c2, 7 AS c3, ref_3.r_comment AS c4, subq_1.c2 AS c5, ref_3.r_comment AS c6, ref_6.c_comment AS c7, ref_5.s_address AS c8
                                                                FROM
                                                                    main.customer AS ref_6
                                                                WHERE
                                                                    0
                                                                LIMIT 40))
                                                        OR (((0)
                                                                OR ((0)
                                                                    OR (1)))
                                                            OR (0)))
                                                    AND ((((1)
                                                                OR (1))
                                                            OR (0))
                                                        AND (ref_5.s_address IS NULL)))
                                                AND ((0)
                                                    OR (subq_1.c0 IS NULL))))
                                        AND (1))
                                LIMIT 89))
                        OR (subq_1.c1 IS NOT NULL)))
                AND (1))
            AND ((EXISTS (
                        SELECT
                            ref_7.n_name AS c0,
                            (
                                SELECT
                                    l_partkey
                                FROM
                                    main.lineitem
                                LIMIT 1 offset 6) AS c1,
                            ref_7.n_name AS c2,
                            subq_1.c2 AS c3,
                            subq_1.c0 AS c4,
                            ref_3.r_name AS c5,
                            ref_3.r_name AS c6,
                            ref_7.n_name AS c7,
                            subq_1.c0 AS c8
                        FROM
                            main.nation AS ref_7
                        WHERE (1)
                        OR (ref_3.r_regionkey IS NULL)))
                OR ((1)
                    AND (((
                                SELECT
                                    l_shipmode
                                FROM
                                    main.lineitem
                                LIMIT 1 offset 4)
                            IS NULL)
                        OR (EXISTS (
                                SELECT
                                    ref_8.ps_supplycost AS c0,
                                    ref_3.r_comment AS c1,
                                    2 AS c2,
                                    ref_3.r_comment AS c3,
                                    ref_8.ps_comment AS c4,
                                    subq_1.c0 AS c5,
                                    subq_1.c0 AS c6,
                                    subq_1.c0 AS c7
                                FROM
                                    main.partsupp AS ref_8
                                WHERE ((((0)
                                            AND (0))
                                        AND (1))
                                    AND (ref_8.ps_supplycost IS NULL))
                                OR ((ref_8.ps_availqty IS NULL)
                                    AND (0))
                            LIMIT 101)))))
    LIMIT 127))
OR (49 IS NULL))
OR (0) THEN
subq_1.c0
ELSE
    subq_1.c0
END AS c5,
subq_1.c2 AS c6,
subq_1.c1 AS c7,
subq_1.c2 AS c8,
subq_1.c1 AS c9
FROM (
    SELECT
        ref_2.r_name AS c0,
        CASE WHEN (ref_1.l_suppkey IS NOT NULL)
            OR (0) THEN
            ref_2.r_regionkey
        ELSE
            ref_2.r_regionkey
        END AS c1,
        59 AS c2
    FROM (
        SELECT
            ref_0.ps_comment AS c0,
            ref_0.ps_partkey AS c1,
            ref_0.ps_availqty AS c2,
            ref_0.ps_partkey AS c3,
            ref_0.ps_supplycost AS c4
        FROM
            main.partsupp AS ref_0
        WHERE
            0
        LIMIT 129) AS subq_0
    LEFT JOIN main.lineitem AS ref_1
    RIGHT JOIN main.region AS ref_2 ON (ref_1.l_comment IS NOT NULL) ON (subq_0.c0 = ref_1.l_returnflag)
WHERE
    subq_0.c1 IS NULL
LIMIT 116) AS subq_1
WHERE (subq_1.c2 IS NULL)
OR (0)
LIMIT 140
