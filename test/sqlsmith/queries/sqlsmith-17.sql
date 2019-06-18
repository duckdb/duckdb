SELECT
    subq_1.c0 AS c0,
    subq_1.c6 AS c1,
    CASE WHEN (EXISTS (
            SELECT
                subq_1.c8 AS c0,
                subq_1.c9 AS c1,
                subq_1.c3 AS c2
            FROM
                main.nation AS ref_7
            WHERE
                0))
        AND (1) THEN
        subq_1.c0
    ELSE
        subq_1.c0
    END AS c2,
    subq_1.c4 AS c3,
    subq_1.c6 AS c4,
    subq_1.c4 AS c5,
    subq_1.c8 AS c6
FROM (
    SELECT
        12 AS c0,
        subq_0.c9 AS c1,
        subq_0.c8 AS c2,
        (
            SELECT
                c_comment
            FROM
                main.customer
            LIMIT 1 offset 21) AS c3,
        subq_0.c2 AS c4,
        CASE WHEN (subq_0.c1 IS NULL)
            AND ((0)
                AND (1)) THEN
            subq_0.c7
        ELSE
            subq_0.c7
        END AS c5,
        subq_0.c6 AS c6,
        subq_0.c7 AS c7,
        63 AS c8,
        subq_0.c1 AS c9
    FROM (
        SELECT
            ref_1.r_name AS c0,
            ref_1.r_name AS c1,
            ref_0.r_name AS c2,
            ref_1.r_regionkey AS c3,
            ref_1.r_name AS c4,
            ref_1.r_name AS c5,
            ref_1.r_name AS c6,
            ref_1.r_regionkey AS c7,
            ref_0.r_comment AS c8,
            ref_0.r_name AS c9
        FROM
            main.region AS ref_0
        LEFT JOIN main.region AS ref_1 ON (((0)
                    OR ((0)
                        AND (0)))
                OR (0))
    WHERE (EXISTS (
            SELECT
                ref_0.r_regionkey AS c0
            FROM
                main.lineitem AS ref_2
            WHERE
                EXISTS (
                    SELECT
                        ref_0.r_comment AS c0, ref_2.l_linestatus AS c1, ref_1.r_name AS c2, ref_0.r_regionkey AS c3, ref_0.r_regionkey AS c4, ref_2.l_partkey AS c5, ref_2.l_orderkey AS c6, (
                            SELECT
                                s_name
                            FROM
                                main.supplier
                            LIMIT 1 offset 6) AS c7,
                        ref_2.l_returnflag AS c8,
                        ref_3.ps_suppkey AS c9,
                        (
                            SELECT
                                o_totalprice
                            FROM
                                main.orders
                            LIMIT 1 offset 1) AS c10,
                        ref_1.r_comment AS c11
                    FROM
                        main.partsupp AS ref_3
                    WHERE
                        EXISTS (
                            SELECT
                                ref_1.r_comment AS c0, ref_4.ps_partkey AS c1, ref_3.ps_suppkey AS c2, ref_4.ps_supplycost AS c3
                            FROM
                                main.partsupp AS ref_4
                            WHERE
                                EXISTS (
                                    SELECT
                                        ref_3.ps_supplycost AS c0, ref_4.ps_comment AS c1, (
                                            SELECT
                                                p_partkey
                                            FROM
                                                main.part
                                            LIMIT 1 offset 4) AS c2,
                                        ref_5.l_comment AS c3,
                                        ref_5.l_linestatus AS c4
                                    FROM
                                        main.lineitem AS ref_5
                                    WHERE
                                        ref_1.r_regionkey IS NULL)
                                LIMIT 111)
                        LIMIT 60)
                LIMIT 145))
        AND (0)) AS subq_0
WHERE (subq_0.c5 IS NULL)
    OR (EXISTS (
            SELECT
                subq_0.c5 AS c0, 12 AS c1, ref_6.r_name AS c2, ref_6.r_comment AS c3, subq_0.c9 AS c4, ref_6.r_name AS c5, subq_0.c2 AS c6, subq_0.c0 AS c7
            FROM
                main.region AS ref_6
            WHERE
                1))) AS subq_1
WHERE (((subq_1.c2 IS NOT NULL)
        AND (((1)
                OR (subq_1.c7 IS NULL))
            OR ((((subq_1.c4 IS NULL)
                        AND ((subq_1.c1 IS NULL)
                            OR (subq_1.c2 IS NOT NULL)))
                    AND (((((subq_1.c6 IS NOT NULL)
                                    OR ((0)
                                        OR ((1)
                                            OR (EXISTS (
                                                    SELECT
                                                        ref_8.r_comment AS c0, subq_1.c2 AS c1, ref_8.r_name AS c2, ref_8.r_name AS c3, subq_1.c9 AS c4, subq_1.c1 AS c5, subq_1.c3 AS c6, subq_1.c9 AS c7, subq_1.c8 AS c8
                                                    FROM
                                                        main.region AS ref_8
                                                    WHERE (subq_1.c9 IS NULL)
                                                    AND ((1)
                                                        AND (0))
                                                LIMIT 94)))))
                            AND ((subq_1.c9 IS NOT NULL)
                                OR ((((EXISTS (
                                                    SELECT
                                                        ref_9.c_nationkey AS c0,
                                                        ref_9.c_name AS c1
                                                    FROM
                                                        main.customer AS ref_9
                                                    WHERE
                                                        0
                                                    LIMIT 157))
                                            OR (((0)
                                                    OR ((1)
                                                        OR (subq_1.c6 IS NULL)))
                                                AND (0)))
                                        AND ((EXISTS (
                                                    SELECT
                                                        ref_10.p_brand AS c0
                                                    FROM
                                                        main.part AS ref_10
                                                    WHERE
                                                        1))
                                                AND (subq_1.c2 IS NULL)))
                                        OR (1))))
                            OR (((EXISTS (
                                            SELECT
                                                subq_1.c8 AS c0, subq_1.c2 AS c1, subq_1.c7 AS c2, subq_1.c2 AS c3, ref_11.ps_comment AS c4, subq_1.c9 AS c5, subq_1.c4 AS c6, 43 AS c7, subq_1.c8 AS c8, subq_1.c4 AS c9, ref_11.ps_suppkey AS c10
                                            FROM
                                                main.partsupp AS ref_11
                                            WHERE
                                                1))
                                        OR ((((((1)
                                                            AND (1))
                                                        OR (EXISTS (
                                                                SELECT
                                                                    ref_12.r_comment AS c0
                                                                FROM
                                                                    main.region AS ref_12
                                                                WHERE
                                                                    0
                                                                LIMIT 74)))
                                                    AND (0))
                                                OR (((((1)
                                                                OR (EXISTS (
                                                                        SELECT
                                                                            ref_13.n_regionkey AS c0,
                                                                            ref_13.n_name AS c1,
                                                                            subq_1.c4 AS c2
                                                                        FROM
                                                                            main.nation AS ref_13
                                                                        WHERE
                                                                            subq_1.c3 IS NOT NULL
                                                                        LIMIT 96)))
                                                            AND ((0)
                                                                OR ((subq_1.c3 IS NULL)
                                                                    OR (0))))
                                                        AND (0))
                                                    OR ((1)
                                                        OR ((((subq_1.c1 IS NOT NULL)
                                                                    OR (1))
                                                                OR ((
                                                                        SELECT
                                                                            c_address
                                                                        FROM
                                                                            main.customer
                                                                        LIMIT 1 offset 5) IS NULL))
                                                            AND (1)))))
                                            OR (subq_1.c2 IS NULL)))
                                    OR (0)))
                            AND (0)))
                    AND ((0)
                        AND ((((subq_1.c8 IS NOT NULL)
                                    OR (((subq_1.c2 IS NULL)
                                            AND ((EXISTS (
                                                        SELECT
                                                            ref_14.r_regionkey AS c0,
                                                            subq_1.c6 AS c1,
                                                            ref_14.r_name AS c2,
                                                            subq_1.c7 AS c3
                                                        FROM
                                                            main.region AS ref_14
                                                        WHERE
                                                            16 IS NOT NULL))
                                                    AND (0)))
                                            OR (subq_1.c2 IS NOT NULL)))
                                    AND ((subq_1.c1 IS NOT NULL)
                                        OR (((EXISTS (
                                                        SELECT
                                                            subq_1.c4 AS c0
                                                        FROM
                                                            main.region AS ref_15
                                                        WHERE
                                                            subq_1.c2 IS NULL))
                                                    AND (subq_1.c7 IS NOT NULL))
                                                AND ((subq_1.c3 IS NOT NULL)
                                                    AND ((0)
                                                        OR ((1)
                                                            AND ((1)
                                                                AND (0))))))))
                                    OR (subq_1.c4 IS NULL))))))
                AND ((subq_1.c1 IS NOT NULL)
                    AND (subq_1.c2 IS NULL)))
        OR (((
                    SELECT
                        s_comment
                    FROM
                        main.supplier
                    LIMIT 1 offset 2) IS NOT NULL)
            AND (((EXISTS (
                            SELECT
                                subq_1.c5 AS c0
                            FROM
                                main.nation AS ref_16
                            WHERE
                                0
                            LIMIT 126))
                    AND (((subq_1.c1 IS NOT NULL)
                            OR (0))
                        OR (subq_1.c0 IS NULL)))
                AND (((0)
                        AND ((
                                SELECT
                                    ps_partkey
                                FROM
                                    main.partsupp
                                LIMIT 1 offset 4) IS NULL))
                    AND (((0)
                            AND ((subq_1.c5 IS NULL)
                                AND (0)))
                        AND ((EXISTS (
                                    SELECT
                                        ref_17.s_comment AS c0
                                    FROM
                                        main.supplier AS ref_17
                                    WHERE (ref_17.s_comment IS NULL)
                                    OR ((EXISTS (
                                                SELECT
                                                    subq_1.c8 AS c0, ref_18.l_quantity AS c1, ref_17.s_phone AS c2, subq_1.c1 AS c3, subq_1.c1 AS c4, ref_18.l_partkey AS c5, ref_18.l_linestatus AS c6, subq_1.c2 AS c7, subq_1.c4 AS c8, 78 AS c9, ref_18.l_suppkey AS c10, ref_17.s_address AS c11, subq_1.c5 AS c12, ref_17.s_comment AS c13, subq_1.c5 AS c14
                                                FROM
                                                    main.lineitem AS ref_18
                                                WHERE
                                                    0
                                                LIMIT 8))
                                        AND ((0)
                                            AND (ref_17.s_comment IS NULL)))
                                LIMIT 150))
                        AND (1))))))
LIMIT 112
