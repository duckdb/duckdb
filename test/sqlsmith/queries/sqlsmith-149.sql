SELECT
    ref_0.s_phone AS c0
FROM
    main.supplier AS ref_0
    LEFT JOIN (
        SELECT
            ref_2.n_nationkey AS c0,
            ref_1.n_regionkey AS c1,
            ref_3.s_name AS c2,
            ref_3.s_nationkey AS c3,
            ref_3.s_comment AS c4
        FROM
            main.nation AS ref_1
            LEFT JOIN main.nation AS ref_2
            INNER JOIN main.supplier AS ref_3 ON ((ref_2.n_comment IS NULL)
                    OR ((9 IS NULL)
                        OR (0))) ON (ref_1.n_nationkey = ref_3.s_suppkey)
        WHERE ((((0)
                    AND (EXISTS (
                            SELECT
                                ref_3.s_suppkey AS c0, ref_2.n_regionkey AS c1, ref_2.n_nationkey AS c2
                            FROM
                                main.partsupp AS ref_4
                            WHERE ((ref_3.s_name IS NULL)
                                OR ((EXISTS (
                                            SELECT
                                                ref_3.s_suppkey AS c0, ref_2.n_comment AS c1, ref_1.n_name AS c2, ref_1.n_name AS c3, ref_5.n_comment AS c4, 40 AS c5, ref_2.n_comment AS c6
                                            FROM
                                                main.nation AS ref_5
                                            WHERE (ref_3.s_nationkey IS NULL)
                                            AND (EXISTS (
                                                    SELECT
                                                        ref_4.ps_suppkey AS c0
                                                    FROM
                                                        main.orders AS ref_6
                                                    WHERE
                                                        ref_6.o_orderkey IS NOT NULL
                                                    LIMIT 27))
                                        LIMIT 79))
                                AND (ref_4.ps_comment IS NOT NULL)))
                        OR (0))))
            OR (((0)
                    AND ((((1)
                                OR (EXISTS (
                                        SELECT
                                            ref_3.s_acctbal AS c0
                                        FROM
                                            main.supplier AS ref_7
                                        WHERE
                                            0)))
                                OR ((ref_2.n_regionkey IS NULL)
                                    OR (ref_3.s_suppkey IS NOT NULL)))
                            AND ((ref_1.n_regionkey IS NOT NULL)
                                AND (((ref_2.n_comment IS NOT NULL)
                                        AND (ref_3.s_nationkey IS NOT NULL))
                                    AND ((1)
                                        OR (((((1)
                                                        OR (0))
                                                    AND (1))
                                                AND (1))
                                            AND (((((ref_3.s_suppkey IS NULL)
                                                            AND (((1)
                                                                    AND ((0)
                                                                        AND (0)))
                                                                OR (((1)
                                                                        AND (((((0)
                                                                                        OR ((ref_3.s_suppkey IS NOT NULL)
                                                                                            OR (0)))
                                                                                    AND (ref_1.n_regionkey IS NULL))
                                                                                OR (((EXISTS (
                                                                                                SELECT
                                                                                                    ref_3.s_phone AS c0
                                                                                                FROM
                                                                                                    main.region AS ref_8
                                                                                                WHERE
                                                                                                    ref_8.r_name IS NULL
                                                                                                LIMIT 162))
                                                                                        OR (0))
                                                                                    AND ((ref_2.n_comment IS NULL)
                                                                                        OR ((1)
                                                                                            AND (1)))))
                                                                            AND (((ref_3.s_acctbal IS NOT NULL)
                                                                                    OR (EXISTS (
                                                                                            SELECT
                                                                                                ref_3.s_acctbal AS c0,
                                                                                                ref_9.s_comment AS c1,
                                                                                                ref_3.s_acctbal AS c2,
                                                                                                ref_9.s_phone AS c3,
                                                                                                ref_1.n_nationkey AS c4,
                                                                                                ref_3.s_name AS c5,
                                                                                                ref_3.s_address AS c6,
                                                                                                ref_3.s_phone AS c7,
                                                                                                ref_3.s_name AS c8,
                                                                                                ref_1.n_regionkey AS c9
                                                                                            FROM
                                                                                                main.supplier AS ref_9
                                                                                            WHERE
                                                                                                EXISTS (
                                                                                                    SELECT
                                                                                                        ref_2.n_comment AS c0, ref_9.s_comment AS c1, ref_9.s_nationkey AS c2, ref_2.n_name AS c3, ref_9.s_name AS c4, ref_1.n_regionkey AS c5
                                                                                                    FROM
                                                                                                        main.customer AS ref_10
                                                                                                    WHERE
                                                                                                        0
                                                                                                    LIMIT 91))))
                                                                                    AND (1))))
                                                                        AND (1))))
                                                            OR (ref_1.n_comment IS NULL))
                                                        OR ((0)
                                                            AND ((1)
                                                                AND (EXISTS (
                                                                        SELECT
                                                                            ref_11.o_totalprice AS c0
                                                                        FROM
                                                                            main.orders AS ref_11
                                                                        WHERE
                                                                            0
                                                                        LIMIT 136)))))
                                                    OR (EXISTS (
                                                            SELECT
                                                                ref_2.n_name AS c0,
                                                                ref_3.s_suppkey AS c1,
                                                                ref_3.s_nationkey AS c2,
                                                                ref_1.n_regionkey AS c3,
                                                                ref_12.s_comment AS c4,
                                                                ref_2.n_name AS c5,
                                                                ref_2.n_comment AS c6,
                                                                ref_1.n_nationkey AS c7,
                                                                ref_12.s_comment AS c8,
                                                                ref_2.n_name AS c9,
                                                                ref_2.n_regionkey AS c10,
                                                                ref_2.n_regionkey AS c11
                                                            FROM
                                                                main.supplier AS ref_12
                                                            WHERE
                                                                ref_12.s_acctbal IS NOT NULL
                                                            LIMIT 139)))))))))
                        AND (((ref_3.s_acctbal IS NULL)
                                AND (ref_1.n_regionkey IS NULL))
                            OR ((0)
                                OR (0)))))
                AND (0))
            OR (1)
        LIMIT 124) AS subq_0 ON (ref_0.s_nationkey IS NOT NULL)
WHERE ((((subq_0.c2 IS NULL)
            OR (((subq_0.c1 IS NOT NULL)
                    AND (0))
                AND ((subq_0.c2 IS NULL)
                    OR (subq_0.c1 IS NOT NULL))))
        AND (((((ref_0.s_nationkey IS NOT NULL)
                        OR (EXISTS (
                                SELECT
                                    subq_0.c2 AS c0, ref_13.s_comment AS c1
                                FROM
                                    main.supplier AS ref_13
                                WHERE
                                    1
                                LIMIT 129)))
                    OR ((ref_0.s_comment IS NOT NULL)
                        OR (0)))
                OR (((1)
                        OR (1))
                    OR ((subq_0.c4 IS NOT NULL)
                        AND ((1)
                            OR (0)))))
            AND (1)))
    OR (0))
OR ((((((ref_0.s_nationkey IS NULL)
                    AND ((1)
                        OR (((0)
                                AND (26 IS NULL))
                            OR ((ref_0.s_nationkey IS NOT NULL)
                                OR ((ref_0.s_name IS NULL)
                                    AND (1))))))
                AND (((subq_0.c1 IS NULL)
                        AND ((79 IS NULL)
                            AND ((subq_0.c1 IS NULL)
                                AND (ref_0.s_suppkey IS NULL))))
                    OR (1)))
            AND ((((ref_0.s_comment IS NULL)
                        OR (1))
                    OR (0))
                AND ((((1)
                            OR (1))
                        OR (EXISTS (
                                SELECT
                                    subq_0.c2 AS c0,
                                    ref_14.l_tax AS c1,
                                    ref_0.s_nationkey AS c2,
                                    ref_0.s_comment AS c3,
                                    ref_14.l_extendedprice AS c4
                                FROM
                                    main.lineitem AS ref_14
                                WHERE
                                    1)))
                        AND (ref_0.s_acctbal IS NULL))))
            OR ((subq_0.c0 IS NULL)
                OR (subq_0.c1 IS NULL)))
        AND ((subq_0.c0 IS NULL)
            AND (ref_0.s_name IS NULL)))
LIMIT 88
