SELECT
    subq_1.c3 AS c0,
    subq_1.c2 AS c1,
    CASE WHEN ((EXISTS (
                    SELECT
                        ref_5.l_shipinstruct AS c0,
                        ref_5.l_shipinstruct AS c1,
                        ref_5.l_returnflag AS c2,
                        ref_4.r_regionkey AS c3,
                        ref_5.l_comment AS c4,
                        ref_5.l_extendedprice AS c5,
                        ref_5.l_suppkey AS c6
                    FROM
                        main.region AS ref_4
                        INNER JOIN main.lineitem AS ref_5 ON (67 IS NULL)
                    WHERE
                        ref_4.r_name IS NULL))
                AND (((((EXISTS (
                                        SELECT
                                            67 AS c0, subq_1.c2 AS c1
                                        FROM
                                            main.lineitem AS ref_6
                                        WHERE
                                            1
                                        LIMIT 125))
                                OR (subq_1.c0 IS NOT NULL))
                            OR (subq_1.c3 IS NULL))
                        OR (0))
                    OR (subq_1.c3 IS NULL)))
            AND (1) THEN
            subq_1.c1
        ELSE
            subq_1.c1
        END AS c2,
        CASE WHEN (EXISTS (
                    SELECT
                        ref_7.ps_suppkey AS c0,
                        ref_7.ps_comment AS c1,
                        32 AS c2,
                        ref_7.ps_supplycost AS c3,
                        subq_1.c4 AS c4,
                        ref_7.ps_comment AS c5,
                        subq_1.c0 AS c6,
                        subq_1.c3 AS c7
                    FROM
                        main.partsupp AS ref_7
                    WHERE
                        subq_1.c4 IS NULL))
                AND ((1)
                    OR (subq_1.c3 IS NULL)) THEN
                subq_1.c0
            ELSE
                subq_1.c0
            END AS c3, subq_1.c0 AS c4, subq_1.c3 AS c5, subq_1.c2 AS c6, subq_1.c4 AS c7, subq_1.c2 AS c8, 57 AS c9, CASE WHEN ((subq_1.c1 IS NOT NULL)
                    OR (subq_1.c0 IS NOT NULL))
                OR ((((EXISTS (
                                    SELECT
                                        subq_1.c0 AS c0, subq_1.c1 AS c1, ref_8.s_suppkey AS c2, subq_1.c4 AS c3, subq_1.c3 AS c4, ref_8.s_suppkey AS c5, subq_1.c0 AS c6, subq_1.c2 AS c7, subq_1.c3 AS c8
                                    FROM
                                        main.supplier AS ref_8
                                    WHERE
                                        subq_1.c3 IS NULL
                                    LIMIT 80))
                            AND (EXISTS (
                                    SELECT
                                        ref_9.p_comment AS c0
                                    FROM
                                        main.part AS ref_9
                                    WHERE
                                        0)))
                            OR (1))
                        AND ((1)
                            OR ((1)
                                OR (((subq_1.c0 IS NOT NULL)
                                        OR (0))
                                    OR (subq_1.c4 IS NULL))))) THEN
                    subq_1.c0
                ELSE
                    subq_1.c0
                END AS c10, subq_1.c1 AS c11, CASE WHEN (subq_1.c0 IS NULL)
                    AND (0) THEN
                    subq_1.c2
                ELSE
                    subq_1.c2
                END AS c12, CAST(coalesce(12, subq_1.c0) AS INTEGER) AS c13, subq_1.c3 AS c14, subq_1.c2 AS c15, CASE WHEN (((55 IS NULL)
                            OR ((subq_1.c3 IS NULL)
                                AND (subq_1.c3 IS NOT NULL)))
                        AND (EXISTS (
                                SELECT
                                    subq_1.c3 AS c0, 27 AS c1, subq_1.c1 AS c2, subq_1.c1 AS c3, ref_13.o_clerk AS c4, subq_1.c2 AS c5, ref_13.o_comment AS c6
                                FROM
                                    main.orders AS ref_13
                                WHERE
                                    0
                                LIMIT 148)))
                    OR ((((((1)
                                        AND (0))
                                    AND (subq_1.c4 IS NULL))
                                OR ((0)
                                    OR ((((((EXISTS (
                                                                SELECT
                                                                    ref_14.n_regionkey AS c0
                                                                FROM
                                                                    main.nation AS ref_14
                                                                WHERE (1)
                                                                AND ((1)
                                                                    OR (((0)
                                                                            AND (1))
                                                                        OR (1)))
                                                            LIMIT 117))
                                                    AND (EXISTS (
                                                            SELECT
                                                                ref_15.r_name AS c0,
                                                                subq_1.c0 AS c1,
                                                                ref_15.r_name AS c2,
                                                                ref_15.r_comment AS c3,
                                                                subq_1.c0 AS c4,
                                                                ref_15.r_comment AS c5,
                                                                ref_15.r_comment AS c6,
                                                                subq_1.c0 AS c7,
                                                                subq_1.c2 AS c8,
                                                                ref_15.r_name AS c9,
                                                                subq_1.c2 AS c10,
                                                                ref_15.r_comment AS c11,
                                                                subq_1.c3 AS c12,
                                                                subq_1.c0 AS c13,
                                                                ref_15.r_name AS c14,
                                                                subq_1.c2 AS c15,
                                                                subq_1.c1 AS c16,
                                                                subq_1.c2 AS c17,
                                                                ref_15.r_regionkey AS c18
                                                            FROM
                                                                main.region AS ref_15
                                                            WHERE
                                                                EXISTS (
                                                                    SELECT
                                                                        ref_16.o_comment AS c0, subq_1.c0 AS c1, ref_15.r_regionkey AS c2, subq_1.c1 AS c3, subq_1.c1 AS c4, ref_16.o_totalprice AS c5, subq_1.c4 AS c6, subq_1.c4 AS c7, subq_1.c4 AS c8
                                                                    FROM
                                                                        main.orders AS ref_16
                                                                    WHERE
                                                                        1
                                                                    LIMIT 116)
                                                            LIMIT 134)))
                                                AND (subq_1.c2 IS NULL))
                                            OR (((0)
                                                    OR (1))
                                                AND (((((((1)
                                                                        AND ((EXISTS (
                                                                                    SELECT
                                                                                        subq_1.c4 AS c0,
                                                                                        subq_1.c1 AS c1,
                                                                                        subq_1.c1 AS c2,
                                                                                        ref_17.ps_suppkey AS c3
                                                                                    FROM
                                                                                        main.partsupp AS ref_17
                                                                                    WHERE
                                                                                        EXISTS (
                                                                                            SELECT
                                                                                                ref_17.ps_partkey AS c0, ref_18.r_regionkey AS c1, ref_17.ps_supplycost AS c2, ref_18.r_name AS c3, 89 AS c4
                                                                                            FROM
                                                                                                main.region AS ref_18
                                                                                            WHERE
                                                                                                1
                                                                                            LIMIT 100)
                                                                                    LIMIT 99))
                                                                            AND (EXISTS (
                                                                                    SELECT
                                                                                        ref_19.n_nationkey AS c0,
                                                                                        subq_1.c1 AS c1,
                                                                                        ref_19.n_name AS c2,
                                                                                        ref_19.n_comment AS c3,
                                                                                        ref_19.n_comment AS c4,
                                                                                        subq_1.c3 AS c5,
                                                                                        subq_1.c1 AS c6,
                                                                                        ref_19.n_comment AS c7,
                                                                                        subq_1.c0 AS c8,
                                                                                        subq_1.c3 AS c9,
                                                                                        ref_19.n_nationkey AS c10,
                                                                                        ref_19.n_nationkey AS c11,
                                                                                        subq_1.c1 AS c12,
                                                                                        ref_19.n_comment AS c13
                                                                                    FROM
                                                                                        main.nation AS ref_19
                                                                                    WHERE
                                                                                        1))))
                                                                        OR (0))
                                                                    AND (subq_1.c4 IS NULL))
                                                                OR ((EXISTS (
                                                                            SELECT
                                                                                DISTINCT subq_1.c0 AS c0, ref_20.o_custkey AS c1, ref_20.o_shippriority AS c2, ref_20.o_totalprice AS c3, ref_20.o_orderstatus AS c4
                                                                            FROM
                                                                                main.orders AS ref_20
                                                                            WHERE
                                                                                0
                                                                            LIMIT 112))
                                                                    AND (1)))
                                                            OR (EXISTS (
                                                                    SELECT
                                                                        subq_1.c3 AS c0,
                                                                        ref_21.ps_partkey AS c1,
                                                                        ref_21.ps_comment AS c2,
                                                                        ref_21.ps_availqty AS c3,
                                                                        subq_1.c1 AS c4
                                                                    FROM
                                                                        main.partsupp AS ref_21
                                                                    WHERE
                                                                        1
                                                                    LIMIT 58)))
                                                        AND (0))))
                                            OR (((subq_1.c0 IS NOT NULL)
                                                    AND (EXISTS (
                                                            SELECT
                                                                ref_22.ps_availqty AS c0,
                                                                subq_1.c4 AS c1,
                                                                subq_1.c3 AS c2,
                                                                subq_1.c1 AS c3,
                                                                ref_22.ps_availqty AS c4,
                                                                ref_22.ps_partkey AS c5,
                                                                ref_22.ps_supplycost AS c6,
                                                                ref_22.ps_partkey AS c7,
                                                                ref_22.ps_supplycost AS c8,
                                                                subq_1.c0 AS c9,
                                                                subq_1.c0 AS c10,
                                                                subq_1.c1 AS c11,
                                                                subq_1.c0 AS c12,
                                                                subq_1.c0 AS c13,
                                                                ref_22.ps_suppkey AS c14,
                                                                ref_22.ps_supplycost AS c15,
                                                                ref_22.ps_supplycost AS c16,
                                                                ref_22.ps_suppkey AS c17,
                                                                subq_1.c2 AS c18,
                                                                subq_1.c2 AS c19,
                                                                ref_22.ps_supplycost AS c20,
                                                                ref_22.ps_partkey AS c21,
                                                                subq_1.c2 AS c22,
                                                                ref_22.ps_partkey AS c23,
                                                                98 AS c24,
                                                                subq_1.c1 AS c25
                                                            FROM
                                                                main.partsupp AS ref_22
                                                            WHERE
                                                                EXISTS (
                                                                    SELECT
                                                                        ref_22.ps_suppkey AS c0, ref_22.ps_suppkey AS c1, ref_22.ps_comment AS c2
                                                                    FROM
                                                                        main.region AS ref_23
                                                                    WHERE ((EXISTS (
                                                                                SELECT
                                                                                    ref_22.ps_supplycost AS c0, ref_22.ps_suppkey AS c1
                                                                                FROM
                                                                                    main.lineitem AS ref_24
                                                                                WHERE ((ref_24.l_tax IS NULL)
                                                                                    OR ((0)
                                                                                        OR (ref_24.l_orderkey IS NOT NULL)))
                                                                                OR (ref_23.r_regionkey IS NOT NULL)))
                                                                        AND (ref_23.r_regionkey IS NOT NULL))
                                                                    OR (1)
                                                                LIMIT 184)
                                                        LIMIT 65)))
                                            OR (0)))
                                    AND (subq_1.c3 IS NOT NULL))))
                        AND (1))
                    AND ((0)
                        AND (EXISTS (
                                SELECT
                                    subq_1.c3 AS c0
                                FROM
                                    main.customer AS ref_25
                                WHERE
                                    ref_25.c_phone IS NOT NULL
                                LIMIT 27)))) THEN
                subq_1.c1
            ELSE
                subq_1.c1
            END AS c16,
            subq_1.c1 AS c17,
            subq_1.c0 AS c18,
            subq_1.c0 AS c19,
            91 AS c20,
            subq_1.c3 AS c21,
            subq_1.c2 AS c22,
            CASE WHEN 1 THEN
                subq_1.c1
            ELSE
                subq_1.c1
            END AS c23,
            subq_1.c0 AS c24
        FROM (
            SELECT
                subq_0.c2 AS c0,
                ref_1.o_clerk AS c1,
                ref_3.o_totalprice AS c2,
                ref_3.o_orderpriority AS c3,
                ref_3.o_comment AS c4
            FROM (
                SELECT
                    ref_0.r_comment AS c0,
                    ref_0.r_comment AS c1,
                    ref_0.r_regionkey AS c2,
                    ref_0.r_regionkey AS c3
                FROM
                    main.region AS ref_0
                WHERE
                    ref_0.r_name IS NOT NULL
                LIMIT 161) AS subq_0
            INNER JOIN main.orders AS ref_1
            INNER JOIN main.partsupp AS ref_2 ON (ref_1.o_orderkey = ref_2.ps_partkey)
            INNER JOIN main.orders AS ref_3 ON (ref_3.o_custkey IS NOT NULL) ON (subq_0.c0 = ref_2.ps_comment)
        WHERE (ref_3.o_orderpriority IS NULL)
        OR (ref_1.o_shippriority IS NULL)
    LIMIT 98) AS subq_1
    WHERE
        1
    LIMIT 188
