INSERT INTO main.supplier
    VALUES (68, DEFAULT, CASE WHEN ((25 IS NULL)
            AND ((((EXISTS (
                                SELECT
                                    (
                                        SELECT
                                            o_comment
                                        FROM
                                            main.orders
                                        LIMIT 1 offset 4) AS c0,
                                    ref_0.s_nationkey AS c1,
                                    ref_0.s_suppkey AS c2,
                                    ref_0.s_comment AS c3,
                                    ref_0.s_suppkey AS c4,
                                    ref_0.s_suppkey AS c5,
                                    ref_0.s_acctbal AS c6,
                                    ref_0.s_name AS c7,
                                    ref_0.s_phone AS c8,
                                    ref_0.s_acctbal AS c9,
                                    ref_0.s_nationkey AS c10,
                                    ref_0.s_suppkey AS c11,
                                    ref_0.s_name AS c12,
                                    ref_0.s_address AS c13
                                FROM
                                    main.supplier AS ref_0
                                WHERE
                                    0
                                LIMIT 51))
                        AND (63 IS NOT NULL))
                    OR (55 IS NOT NULL))
                OR ((((6 IS NOT NULL)
                            OR (8 IS NULL))
                        OR (0))
                    AND (1))))
            AND (EXISTS (
                    SELECT
                        (
                            SELECT
                                s_suppkey
                            FROM
                                main.supplier
                            LIMIT 1 offset 4) AS c0,
                        60 AS c1,
                        ref_1.o_orderpriority AS c2
                    FROM
                        main.orders AS ref_1
                    WHERE (ref_1.o_orderpriority IS NULL)
                    AND ((1)
                        AND ((28 IS NOT NULL)
                            AND (1)))
                LIMIT 98)) THEN
            CAST(NULL AS VARCHAR)
        ELSE
            CAST(NULL AS VARCHAR)
        END,
        1,
        CAST(NULL AS VARCHAR),
        CAST(NULL AS DECIMAL),
        CASE WHEN (EXISTS (
                SELECT
                    (
                        SELECT
                            l_suppkey
                        FROM
                            main.lineitem
                        LIMIT 1 offset 4) AS c0,
                    subq_0.c0 AS c1,
                    64 AS c2,
                    subq_0.c1 AS c3,
                    (
                        SELECT
                            p_retailprice
                        FROM
                            main.part
                        LIMIT 1 offset 6) AS c4,
                    subq_0.c0 AS c5
                FROM (
                    SELECT
                        ref_2.c_nationkey AS c0,
                        ref_2.c_phone AS c1
                    FROM
                        main.customer AS ref_2
                    WHERE
                        1) AS subq_0
                WHERE ((subq_0.c0 IS NULL)
                    OR (1))
                OR (EXISTS (
                        SELECT
                            ref_3.p_name AS c0, subq_0.c0 AS c1
                        FROM
                            main.part AS ref_3
                        WHERE (((ref_3.p_type IS NOT NULL)
                                AND (ref_3.p_type IS NULL))
                            OR (0))
                        AND (((EXISTS (
                                        SELECT
                                            subq_0.c1 AS c0
                                        FROM
                                            main.orders AS ref_4
                                        WHERE (ref_4.o_orderstatus IS NULL)
                                        OR ((ref_4.o_comment IS NOT NULL)
                                            AND ((((1)
                                                        OR (0))
                                                    AND (0))
                                                AND ((0)
                                                    OR ((1)
                                                        AND (1)))))
                                    LIMIT 134))
                            OR (ref_3.p_retailprice IS NOT NULL))
                        AND (0))))))
    AND ((1)
            OR ((((90 IS NOT NULL)
                        OR ((((EXISTS (
                                            SELECT
                                                ref_5.r_name AS c0, ref_5.r_regionkey AS c1, ref_5.r_name AS c2, ref_5.r_name AS c3, ref_5.r_regionkey AS c4, ref_5.r_name AS c5, ref_5.r_regionkey AS c6, ref_5.r_regionkey AS c7
                                            FROM
                                                main.region AS ref_5
                                            WHERE (0)
                                            OR (ref_5.r_name IS NULL)
                                        LIMIT 125))
                                AND ((94 IS NULL)
                                    OR ((((9 IS NOT NULL)
                                                AND (99 IS NULL))
                                            AND (72 IS NULL))
                                        AND (((42 IS NULL)
                                                OR (0))
                                            OR (((((EXISTS (
                                                                    SELECT
                                                                        ref_6.l_receiptdate AS c0,
                                                                        ref_6.l_shipdate AS c1,
                                                                        ref_6.l_discount AS c2,
                                                                        ref_6.l_extendedprice AS c3,
                                                                        ref_6.l_returnflag AS c4,
                                                                        ref_6.l_partkey AS c5,
                                                                        ref_6.l_receiptdate AS c6
                                                                    FROM
                                                                        main.lineitem AS ref_6
                                                                    WHERE (1)
                                                                    AND (1)
                                                                LIMIT 43))
                                                        AND (0))
                                                    OR (((EXISTS (
                                                                    SELECT
                                                                        ref_7.c_mktsegment AS c0,
                                                                        ref_7.c_address AS c1,
                                                                        ref_7.c_comment AS c2,
                                                                        ref_7.c_custkey AS c3
                                                                    FROM
                                                                        main.customer AS ref_7
                                                                    WHERE
                                                                        EXISTS (
                                                                            SELECT
                                                                                ref_8.n_name AS c0, ref_7.c_acctbal AS c1
                                                                            FROM
                                                                                main.nation AS ref_8
                                                                            WHERE
                                                                                EXISTS (
                                                                                    SELECT
                                                                                        ref_9.o_custkey AS c0, ref_9.o_custkey AS c1, ref_8.n_nationkey AS c2
                                                                                    FROM
                                                                                        main.orders AS ref_9
                                                                                    WHERE
                                                                                        1)
                                                                                LIMIT 81)
                                                                        LIMIT 70))
                                                                OR (((EXISTS (
                                                                                SELECT
                                                                                    subq_6.c1 AS c0,
                                                                                    subq_5.c0 AS c1,
                                                                                    ref_10.p_mfgr AS c2,
                                                                                    ref_10.p_comment AS c3,
                                                                                    subq_5.c0 AS c4
                                                                                FROM
                                                                                    main.part AS ref_10,
                                                                                    LATERAL (
                                                                                        SELECT
                                                                                            99 AS c0,
                                                                                            subq_1.c0 AS c1,
                                                                                            ref_10.p_mfgr AS c2,
                                                                                            subq_4.c5 AS c3,
                                                                                            ref_11.ps_partkey AS c4,
                                                                                            51 AS c5
                                                                                        FROM
                                                                                            main.partsupp AS ref_11,
                                                                                            LATERAL (
                                                                                                SELECT
                                                                                                    ref_12.c_address AS c0,
                                                                                                    ref_12.c_address AS c1,
                                                                                                    ref_12.c_address AS c2
                                                                                                FROM
                                                                                                    main.customer AS ref_12
                                                                                                WHERE
                                                                                                    0
                                                                                                LIMIT 70) AS subq_1,
                                                                                            LATERAL (
                                                                                                SELECT
                                                                                                    2 AS c0,
                                                                                                    ref_13.s_suppkey AS c1,
                                                                                                    27 AS c2,
                                                                                                    subq_1.c1 AS c3,
                                                                                                    ref_11.ps_availqty AS c4,
                                                                                                    ref_10.p_retailprice AS c5,
                                                                                                    ref_10.p_comment AS c6,
                                                                                                    subq_1.c1 AS c7,
                                                                                                    ref_13.s_nationkey AS c8,
                                                                                                    ref_11.ps_availqty AS c9,
                                                                                                    subq_1.c0 AS c10,
                                                                                                    subq_3.c2 AS c11
                                                                                                FROM
                                                                                                    main.supplier AS ref_13,
                                                                                                    LATERAL (
                                                                                                        SELECT
                                                                                                            ref_10.p_container AS c0,
                                                                                                            subq_1.c2 AS c1,
                                                                                                            ref_11.ps_suppkey AS c2,
                                                                                                            ref_14.ps_supplycost AS c3,
                                                                                                            ref_11.ps_availqty AS c4,
                                                                                                            ref_10.p_partkey AS c5,
                                                                                                            ref_13.s_nationkey AS c6,
                                                                                                            ref_13.s_suppkey AS c7
                                                                                                        FROM
                                                                                                            main.partsupp AS ref_14
                                                                                                        WHERE
                                                                                                            EXISTS (
                                                                                                                SELECT
                                                                                                                    subq_1.c0 AS c0, (
                                                                                                                        SELECT
                                                                                                                            p_mfgr
                                                                                                                        FROM
                                                                                                                            main.part
                                                                                                                        LIMIT 1 offset 75) AS c1,
                                                                                                                    ref_13.s_suppkey AS c2,
                                                                                                                    ref_14.ps_suppkey AS c3,
                                                                                                                    (
                                                                                                                        SELECT
                                                                                                                            o_orderkey
                                                                                                                        FROM
                                                                                                                            main.orders
                                                                                                                        LIMIT 1 offset 4) AS c4,
                                                                                                                    (
                                                                                                                        SELECT
                                                                                                                            c_comment
                                                                                                                        FROM
                                                                                                                            main.customer
                                                                                                                        LIMIT 1 offset 6) AS c5,
                                                                                                                    ref_13.s_comment AS c6,
                                                                                                                    ref_14.ps_supplycost AS c7
                                                                                                                FROM
                                                                                                                    main.lineitem AS ref_15,
                                                                                                                    LATERAL (
                                                                                                                        SELECT
                                                                                                                            ref_13.s_suppkey AS c0,
                                                                                                                            subq_1.c0 AS c1,
                                                                                                                            ref_11.ps_availqty AS c2,
                                                                                                                            subq_1.c1 AS c3,
                                                                                                                            ref_11.ps_partkey AS c4,
                                                                                                                            ref_14.ps_comment AS c5
                                                                                                                        FROM
                                                                                                                            main.nation AS ref_16
                                                                                                                        WHERE
                                                                                                                            1
                                                                                                                        LIMIT 47) AS subq_2
                                                                                                                WHERE
                                                                                                                    EXISTS (
                                                                                                                        SELECT
                                                                                                                            ref_14.ps_suppkey AS c0, ref_10.p_retailprice AS c1, ref_11.ps_availqty AS c2, ref_17.c_acctbal AS c3, ref_14.ps_supplycost AS c4, ref_15.l_receiptdate AS c5, ref_11.ps_availqty AS c6, ref_17.c_comment AS c7
                                                                                                                        FROM
                                                                                                                            main.customer AS ref_17
                                                                                                                        WHERE ((0)
                                                                                                                            AND ((ref_14.ps_comment IS NOT NULL)
                                                                                                                                AND (1)))
                                                                                                                        OR ((0)
                                                                                                                            OR (1))
                                                                                                                    LIMIT 123))) AS subq_3
                                                                                                    WHERE
                                                                                                        0) AS subq_4
                                                                                                WHERE
                                                                                                    1
                                                                                                LIMIT 92) AS subq_5, LATERAL (
                                                                                                SELECT
                                                                                                    ref_10.p_container AS c0, ref_10.p_brand AS c1
                                                                                                FROM
                                                                                                    main.region AS ref_18
                                                                                                WHERE
                                                                                                    0
                                                                                                LIMIT 185) AS subq_6
                                                                                        WHERE ((
                                                                                                SELECT
                                                                                                    s_name
                                                                                                FROM
                                                                                                    main.supplier
                                                                                                LIMIT 1 offset 43) IS NOT NULL)
                                                                                        OR ((ref_10.p_name IS NOT NULL)
                                                                                            AND ((0)
                                                                                                OR (0)))
                                                                                    LIMIT 107))
                                                                            AND ((0)
                                                                                OR (1)))
                                                                        OR ((((
                                                                                        SELECT
                                                                                            s_acctbal
                                                                                        FROM
                                                                                            main.supplier
                                                                                        LIMIT 1 offset 4) IS NULL)
                                                                                AND (0))
                                                                            AND ((0)
                                                                                AND (((5 IS NULL)
                                                                                        AND (0))
                                                                                    AND (1))))))
                                                                OR (5 IS NOT NULL)))
                                                        AND ((79 IS NOT NULL)
                                                            AND ((2 IS NOT NULL)
                                                                OR ((EXISTS (
                                                                            SELECT
                                                                                ref_19.o_orderdate AS c0,
                                                                                ref_19.o_orderdate AS c1,
                                                                                ref_19.o_orderstatus AS c2,
                                                                                ref_19.o_orderstatus AS c3,
                                                                                ref_19.o_shippriority AS c4,
                                                                                ref_19.o_orderkey AS c5
                                                                            FROM
                                                                                main.orders AS ref_19
                                                                            WHERE
                                                                                ref_19.o_orderstatus IS NOT NULL))
                                                                        AND ((0)
                                                                            AND (1))))))
                                                        OR (EXISTS (
                                                                SELECT
                                                                    ref_20.c_custkey AS c0, ref_20.c_nationkey AS c1, (
                                                                        SELECT
                                                                            n_regionkey
                                                                        FROM
                                                                            main.nation
                                                                        LIMIT 1 offset 5) AS c2,
                                                                    ref_20.c_mktsegment AS c3,
                                                                    ref_20.c_acctbal AS c4,
                                                                    ref_20.c_nationkey AS c5,
                                                                    ref_20.c_custkey AS c6,
                                                                    ref_20.c_nationkey AS c7
                                                                FROM
                                                                    main.customer AS ref_20
                                                                WHERE
                                                                    1)))))))
                                        OR (EXISTS (
                                                SELECT
                                                    ref_21.c_nationkey AS c0, ref_21.c_name AS c1
                                                FROM
                                                    main.customer AS ref_21
                                                WHERE
                                                    ref_21.c_comment IS NULL)))
                                        OR ((7 IS NOT NULL)
                                            AND (EXISTS (
                                                    SELECT
                                                        ref_22.p_size AS c0
                                                    FROM
                                                        main.part AS ref_22
                                                    WHERE (0)
                                                    OR ((ref_22.p_size IS NOT NULL)
                                                        AND (1))
                                                LIMIT 194)))))
                            AND (1))
                        AND ((1)
                            OR ((EXISTS (
                                        SELECT
                                            subq_7.c2 AS c0,
                                            ref_23.l_extendedprice AS c1,
                                            subq_7.c6 AS c2
                                        FROM
                                            main.lineitem AS ref_23,
                                            LATERAL (
                                                SELECT
                                                    ref_24.c_mktsegment AS c0,
                                                    ref_24.c_phone AS c1,
                                                    ref_23.l_shipmode AS c2,
                                                    ref_24.c_nationkey AS c3,
                                                    ref_23.l_suppkey AS c4,
                                                    ref_24.c_phone AS c5,
                                                    ref_24.c_name AS c6
                                                FROM
                                                    main.customer AS ref_24
                                                WHERE
                                                    0
                                                LIMIT 100) AS subq_7
                                        WHERE
                                            subq_7.c4 IS NULL
                                        LIMIT 87))
                                OR ((EXISTS (
                                            SELECT
                                                ref_25.n_comment AS c0,
                                                ref_25.n_comment AS c1,
                                                ref_25.n_regionkey AS c2
                                            FROM
                                                main.nation AS ref_25
                                            WHERE
                                                ref_25.n_name IS NOT NULL))
                                        AND (((EXISTS (
                                                        SELECT
                                                            ref_26.n_regionkey AS c0, subq_8.c2 AS c1, ref_26.n_nationkey AS c2, ref_26.n_regionkey AS c3, subq_8.c2 AS c4
                                                        FROM
                                                            main.nation AS ref_26,
                                                            LATERAL (
                                                                SELECT
                                                                    ref_27.ps_availqty AS c0,
                                                                    ref_27.ps_supplycost AS c1,
                                                                    ref_27.ps_partkey AS c2,
                                                                    ref_27.ps_availqty AS c3,
                                                                    ref_26.n_name AS c4
                                                                FROM
                                                                    main.partsupp AS ref_27
                                                                WHERE (ref_27.ps_supplycost IS NULL)
                                                                OR ((ref_27.ps_partkey IS NULL)
                                                                    AND (ref_27.ps_comment IS NULL))) AS subq_8
                                                        WHERE ((1)
                                                            OR (((1)
                                                                    OR ((1)
                                                                        AND ((0)
                                                                            OR ((0)
                                                                                AND (EXISTS (
                                                                                        SELECT
                                                                                            ref_26.n_regionkey AS c0, subq_8.c4 AS c1, subq_8.c1 AS c2, subq_8.c0 AS c3, ref_28.o_orderdate AS c4
                                                                                        FROM
                                                                                            main.orders AS ref_28
                                                                                        WHERE
                                                                                            EXISTS (
                                                                                                SELECT
                                                                                                    ref_28.o_custkey AS c0, ref_26.n_nationkey AS c1
                                                                                                FROM
                                                                                                    main.nation AS ref_29
                                                                                                WHERE
                                                                                                    1
                                                                                                LIMIT 154)
                                                                                        LIMIT 105))))))
                                                                OR (0)))
                                                        OR ((1)
                                                            OR (((0)
                                                                    AND (0))
                                                                AND (1)))
                                                    LIMIT 181))
                                            AND (0))
                                        OR (84 IS NOT NULL))))))) THEN
            CAST(NULL AS VARCHAR)
        ELSE
            CAST(NULL AS VARCHAR)
        END)
