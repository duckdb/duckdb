SELECT
    (
        SELECT
            s_address
        FROM
            main.supplier
        LIMIT 1 offset 5) AS c0,
    subq_0.c2 AS c1,
    subq_0.c12 AS c2,
    subq_0.c10 AS c3,
    CASE WHEN (1)
        AND (1) THEN
        CASE WHEN ((EXISTS (
                        SELECT
                            ref_6.s_nationkey AS c0,
                            ref_6.s_suppkey AS c1,
                            ref_6.s_nationkey AS c2,
                            ref_6.s_address AS c3,
                            ref_6.s_comment AS c4,
                            ref_6.s_suppkey AS c5,
                            ref_6.s_acctbal AS c6
                        FROM
                            main.supplier AS ref_6
                        WHERE (0)
                        AND (1)
                    LIMIT 108))
            OR ((((1)
                        OR (94 IS NOT NULL))
                    AND (((61 IS NOT NULL)
                            OR (((EXISTS (
                                            SELECT
                                                subq_0.c8 AS c0,
                                                ref_7.o_orderpriority AS c1,
                                                ref_7.o_orderdate AS c2,
                                                ref_7.o_clerk AS c3,
                                                ref_7.o_orderstatus AS c4,
                                                ref_7.o_orderpriority AS c5,
                                                ref_7.o_orderpriority AS c6,
                                                ref_7.o_clerk AS c7,
                                                subq_0.c0 AS c8
                                            FROM
                                                main.orders AS ref_7
                                            WHERE
                                                1
                                            LIMIT 120))
                                    OR (((EXISTS (
                                                    SELECT
                                                        ref_8.n_name AS c0,
                                                        subq_0.c13 AS c1,
                                                        subq_0.c8 AS c2
                                                    FROM
                                                        main.nation AS ref_8
                                                    WHERE
                                                        EXISTS (
                                                            SELECT
                                                                ref_8.n_comment AS c0, 62 AS c1, ref_8.n_regionkey AS c2, subq_0.c7 AS c3, ref_9.n_comment AS c4
                                                            FROM
                                                                main.nation AS ref_9
                                                            WHERE (ref_9.n_comment IS NULL)
                                                            AND (0)
                                                        LIMIT 32)
                                                LIMIT 138))
                                        AND ((0)
                                            AND (0)))
                                    OR (0)))
                            OR (1)))
                    AND (subq_0.c10 IS NOT NULL)))
            AND (EXISTS (
                    SELECT
                        ref_10.c_comment AS c0,
                        subq_0.c5 AS c1,
                        ref_10.c_name AS c2
                    FROM
                        main.customer AS ref_10
                    WHERE
                        0))))
        OR (1) THEN
        subq_0.c13
    ELSE
        subq_0.c13
    END
ELSE
    CASE WHEN ((EXISTS (
                    SELECT
                        ref_6.s_nationkey AS c0, ref_6.s_suppkey AS c1, ref_6.s_nationkey AS c2, ref_6.s_address AS c3, ref_6.s_comment AS c4, ref_6.s_suppkey AS c5, ref_6.s_acctbal AS c6
                    FROM
                        main.supplier AS ref_6
                    WHERE (0)
                    AND (1)
                LIMIT 108))
        OR ((((1)
                    OR (94 IS NOT NULL))
                AND (((61 IS NOT NULL)
                        OR (((EXISTS (
                                        SELECT
                                            subq_0.c8 AS c0,
                                            ref_7.o_orderpriority AS c1,
                                            ref_7.o_orderdate AS c2,
                                            ref_7.o_clerk AS c3,
                                            ref_7.o_orderstatus AS c4,
                                            ref_7.o_orderpriority AS c5,
                                            ref_7.o_orderpriority AS c6,
                                            ref_7.o_clerk AS c7,
                                            subq_0.c0 AS c8
                                        FROM
                                            main.orders AS ref_7
                                        WHERE
                                            1
                                        LIMIT 120))
                                OR (((EXISTS (
                                                SELECT
                                                    ref_8.n_name AS c0,
                                                    subq_0.c13 AS c1,
                                                    subq_0.c8 AS c2
                                                FROM
                                                    main.nation AS ref_8
                                                WHERE
                                                    EXISTS (
                                                        SELECT
                                                            ref_8.n_comment AS c0, 62 AS c1, ref_8.n_regionkey AS c2, subq_0.c7 AS c3, ref_9.n_comment AS c4
                                                        FROM
                                                            main.nation AS ref_9
                                                        WHERE (ref_9.n_comment IS NULL)
                                                        AND (0)
                                                    LIMIT 32)
                                            LIMIT 138))
                                    AND ((0)
                                        AND (0)))
                                OR (0)))
                        OR (1)))
                AND (subq_0.c10 IS NOT NULL)))
        AND (EXISTS (
                SELECT
                    ref_10.c_comment AS c0,
                    subq_0.c5 AS c1,
                    ref_10.c_name AS c2
                FROM
                    main.customer AS ref_10
                WHERE
                    0))))
    OR (1) THEN
    subq_0.c13
ELSE
    subq_0.c13
END
END AS c4
FROM (
    SELECT
        ref_2.l_comment AS c0,
        (
            SELECT
                p_size
            FROM
                main.part
            LIMIT 1 offset 1) AS c1,
        ref_5.s_name AS c2,
        ref_5.s_acctbal AS c3,
        ref_4.l_quantity AS c4,
        ref_0.c_acctbal AS c5,
        ref_0.c_comment AS c6,
        ref_3.n_comment AS c7,
        79 AS c8,
        ref_0.c_name AS c9,
        ref_1.c_name AS c10,
        27 AS c11,
        (
            SELECT
                r_name
            FROM
                main.region
            LIMIT 1 offset 6) AS c12,
        ref_1.c_name AS c13
    FROM
        main.customer AS ref_0
    LEFT JOIN main.customer AS ref_1 ON (((0)
                AND (1))
            OR ((ref_1.c_acctbal IS NULL)
                AND (0)))
    LEFT JOIN main.lineitem AS ref_2
    RIGHT JOIN main.nation AS ref_3 ON (ref_2.l_quantity IS NOT NULL)
    INNER JOIN main.lineitem AS ref_4 ON (ref_2.l_receiptdate = ref_4.l_shipdate) ON (ref_1.c_address = ref_2.l_returnflag)
    LEFT JOIN main.supplier AS ref_5 ON ((0)
            OR (ref_5.s_name IS NOT NULL))
WHERE (1)
OR (0)) AS subq_0
WHERE ((((1)
            AND (((subq_0.c11 IS NULL)
                    AND ((EXISTS (
                                SELECT
                                    subq_0.c11 AS c0, ref_11.n_nationkey AS c1, 31 AS c2, subq_0.c9 AS c3, ref_11.n_name AS c4, subq_0.c6 AS c5, subq_0.c13 AS c6, subq_0.c10 AS c7, ref_11.n_comment AS c8, subq_0.c10 AS c9, ref_11.n_regionkey AS c10, ref_11.n_name AS c11, subq_0.c0 AS c12, subq_0.c4 AS c13, subq_0.c6 AS c14, ref_11.n_nationkey AS c15, ref_11.n_comment AS c16, ref_11.n_nationkey AS c17, subq_0.c3 AS c18, subq_0.c13 AS c19, subq_0.c5 AS c20, subq_0.c3 AS c21, subq_0.c13 AS c22, subq_0.c2 AS c23, subq_0.c13 AS c24, subq_0.c12 AS c25, ref_11.n_name AS c26, ref_11.n_comment AS c27, ref_11.n_name AS c28, ref_11.n_comment AS c29, subq_0.c12 AS c30, subq_0.c5 AS c31, ref_11.n_comment AS c32, subq_0.c3 AS c33, ref_11.n_comment AS c34, ref_11.n_name AS c35, ref_11.n_nationkey AS c36, subq_0.c0 AS c37, subq_0.c12 AS c38
                                FROM
                                    main.nation AS ref_11
                                WHERE
                                    0))
                            AND (subq_0.c6 IS NOT NULL)))
                    AND (subq_0.c4 IS NULL)))
            AND ((EXISTS (
                        SELECT
                            ref_12.p_mfgr AS c0
                        FROM
                            main.part AS ref_12
                        WHERE ((((ref_12.p_size IS NOT NULL)
                                    AND (((1)
                                            AND ((EXISTS (
                                                        SELECT
                                                            subq_0.c4 AS c0, ref_13.s_suppkey AS c1, subq_0.c5 AS c2, ref_12.p_comment AS c3, ref_12.p_partkey AS c4, ref_12.p_partkey AS c5, ref_13.s_address AS c6, (
                                                                SELECT
                                                                    l_extendedprice
                                                                FROM
                                                                    main.lineitem
                                                                LIMIT 1 offset 4) AS c7,
                                                            38 AS c8,
                                                            ref_12.p_comment AS c9,
                                                            ref_12.p_container AS c10
                                                        FROM
                                                            main.supplier AS ref_13
                                                        WHERE
                                                            1))
                                                    AND ((0)
                                                        OR (1))))
                                            AND (((1)
                                                    OR (1))
                                                AND (0))))
                                    OR ((subq_0.c10 IS NOT NULL)
                                        AND (ref_12.p_type IS NULL)))
                                AND ((ref_12.p_retailprice IS NULL)
                                    AND ((ref_12.p_type IS NOT NULL)
                                        AND (1))))
                            OR (((((0)
                                            AND (((((1)
                                                            AND (((subq_0.c2 IS NOT NULL)
                                                                    AND ((1)
                                                                        AND (0)))
                                                                OR (((1)
                                                                        AND (subq_0.c5 IS NULL))
                                                                    OR (0))))
                                                        OR (((0)
                                                                OR (0))
                                                            AND (1)))
                                                    AND (subq_0.c7 IS NOT NULL))
                                                OR (1)))
                                        AND (EXISTS (
                                                SELECT
                                                    ref_12.p_type AS c0, ref_14.r_regionkey AS c1, subq_0.c2 AS c2, subq_0.c4 AS c3, ref_14.r_regionkey AS c4, ref_12.p_brand AS c5, subq_0.c6 AS c6, ref_14.r_regionkey AS c7, ref_12.p_container AS c8, ref_12.p_comment AS c9, ref_12.p_retailprice AS c10, subq_0.c8 AS c11
                                                FROM
                                                    main.region AS ref_14
                                                WHERE
                                                    EXISTS (
                                                        SELECT
                                                            ref_14.r_name AS c0
                                                        FROM
                                                            main.orders AS ref_15
                                                        WHERE
                                                            1
                                                        LIMIT 108)
                                                LIMIT 51)))
                                    AND ((((0)
                                                OR (0))
                                            AND (subq_0.c6 IS NOT NULL))
                                        AND (EXISTS (
                                                SELECT
                                                    ref_12.p_retailprice AS c0
                                                FROM
                                                    main.nation AS ref_16
                                                WHERE (((EXISTS (
                                                                SELECT
                                                                    subq_0.c5 AS c0, ref_12.p_name AS c1, ref_17.s_nationkey AS c2, subq_0.c3 AS c3, (
                                                                        SELECT
                                                                            l_extendedprice
                                                                        FROM
                                                                            main.lineitem
                                                                        LIMIT 1 offset 4) AS c4,
                                                                    ref_17.s_nationkey AS c5,
                                                                    (
                                                                        SELECT
                                                                            p_mfgr
                                                                        FROM
                                                                            main.part
                                                                        LIMIT 1 offset 4) AS c6,
                                                                    ref_17.s_suppkey AS c7,
                                                                    95 AS c8,
                                                                    ref_12.p_partkey AS c9,
                                                                    ref_17.s_suppkey AS c10,
                                                                    (
                                                                        SELECT
                                                                            c_acctbal
                                                                        FROM
                                                                            main.customer
                                                                        LIMIT 1 offset 1) AS c11,
                                                                    subq_0.c2 AS c12,
                                                                    ref_16.n_nationkey AS c13,
                                                                    ref_17.s_suppkey AS c14,
                                                                    ref_17.s_suppkey AS c15,
                                                                    ref_16.n_name AS c16
                                                                FROM
                                                                    main.supplier AS ref_17
                                                                WHERE
                                                                    1))
                                                            AND (subq_0.c10 IS NOT NULL))
                                                        AND (0))
                                                    OR ((((0)
                                                                OR ((0)
                                                                    OR (ref_16.n_nationkey IS NULL)))
                                                            AND (((1)
                                                                    AND (ref_12.p_retailprice IS NULL))
                                                                AND (EXISTS (
                                                                        SELECT
                                                                            subq_0.c8 AS c0, ref_16.n_nationkey AS c1, (
                                                                                SELECT
                                                                                    s_comment
                                                                                FROM
                                                                                    main.supplier
                                                                                LIMIT 1 offset 3) AS c2
                                                                        FROM
                                                                            main.nation AS ref_18
                                                                        WHERE ((subq_0.c4 IS NULL)
                                                                            AND (((0)
                                                                                    OR (ref_18.n_comment IS NULL))
                                                                                OR (0)))
                                                                        AND (0)
                                                                    LIMIT 85))))
                                                    OR ((0)
                                                        AND (ref_12.p_comment IS NOT NULL)))
                                            LIMIT 144))))
                            OR (subq_0.c4 IS NOT NULL))
                    LIMIT 64))
            OR ((((0)
                        OR (EXISTS (
                                SELECT
                                    subq_0.c2 AS c0,
                                    subq_0.c0 AS c1,
                                    ref_19.r_comment AS c2,
                                    ref_19.r_name AS c3,
                                    subq_0.c7 AS c4,
                                    ref_19.r_regionkey AS c5,
                                    subq_0.c0 AS c6
                                FROM
                                    main.region AS ref_19
                                WHERE (ref_19.r_name IS NOT NULL)
                                AND (subq_0.c6 IS NOT NULL)
                            LIMIT 118)))
                OR ((1)
                    OR (1)))
            AND ((EXISTS (
                        SELECT
                            ref_20.l_linenumber AS c0,
                            subq_0.c11 AS c1,
                            ref_20.l_partkey AS c2,
                            ref_20.l_tax AS c3,
                            ref_20.l_tax AS c4,
                            subq_0.c3 AS c5,
                            ref_20.l_linenumber AS c6
                        FROM
                            main.lineitem AS ref_20
                        WHERE (0)
                        OR ((0)
                            AND (0))
                    LIMIT 73))
            OR (EXISTS (
                    SELECT
                        (
                            SELECT
                                c_comment
                            FROM
                                main.customer
                            LIMIT 1 offset 40) AS c0,
                        ref_21.n_comment AS c1,
                        subq_0.c3 AS c2,
                        ref_21.n_comment AS c3,
                        subq_0.c3 AS c4,
                        ref_21.n_regionkey AS c5
                    FROM
                        main.nation AS ref_21
                    WHERE
                        ref_21.n_comment IS NULL
                    LIMIT 144))))))
AND (((1)
        OR (((((subq_0.c10 IS NULL)
                        AND ((((EXISTS (
                                            SELECT
                                                ref_22.s_name AS c0,
                                                ref_22.s_name AS c1
                                            FROM
                                                main.supplier AS ref_22
                                            WHERE (((ref_22.s_suppkey IS NOT NULL)
                                                    OR (0))
                                                AND (0))
                                            AND (1)
                                        LIMIT 140))
                                OR (1))
                            OR (((EXISTS (
                                            SELECT
                                                ref_23.ps_partkey AS c0,
                                                ref_23.ps_comment AS c1,
                                                subq_0.c13 AS c2,
                                                subq_0.c7 AS c3
                                            FROM
                                                main.partsupp AS ref_23
                                            WHERE
                                                EXISTS (
                                                    SELECT
                                                        ref_24.l_discount AS c0, ref_23.ps_supplycost AS c1
                                                    FROM
                                                        main.lineitem AS ref_24
                                                    WHERE ((0)
                                                        OR (ref_23.ps_comment IS NULL))
                                                    AND ((1)
                                                        AND (subq_0.c8 IS NULL)))
                                            LIMIT 176))
                                    AND (0))
                                OR ((subq_0.c1 IS NULL)
                                    OR (EXISTS (
                                            SELECT
                                                (
                                                    SELECT
                                                        r_regionkey
                                                    FROM
                                                        main.region
                                                    LIMIT 1 offset 2) AS c0,
                                                ref_25.o_orderpriority AS c1,
                                                ref_25.o_totalprice AS c2,
                                                subq_0.c3 AS c3,
                                                ref_25.o_shippriority AS c4,
                                                ref_25.o_orderstatus AS c5,
                                                (
                                                    SELECT
                                                        n_name
                                                    FROM
                                                        main.nation
                                                    LIMIT 1 offset 55) AS c6
                                            FROM
                                                main.orders AS ref_25
                                            WHERE
                                                0)))))
                            OR (subq_0.c12 IS NULL)))
                    OR (subq_0.c6 IS NOT NULL))
                OR ((1)
                    AND (8 IS NOT NULL)))
            AND (subq_0.c2 IS NULL)))
    AND (((subq_0.c3 IS NULL)
            OR ((subq_0.c7 IS NULL)
                AND (0)))
        AND (((1)
                AND ((1)
                    AND (1)))
            OR (((((EXISTS (
                                    SELECT
                                        subq_0.c10 AS c0, ref_26.n_comment AS c1, ref_26.n_regionkey AS c2, ref_26.n_comment AS c3, subq_0.c9 AS c4, ref_26.n_regionkey AS c5, ref_26.n_comment AS c6, ref_26.n_nationkey AS c7, subq_0.c3 AS c8
                                    FROM
                                        main.nation AS ref_26
                                    WHERE (((((0)
                                                    OR ((ref_26.n_name IS NULL)
                                                        AND ((subq_0.c3 IS NULL)
                                                            OR (ref_26.n_name IS NOT NULL))))
                                                AND ((ref_26.n_regionkey IS NULL)
                                                    OR (1)))
                                            AND (((EXISTS (
                                                            SELECT
                                                                subq_0.c1 AS c0, ref_26.n_comment AS c1, ref_27.n_name AS c2, ref_26.n_name AS c3, ref_26.n_name AS c4, subq_0.c10 AS c5, ref_26.n_nationkey AS c6, subq_0.c1 AS c7, ref_26.n_regionkey AS c8, subq_0.c11 AS c9, ref_27.n_comment AS c10, subq_0.c8 AS c11, ref_27.n_comment AS c12, (
                                                                    SELECT
                                                                        o_orderpriority
                                                                    FROM
                                                                        main.orders
                                                                    LIMIT 1 offset 5) AS c13,
                                                                ref_27.n_regionkey AS c14,
                                                                ref_27.n_comment AS c15,
                                                                subq_0.c6 AS c16
                                                            FROM
                                                                main.nation AS ref_27
                                                            WHERE ((subq_0.c5 IS NOT NULL)
                                                                OR (48 IS NOT NULL))
                                                            AND (ref_27.n_comment IS NOT NULL)
                                                        LIMIT 100))
                                                OR (((1)
                                                        OR ((((EXISTS (
                                                                            SELECT
                                                                                ref_26.n_comment AS c0,
                                                                                ref_28.c_mktsegment AS c1,
                                                                                subq_0.c0 AS c2,
                                                                                ref_26.n_comment AS c3,
                                                                                ref_26.n_nationkey AS c4,
                                                                                (
                                                                                    SELECT
                                                                                        c_mktsegment
                                                                                    FROM
                                                                                        main.customer
                                                                                    LIMIT 1 offset 1) AS c5,
                                                                                ref_28.c_custkey AS c6,
                                                                                ref_26.n_regionkey AS c7
                                                                            FROM
                                                                                main.customer AS ref_28
                                                                            WHERE
                                                                                subq_0.c13 IS NULL))
                                                                        OR ((1)
                                                                            OR ((1)
                                                                                OR (1))))
                                                                    OR (EXISTS (
                                                                            SELECT
                                                                                subq_0.c13 AS c0, subq_0.c5 AS c1, 91 AS c2
                                                                            FROM
                                                                                main.lineitem AS ref_29
                                                                            WHERE (0)
                                                                            OR (0))))
                                                                AND (EXISTS (
                                                                        SELECT
                                                                            subq_0.c7 AS c0, subq_0.c7 AS c1, subq_0.c3 AS c2, subq_0.c3 AS c3, 90 AS c4, ref_30.l_discount AS c5, ref_26.n_regionkey AS c6, ref_26.n_nationkey AS c7, ref_26.n_name AS c8, ref_30.l_partkey AS c9, subq_0.c12 AS c10, subq_0.c1 AS c11, subq_0.c6 AS c12, ref_26.n_comment AS c13, ref_30.l_suppkey AS c14, ref_30.l_shipdate AS c15, ref_26.n_name AS c16, (
                                                                                SELECT
                                                                                    c_custkey
                                                                                FROM
                                                                                    main.customer
                                                                                LIMIT 1 offset 60) AS c17,
                                                                            ref_30.l_orderkey AS c18,
                                                                            ref_26.n_nationkey AS c19,
                                                                            65 AS c20
                                                                        FROM
                                                                            main.lineitem AS ref_30
                                                                        WHERE
                                                                            0
                                                                        LIMIT 180))))
                                                        AND (1)))
                                                AND (ref_26.n_regionkey IS NULL)))
                                        AND (0))
                                    OR (((1)
                                            AND (0))
                                        AND (subq_0.c5 IS NULL))))
                            OR ((1)
                                OR ((subq_0.c2 IS NOT NULL)
                                    AND ((0)
                                        OR (0)))))
                        OR (subq_0.c5 IS NULL))
                    OR ((((EXISTS (
                                        SELECT
                                            subq_0.c7 AS c0,
                                            subq_0.c1 AS c1,
                                            subq_0.c7 AS c2,
                                            subq_0.c4 AS c3,
                                            ref_31.l_tax AS c4,
                                            ref_31.l_discount AS c5,
                                            subq_0.c5 AS c6,
                                            ref_31.l_extendedprice AS c7,
                                            (
                                                SELECT
                                                    s_acctbal
                                                FROM
                                                    main.supplier
                                                LIMIT 1 offset 5) AS c8,
                                            ref_31.l_receiptdate AS c9,
                                            ref_31.l_comment AS c10,
                                            (
                                                SELECT
                                                    l_receiptdate
                                                FROM
                                                    main.lineitem
                                                LIMIT 1 offset 30) AS c11,
                                            subq_0.c2 AS c12
                                        FROM
                                            main.lineitem AS ref_31
                                        WHERE
                                            ref_31.l_comment IS NOT NULL
                                        LIMIT 68))
                                OR ((((subq_0.c7 IS NOT NULL)
                                            AND (subq_0.c11 IS NOT NULL))
                                        OR (1))
                                    OR (((
                                                SELECT
                                                    c_comment
                                                FROM
                                                    main.customer
                                                LIMIT 1 offset 5)
                                            IS NOT NULL)
                                        OR (EXISTS (
                                                SELECT
                                                    ref_32.ps_suppkey AS c0
                                                FROM
                                                    main.partsupp AS ref_32
                                                WHERE (subq_0.c1 IS NULL)
                                                AND (EXISTS (
                                                        SELECT
                                                            ref_32.ps_suppkey AS c0, subq_0.c2 AS c1, ref_33.r_regionkey AS c2, ref_33.r_name AS c3
                                                        FROM
                                                            main.region AS ref_33
                                                        WHERE
                                                            0
                                                        LIMIT 31))
                                            LIMIT 150)))))
                        OR ((subq_0.c11 IS NULL)
                            OR ((subq_0.c1 IS NOT NULL)
                                OR (((
                                            SELECT
                                                c_custkey
                                            FROM
                                                main.customer
                                            LIMIT 1 offset 3)
                                        IS NULL)
                                    OR (1)))))
                    AND (subq_0.c2 IS NOT NULL)))
            OR (((1)
                    OR (subq_0.c5 IS NOT NULL))
                AND (((subq_0.c7 IS NULL)
                        AND (0))
                    OR (((subq_0.c5 IS NULL)
                            AND ((((0)
                                        OR (((0)
                                                AND ((0)
                                                    AND (EXISTS (
                                                            SELECT
                                                                ref_34.p_brand AS c0,
                                                                ref_34.p_type AS c1,
                                                                ref_34.p_comment AS c2,
                                                                ref_34.p_name AS c3,
                                                                ref_34.p_brand AS c4,
                                                                subq_0.c0 AS c5,
                                                                ref_34.p_size AS c6,
                                                                ref_34.p_size AS c7
                                                            FROM
                                                                main.part AS ref_34
                                                            WHERE ((0)
                                                                AND (1))
                                                            OR (subq_0.c10 IS NOT NULL)))))
                                            OR (0)))
                                    AND (1))
                                OR (subq_0.c11 IS NOT NULL)))
                        OR ((subq_0.c4 IS NULL)
                            OR ((0)
                                AND (((1)
                                        AND ((0)
                                            AND (0)))
                                    OR (EXISTS (
                                            SELECT
                                                DISTINCT ref_35.c_comment AS c0, subq_0.c7 AS c1, ref_35.c_acctbal AS c2, subq_0.c1 AS c3, (
                                                    SELECT
                                                        o_orderstatus
                                                    FROM
                                                        main.orders
                                                    LIMIT 1 offset 5) AS c4,
                                                ref_35.c_custkey AS c5
                                            FROM
                                                main.customer AS ref_35
                                            WHERE ((subq_0.c12 IS NULL)
                                                OR ((ref_35.c_phone IS NULL)
                                                    AND (ref_35.c_acctbal IS NOT NULL)))
                                            OR ((0)
                                                OR (ref_35.c_comment IS NOT NULL))
                                        LIMIT 146)))))))))))))
AND (((0)
        OR ((((((((1)
                                    OR (((subq_0.c1 IS NULL)
                                            OR ((((EXISTS (
                                                                SELECT
                                                                    subq_0.c8 AS c0,
                                                                    subq_0.c5 AS c1
                                                                FROM
                                                                    main.region AS ref_36
                                                                WHERE
                                                                    0
                                                                LIMIT 193))
                                                        AND (0))
                                                    AND (((subq_0.c10 IS NULL)
                                                            OR (EXISTS (
                                                                    SELECT
                                                                        subq_0.c11 AS c0,
                                                                        subq_0.c3 AS c1,
                                                                        ref_37.c_custkey AS c2
                                                                    FROM
                                                                        main.customer AS ref_37
                                                                    WHERE
                                                                        1
                                                                    LIMIT 132)))
                                                        AND (((((0)
                                                                        OR ((EXISTS (
                                                                                    SELECT
                                                                                        subq_0.c9 AS c0,
                                                                                        ref_38.c_acctbal AS c1,
                                                                                        ref_38.c_acctbal AS c2,
                                                                                        subq_0.c7 AS c3
                                                                                    FROM
                                                                                        main.customer AS ref_38
                                                                                    WHERE
                                                                                        0))
                                                                                OR (subq_0.c11 IS NULL)))
                                                                        OR (0))
                                                                    AND (0))
                                                                AND ((1)
                                                                    AND ((EXISTS (
                                                                                SELECT
                                                                                    subq_0.c12 AS c0, ref_39.o_orderpriority AS c1, 68 AS c2, ref_39.o_clerk AS c3, subq_0.c0 AS c4, ref_39.o_orderstatus AS c5, ref_39.o_totalprice AS c6, subq_0.c10 AS c7, subq_0.c6 AS c8, ref_39.o_shippriority AS c9, subq_0.c4 AS c10, ref_39.o_orderkey AS c11, subq_0.c11 AS c12
                                                                                FROM
                                                                                    main.orders AS ref_39
                                                                                WHERE
                                                                                    ref_39.o_custkey IS NULL
                                                                                LIMIT 101))
                                                                        AND (1))))))
                                                    AND (1)))
                                            OR (1)))
                                    AND (((subq_0.c9 IS NOT NULL)
                                            AND ((((0)
                                                        AND (0))
                                                    OR (0))
                                                OR (EXISTS (
                                                        SELECT
                                                            ref_40.c_mktsegment AS c0,
                                                            subq_0.c8 AS c1,
                                                            subq_0.c9 AS c2,
                                                            ref_40.c_custkey AS c3,
                                                            ref_40.c_custkey AS c4,
                                                            (
                                                                SELECT
                                                                    ps_partkey
                                                                FROM
                                                                    main.partsupp
                                                                LIMIT 1 offset 4) AS c5,
                                                            subq_0.c3 AS c6,
                                                            subq_0.c0 AS c7,
                                                            subq_0.c11 AS c8,
                                                            ref_40.c_custkey AS c9,
                                                            ref_40.c_address AS c10,
                                                            subq_0.c9 AS c11,
                                                            ref_40.c_nationkey AS c12,
                                                            subq_0.c10 AS c13,
                                                            subq_0.c11 AS c14,
                                                            subq_0.c8 AS c15,
                                                            subq_0.c13 AS c16,
                                                            ref_40.c_acctbal AS c17,
                                                            37 AS c18,
                                                            subq_0.c9 AS c19,
                                                            ref_40.c_nationkey AS c20,
                                                            ref_40.c_mktsegment AS c21,
                                                            ref_40.c_phone AS c22,
                                                            ref_40.c_nationkey AS c23,
                                                            subq_0.c5 AS c24,
                                                            ref_40.c_name AS c25
                                                        FROM
                                                            main.customer AS ref_40
                                                        WHERE (subq_0.c1 IS NULL)
                                                        OR ((ref_40.c_mktsegment IS NULL)
                                                            OR ((((1)
                                                                        OR ((0)
                                                                            AND ((1)
                                                                                AND (0))))
                                                                    AND (1))
                                                                OR ((1)
                                                                    OR (9 IS NULL))))
                                                    LIMIT 66))))
                                    AND ((EXISTS (
                                                SELECT
                                                    ref_41.s_comment AS c0
                                                FROM
                                                    main.supplier AS ref_41
                                                WHERE
                                                    1))
                                            OR (1))))
                                AND (0))
                            AND (1))
                        AND ((0)
                            OR (subq_0.c1 IS NULL)))
                    OR ((0)
                        AND (19 IS NOT NULL)))
                AND ((0)
                    AND (subq_0.c0 IS NULL))))
        AND (1))
LIMIT 122
