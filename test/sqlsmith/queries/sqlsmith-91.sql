SELECT
    subq_1.c0 AS c0,
    subq_1.c0 AS c1,
    subq_1.c0 AS c2,
    subq_1.c0 AS c3,
    (
        SELECT
            l_discount
        FROM
            main.lineitem
        LIMIT 1 offset 1) AS c4,
    subq_1.c0 AS c5,
    subq_1.c0 AS c6,
    subq_1.c0 AS c7,
    subq_1.c0 AS c8,
    subq_1.c0 AS c9,
    subq_1.c0 AS c10,
    subq_1.c0 AS c11,
    subq_1.c0 AS c12,
    subq_1.c0 AS c13,
    subq_1.c0 AS c14,
    subq_1.c0 AS c15,
    subq_1.c0 AS c16,
    subq_1.c0 AS c17,
    subq_1.c0 AS c18,
    subq_1.c0 AS c19,
    subq_1.c0 AS c20,
    subq_1.c0 AS c21,
    subq_1.c0 AS c22,
    (
        SELECT
            l_comment
        FROM
            main.lineitem
        LIMIT 1 offset 2) AS c23,
    subq_1.c0 AS c24,
    subq_1.c0 AS c25,
    CASE WHEN (1)
        OR (EXISTS (
                SELECT
                    ref_4.o_orderkey AS c0,
                    subq_1.c0 AS c1,
                    subq_1.c0 AS c2,
                    subq_1.c0 AS c3,
                    subq_1.c0 AS c4,
                    subq_1.c0 AS c5,
                    50 AS c6,
                    subq_1.c0 AS c7,
                    ref_4.o_comment AS c8,
                    subq_1.c0 AS c9,
                    ref_4.o_comment AS c10,
                    ref_4.o_orderpriority AS c11,
                    ref_4.o_orderkey AS c12,
                    ref_4.o_orderpriority AS c13,
                    42 AS c14,
                    ref_4.o_orderpriority AS c15,
                    ref_4.o_orderkey AS c16,
                    subq_1.c0 AS c17,
                    ref_4.o_orderstatus AS c18,
                    subq_1.c0 AS c19,
                    ref_4.o_orderkey AS c20
                FROM
                    main.orders AS ref_4
                WHERE (ref_4.o_clerk IS NULL)
                OR (subq_1.c0 IS NOT NULL)
            LIMIT 26)) THEN
    subq_1.c0
ELSE
    subq_1.c0
END AS c26,
subq_1.c0 AS c27,
subq_1.c0 AS c28,
subq_1.c0 AS c29,
CASE WHEN (((subq_1.c0 IS NULL)
            OR (80 IS NULL))
        OR (EXISTS (
                SELECT
                    subq_1.c0 AS c0,
                    subq_1.c0 AS c1,
                    subq_1.c0 AS c2
                FROM
                    main.supplier AS ref_5
                WHERE
                    ref_5.s_address IS NOT NULL
                LIMIT 103)))
    OR (subq_1.c0 IS NOT NULL) THEN
    subq_1.c0
ELSE
    subq_1.c0
END AS c30,
subq_1.c0 AS c31,
subq_1.c0 AS c32,
subq_1.c0 AS c33
FROM (
    SELECT
        10 AS c0
    FROM
        main.customer AS ref_0
        INNER JOIN (
            SELECT
                (
                    SELECT
                        n_name
                    FROM
                        main.nation
                    LIMIT 1 offset 3) AS c0,
                ref_1.o_comment AS c1,
                ref_1.o_orderkey AS c2,
                ref_1.o_shippriority AS c3,
                ref_1.o_orderpriority AS c4,
                ref_1.o_clerk AS c5,
                ref_1.o_orderpriority AS c6,
                ref_1.o_orderstatus AS c7,
                ref_1.o_orderstatus AS c8,
                ref_1.o_orderkey AS c9,
                ref_1.o_totalprice AS c10,
                ref_1.o_custkey AS c11,
                ref_1.o_orderstatus AS c12
            FROM
                main.orders AS ref_1
            WHERE (ref_1.o_shippriority IS NOT NULL)
            AND ((EXISTS (
                        SELECT
                            91 AS c0, ref_1.o_orderkey AS c1, ref_2.s_phone AS c2, ref_1.o_orderstatus AS c3
                        FROM
                            main.supplier AS ref_2
                        WHERE ((1)
                            AND ((0)
                                OR (0)))
                        OR (1)
                    LIMIT 124))
            OR ((1)
                AND (1)))
    LIMIT 143) AS subq_0 ON (ref_0.c_comment = subq_0.c0)
WHERE
    EXISTS (
        SELECT
            subq_0.c8 AS c0, ref_0.c_phone AS c1, ref_3.n_nationkey AS c2, ref_3.n_name AS c3, subq_0.c3 AS c4, subq_0.c0 AS c5, subq_0.c12 AS c6, ref_3.n_regionkey AS c7, (
                SELECT
                    ps_availqty
                FROM
                    main.partsupp
                LIMIT 1 offset 2) AS c8
        FROM
            main.nation AS ref_3
        WHERE
            ref_3.n_nationkey IS NOT NULL
        LIMIT 172)) AS subq_1
WHERE (((((subq_1.c0 IS NOT NULL)
                OR (EXISTS (
                        SELECT
                            subq_1.c0 AS c0, ref_6.s_suppkey AS c1, subq_1.c0 AS c2
                        FROM
                            main.supplier AS ref_6
                        WHERE ((1)
                            OR (EXISTS (
                                    SELECT
                                        ref_7.n_comment AS c0, ref_7.n_nationkey AS c1, subq_1.c0 AS c2, ref_7.n_name AS c3, ref_6.s_nationkey AS c4, 26 AS c5
                                    FROM
                                        main.nation AS ref_7
                                    WHERE
                                        0
                                    LIMIT 96)))
                        AND (subq_1.c0 IS NOT NULL)
                    LIMIT 47)))
        OR ((95 IS NULL)
            AND (1)))
    AND (((((1)
                    AND (0))
                AND (((((subq_1.c0 IS NULL)
                                OR (((subq_1.c0 IS NULL)
                                        OR ((EXISTS (
                                                    SELECT
                                                        ref_8.c_mktsegment AS c0,
                                                        subq_1.c0 AS c1,
                                                        ref_8.c_nationkey AS c2,
                                                        subq_1.c0 AS c3,
                                                        ref_8.c_acctbal AS c4,
                                                        ref_8.c_acctbal AS c5,
                                                        64 AS c6,
                                                        subq_1.c0 AS c7
                                                    FROM
                                                        main.customer AS ref_8
                                                    WHERE
                                                        EXISTS (
                                                            SELECT
                                                                subq_1.c0 AS c0
                                                            FROM
                                                                main.lineitem AS ref_9
                                                            WHERE ((EXISTS (
                                                                        SELECT
                                                                            ref_9.l_suppkey AS c0, ref_10.n_nationkey AS c1, 26 AS c2, subq_1.c0 AS c3, ref_8.c_acctbal AS c4, ref_9.l_discount AS c5, 47 AS c6, ref_8.c_mktsegment AS c7, ref_10.n_comment AS c8, ref_9.l_returnflag AS c9, ref_8.c_mktsegment AS c10, ref_9.l_linestatus AS c11, subq_1.c0 AS c12
                                                                        FROM
                                                                            main.nation AS ref_10
                                                                        WHERE
                                                                            1
                                                                        LIMIT 163))
                                                                AND (ref_9.l_receiptdate IS NOT NULL))
                                                            OR (ref_9.l_receiptdate IS NULL))
                                                    LIMIT 111))
                                            OR (subq_1.c0 IS NOT NULL)))
                                    AND (EXISTS (
                                            SELECT
                                                subq_1.c0 AS c0,
                                                ref_11.p_name AS c1,
                                                34 AS c2,
                                                ref_11.p_retailprice AS c3,
                                                subq_1.c0 AS c4,
                                                subq_1.c0 AS c5,
                                                ref_11.p_mfgr AS c6,
                                                subq_1.c0 AS c7,
                                                ref_11.p_name AS c8,
                                                ref_11.p_comment AS c9
                                            FROM
                                                main.part AS ref_11
                                            WHERE (0)
                                            OR ((EXISTS (
                                                        SELECT
                                                            ref_11.p_mfgr AS c0, ref_11.p_comment AS c1, subq_1.c0 AS c2
                                                        FROM
                                                            main.lineitem AS ref_12
                                                        WHERE
                                                            1))
                                                    AND (ref_11.p_partkey IS NULL))
                                            LIMIT 146))))
                            AND ((((0)
                                        OR (99 IS NOT NULL))
                                    AND (0))
                                OR (((subq_1.c0 IS NOT NULL)
                                        OR ((((subq_1.c0 IS NOT NULL)
                                                    OR ((0)
                                                        AND ((subq_1.c0 IS NOT NULL)
                                                            OR (0))))
                                                OR (1))
                                            AND ((0)
                                                AND ((0)
                                                    AND (EXISTS (
                                                            SELECT
                                                                ref_13.ps_availqty AS c0,
                                                                ref_13.ps_availqty AS c1,
                                                                subq_1.c0 AS c2,
                                                                subq_1.c0 AS c3,
                                                                subq_1.c0 AS c4
                                                            FROM
                                                                main.partsupp AS ref_13
                                                            WHERE (ref_13.ps_supplycost IS NOT NULL)
                                                            AND (EXISTS (
                                                                    SELECT
                                                                        subq_1.c0 AS c0, subq_1.c0 AS c1, subq_1.c0 AS c2
                                                                    FROM
                                                                        main.customer AS ref_14
                                                                    WHERE
                                                                        EXISTS (
                                                                            SELECT
                                                                                ref_13.ps_partkey AS c0
                                                                            FROM
                                                                                main.customer AS ref_15
                                                                            WHERE
                                                                                EXISTS (
                                                                                    SELECT
                                                                                        (
                                                                                            SELECT
                                                                                                s_nationkey
                                                                                            FROM
                                                                                                main.supplier
                                                                                            LIMIT 1 offset 3) AS c0
                                                                                    FROM
                                                                                        main.nation AS ref_16
                                                                                    WHERE
                                                                                        1
                                                                                    LIMIT 148)
                                                                            LIMIT 95)
                                                                    LIMIT 113))))))))
                                    OR ((1)
                                        OR ((subq_1.c0 IS NULL)
                                            AND (0))))))
                        OR (EXISTS (
                                SELECT
                                    ref_17.s_nationkey AS c0,
                                    subq_1.c0 AS c1
                                FROM
                                    main.supplier AS ref_17
                                WHERE (39 IS NULL)
                                AND (ref_17.s_nationkey IS NOT NULL))))
                    OR (33 IS NULL)))
            OR ((subq_1.c0 IS NOT NULL)
                OR (1)))
        OR (subq_1.c0 IS NOT NULL)))
AND (((subq_1.c0 IS NOT NULL)
        OR (((EXISTS (
                        SELECT
                            ref_18.o_orderpriority AS c0
                        FROM
                            main.orders AS ref_18
                        WHERE
                            EXISTS (
                                SELECT
                                    ref_18.o_shippriority AS c0, subq_1.c0 AS c1
                                FROM
                                    main.orders AS ref_19
                                WHERE (1)
                                OR (0))
                        LIMIT 179))
                OR (1))
            AND (((subq_1.c0 IS NOT NULL)
                    OR (EXISTS (
                            SELECT
                                ref_20.o_custkey AS c0,
                                subq_1.c0 AS c1,
                                ref_20.o_totalprice AS c2,
                                ref_20.o_orderstatus AS c3,
                                subq_1.c0 AS c4
                            FROM
                                main.orders AS ref_20
                            WHERE (((0)
                                    AND (0))
                                AND ((EXISTS (
                                            SELECT
                                                ref_21.l_suppkey AS c0, ref_20.o_orderkey AS c1, ref_20.o_orderstatus AS c2, ref_21.l_suppkey AS c3, subq_1.c0 AS c4, ref_20.o_comment AS c5, ref_20.o_shippriority AS c6, subq_1.c0 AS c7, subq_1.c0 AS c8, ref_21.l_linestatus AS c9, ref_20.o_orderdate AS c10, ref_20.o_orderpriority AS c11, ref_21.l_extendedprice AS c12, subq_1.c0 AS c13, subq_1.c0 AS c14, ref_21.l_receiptdate AS c15, ref_21.l_tax AS c16, ref_21.l_commitdate AS c17, ref_20.o_custkey AS c18, subq_1.c0 AS c19, ref_21.l_receiptdate AS c20, ref_20.o_clerk AS c21, ref_21.l_extendedprice AS c22
                                            FROM
                                                main.lineitem AS ref_21
                                            WHERE
                                                1))
                                        AND (1)))
                                OR (0)
                            LIMIT 167)))
                AND (((EXISTS (
                                SELECT
                                    ref_22.o_orderpriority AS c0,
                                    ref_22.o_orderpriority AS c1,
                                    70 AS c2,
                                    49 AS c3,
                                    ref_22.o_custkey AS c4,
                                    subq_1.c0 AS c5,
                                    subq_1.c0 AS c6,
                                    ref_22.o_orderpriority AS c7,
                                    ref_22.o_orderdate AS c8,
                                    subq_1.c0 AS c9
                                FROM
                                    main.orders AS ref_22
                                WHERE ((ref_22.o_orderdate IS NOT NULL)
                                    AND (EXISTS (
                                            SELECT
                                                ref_23.o_orderpriority AS c0, (
                                                    SELECT
                                                        r_comment
                                                    FROM
                                                        main.region
                                                    LIMIT 1 offset 4) AS c1,
                                                subq_1.c0 AS c2,
                                                subq_1.c0 AS c3,
                                                ref_22.o_orderstatus AS c4,
                                                ref_22.o_custkey AS c5,
                                                ref_23.o_orderkey AS c6,
                                                subq_1.c0 AS c7,
                                                ref_22.o_comment AS c8,
                                                subq_1.c0 AS c9,
                                                subq_1.c0 AS c10,
                                                ref_23.o_totalprice AS c11,
                                                subq_1.c0 AS c12,
                                                ref_22.o_custkey AS c13,
                                                ref_23.o_custkey AS c14
                                            FROM
                                                main.orders AS ref_23
                                            WHERE (0)
                                            OR (EXISTS (
                                                    SELECT
                                                        ref_23.o_shippriority AS c0, ref_24.c_nationkey AS c1, ref_24.c_mktsegment AS c2
                                                    FROM
                                                        main.customer AS ref_24
                                                    WHERE (
                                                        SELECT
                                                            n_regionkey
                                                        FROM
                                                            main.nation
                                                        LIMIT 1 offset 5)
                                                    IS NOT NULL
                                                LIMIT 67))
                                    LIMIT 106)))
                        OR (EXISTS (
                                SELECT
                                    ref_25.n_comment AS c0,
                                    ref_22.o_orderstatus AS c1,
                                    45 AS c2,
                                    ref_22.o_orderkey AS c3
                                FROM
                                    main.nation AS ref_25
                                WHERE
                                    ref_25.n_comment IS NULL))
                        LIMIT 80))
                AND (subq_1.c0 IS NULL))
            AND (0)))))
AND ((((1)
            OR ((((EXISTS (
                                SELECT
                                    subq_1.c0 AS c0,
                                    ref_26.r_comment AS c1,
                                    subq_1.c0 AS c2
                                FROM
                                    main.region AS ref_26
                                WHERE
                                    0
                                LIMIT 71))
                        AND (EXISTS (
                                SELECT
                                    subq_1.c0 AS c0,
                                    ref_27.s_acctbal AS c1,
                                    ref_27.s_name AS c2,
                                    ref_27.s_acctbal AS c3,
                                    subq_1.c0 AS c4,
                                    ref_27.s_name AS c5
                                FROM
                                    main.supplier AS ref_27
                                WHERE (0)
                                OR (((ref_27.s_suppkey IS NOT NULL)
                                        OR (ref_27.s_name IS NOT NULL))
                                    AND (EXISTS (
                                            SELECT
                                                ref_27.s_nationkey AS c0, ref_27.s_name AS c1, ref_27.s_acctbal AS c2
                                            FROM
                                                main.region AS ref_28
                                            WHERE
                                                EXISTS (
                                                    SELECT
                                                        ref_28.r_comment AS c0, ref_27.s_nationkey AS c1, ref_28.r_comment AS c2, ref_28.r_regionkey AS c3
                                                    FROM
                                                        main.orders AS ref_29
                                                    WHERE (subq_1.c0 IS NOT NULL)
                                                    AND (1)))))
                                LIMIT 109)))
                    OR (((0)
                            AND ((subq_1.c0 IS NOT NULL)
                                AND (1)))
                        OR (subq_1.c0 IS NULL)))
                OR (0)))
        AND (1))
    OR (1))))
OR (subq_1.c0 IS NULL)
