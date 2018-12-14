WITH jennifer_0 AS (
    SELECT
        subq_0.c3 AS c0,
        subq_0.c2 AS c1,
        subq_0.c11 AS c2,
        subq_0.c6 AS c3,
        subq_0.c7 AS c4,
        subq_0.c9 AS c5
    FROM (
        SELECT
            30 AS c0,
            ref_0.p_retailprice AS c1,
            ref_0.p_container AS c2,
            ref_0.p_retailprice AS c3,
            ref_0.p_name AS c4,
            CASE WHEN ref_0.p_comment IS NULL THEN
                ref_0.p_comment
            ELSE
                ref_0.p_comment
            END AS c5,
            ref_0.p_container AS c6,
            ref_0.p_name AS c7,
            ref_0.p_name AS c8,
            ref_0.p_partkey AS c9,
            ref_0.p_container AS c10,
            ref_0.p_name AS c11
        FROM
            main.part AS ref_0
        WHERE
            EXISTS (
                SELECT
                    ref_1.ps_availqty AS c0, ref_1.ps_suppkey AS c1, ref_1.ps_availqty AS c2, ref_0.p_name AS c3, ref_0.p_brand AS c4
                FROM
                    main.partsupp AS ref_1
                    INNER JOIN main.lineitem AS ref_2 ON (ref_0.p_container IS NULL)
                WHERE ((0)
                    AND ((((1)
                                OR (0))
                            OR (((((EXISTS (
                                                    SELECT
                                                        ref_2.l_returnflag AS c0, ref_3.l_receiptdate AS c1, (
                                                            SELECT
                                                                o_clerk
                                                            FROM
                                                                main.orders
                                                            LIMIT 1 offset 1) AS c2
                                                    FROM
                                                        main.lineitem AS ref_3
                                                    WHERE (((1)
                                                            OR (0))
                                                        AND (ref_0.p_brand IS NOT NULL))
                                                    OR (1)
                                                LIMIT 110))
                                        OR (EXISTS (
                                                SELECT
                                                    ref_4.l_shipdate AS c0,
                                                    ref_0.p_type AS c1
                                                FROM
                                                    main.lineitem AS ref_4
                                                WHERE
                                                    ref_1.ps_supplycost IS NULL
                                                LIMIT 118)))
                                    AND ((((0)
                                                OR (1))
                                            OR ((0)
                                                OR (0)))
                                        OR (1)))
                                AND ((0)
                                    OR (0)))
                            OR (ref_0.p_retailprice IS NOT NULL)))
                    AND (EXISTS (
                            SELECT
                                ref_0.p_type AS c0,
                                ref_0.p_comment AS c1,
                                (
                                    SELECT
                                        n_name
                                    FROM
                                        main.nation
                                    LIMIT 1 offset 2) AS c2
                            FROM
                                main.customer AS ref_5
                            WHERE ((1)
                                AND ((EXISTS (
                                            SELECT
                                                ref_0.p_comment AS c0, ref_6.p_name AS c1, ref_0.p_partkey AS c2, ref_0.p_mfgr AS c3, ref_1.ps_partkey AS c4, ref_6.p_comment AS c5, ref_1.ps_comment AS c6, ref_1.ps_suppkey AS c7, ref_1.ps_availqty AS c8, ref_2.l_commitdate AS c9, ref_1.ps_suppkey AS c10, ref_6.p_size AS c11, ref_0.p_size AS c12, ref_2.l_linestatus AS c13, ref_0.p_comment AS c14, ref_5.c_mktsegment AS c15, ref_5.c_mktsegment AS c16, ref_5.c_name AS c17, ref_1.ps_availqty AS c18, ref_1.ps_comment AS c19, ref_1.ps_supplycost AS c20, ref_1.ps_suppkey AS c21, ref_5.c_address AS c22, ref_2.l_extendedprice AS c23, ref_6.p_size AS c24, (
                                                    SELECT
                                                        c_nationkey
                                                    FROM
                                                        main.customer
                                                    LIMIT 1 offset 3) AS c25,
                                                ref_5.c_nationkey AS c26,
                                                ref_6.p_name AS c27,
                                                (
                                                    SELECT
                                                        c_acctbal
                                                    FROM
                                                        main.customer
                                                    LIMIT 1 offset 6) AS c28,
                                                ref_5.c_name AS c29,
                                                ref_5.c_address AS c30,
                                                ref_2.l_extendedprice AS c31,
                                                ref_2.l_shipdate AS c32,
                                                ref_5.c_nationkey AS c33
                                            FROM
                                                main.part AS ref_6
                                            WHERE ((0)
                                                OR (1))
                                            AND (ref_1.ps_suppkey IS NULL)))
                                    AND (ref_1.ps_partkey IS NOT NULL)))
                            OR (0)))))
            AND (EXISTS (
                    SELECT
                        ref_7.l_shipdate AS c0, ref_0.p_container AS c1, ref_0.p_partkey AS c2, 34 AS c3
                    FROM
                        main.lineitem AS ref_7
                    WHERE
                        EXISTS (
                            SELECT
                                ref_0.p_name AS c0, ref_0.p_brand AS c1, ref_2.l_partkey AS c2, ref_8.l_shipinstruct AS c3, ref_1.ps_suppkey AS c4, ref_7.l_partkey AS c5, (
                                    SELECT
                                        l_linenumber
                                    FROM
                                        main.lineitem
                                    LIMIT 1 offset 2) AS c6,
                                ref_0.p_type AS c7,
                                ref_1.ps_suppkey AS c8
                            FROM
                                main.lineitem AS ref_8
                            WHERE ((ref_0.p_retailprice IS NOT NULL)
                                OR (ref_8.l_shipinstruct IS NULL))
                            AND (((0)
                                    OR (((0)
                                            OR (1))
                                        AND ((1)
                                            AND ((EXISTS (
                                                        SELECT
                                                            ref_9.n_comment AS c0, ref_8.l_extendedprice AS c1, 100 AS c2, ref_1.ps_comment AS c3, (
                                                                SELECT
                                                                    n_name
                                                                FROM
                                                                    main.nation
                                                                LIMIT 1 offset 6) AS c4,
                                                            (
                                                                SELECT
                                                                    n_comment
                                                                FROM
                                                                    main.nation
                                                                LIMIT 1 offset 8) AS c5,
                                                            ref_2.l_comment AS c6,
                                                            ref_9.n_name AS c7,
                                                            ref_7.l_shipinstruct AS c8,
                                                            ref_2.l_returnflag AS c9
                                                        FROM
                                                            main.nation AS ref_9
                                                        WHERE
                                                            0))
                                                    OR ((EXISTS (
                                                                SELECT
                                                                    72 AS c0, ref_1.ps_comment AS c1, ref_8.l_linenumber AS c2, ref_7.l_partkey AS c3
                                                                FROM
                                                                    main.orders AS ref_10
                                                                WHERE
                                                                    ref_2.l_commitdate IS NOT NULL))
                                                            AND ((EXISTS (
                                                                        SELECT
                                                                            (
                                                                                SELECT
                                                                                    c_mktsegment
                                                                                FROM
                                                                                    main.customer
                                                                                LIMIT 1 offset 1) AS c0,
                                                                            ref_0.p_brand AS c1,
                                                                            ref_0.p_comment AS c2,
                                                                            ref_8.l_shipmode AS c3,
                                                                            ref_0.p_name AS c4,
                                                                            (
                                                                                SELECT
                                                                                    ps_partkey
                                                                                FROM
                                                                                    main.partsupp
                                                                                LIMIT 1 offset 92) AS c5,
                                                                            ref_0.p_mfgr AS c6,
                                                                            ref_1.ps_comment AS c7,
                                                                            ref_7.l_extendedprice AS c8,
                                                                            ref_7.l_suppkey AS c9
                                                                        FROM
                                                                            main.customer AS ref_11
                                                                        WHERE
                                                                            EXISTS (
                                                                                SELECT
                                                                                    ref_8.l_linenumber AS c0, ref_12.ps_partkey AS c1, ref_2.l_quantity AS c2, ref_7.l_commitdate AS c3, (
                                                                                        SELECT
                                                                                            p_partkey
                                                                                        FROM
                                                                                            main.part
                                                                                        LIMIT 1 offset 2) AS c4,
                                                                                    ref_8.l_receiptdate AS c5,
                                                                                    ref_7.l_tax AS c6,
                                                                                    ref_7.l_tax AS c7,
                                                                                    ref_8.l_commitdate AS c8,
                                                                                    ref_12.ps_partkey AS c9,
                                                                                    ref_12.ps_partkey AS c10,
                                                                                    ref_1.ps_partkey AS c11,
                                                                                    ref_2.l_discount AS c12,
                                                                                    42 AS c13,
                                                                                    ref_2.l_discount AS c14,
                                                                                    (
                                                                                        SELECT
                                                                                            l_shipinstruct
                                                                                        FROM
                                                                                            main.lineitem
                                                                                        LIMIT 1 offset 2) AS c15,
                                                                                    ref_11.c_nationkey AS c16,
                                                                                    ref_0.p_mfgr AS c17,
                                                                                    (
                                                                                        SELECT
                                                                                            r_name
                                                                                        FROM
                                                                                            main.region
                                                                                        LIMIT 1 offset 6) AS c18,
                                                                                    ref_0.p_retailprice AS c19
                                                                                FROM
                                                                                    main.partsupp AS ref_12
                                                                                WHERE
                                                                                    0
                                                                                LIMIT 172)))
                                                                    OR (0)))))))
                                            AND (EXISTS (
                                                    SELECT
                                                        ref_0.p_name AS c0,
                                                        ref_13.c_nationkey AS c1,
                                                        ref_1.ps_suppkey AS c2,
                                                        ref_2.l_suppkey AS c3,
                                                        ref_13.c_comment AS c4,
                                                        (
                                                            SELECT
                                                                s_comment
                                                            FROM
                                                                main.supplier
                                                            LIMIT 1 offset 2) AS c5,
                                                        ref_13.c_address AS c6,
                                                        ref_13.c_name AS c7,
                                                        ref_8.l_quantity AS c8,
                                                        ref_0.p_name AS c9
                                                    FROM
                                                        main.customer AS ref_13
                                                    WHERE
                                                        EXISTS (
                                                            SELECT
                                                                92 AS c0, ref_7.l_quantity AS c1, ref_7.l_linenumber AS c2, ref_13.c_comment AS c3, ref_13.c_nationkey AS c4, ref_14.s_name AS c5, ref_7.l_comment AS c6
                                                            FROM
                                                                main.supplier AS ref_14
                                                            WHERE (((0)
                                                                    OR (ref_0.p_type IS NULL))
                                                                OR (EXISTS (
                                                                        SELECT
                                                                            ref_13.c_comment AS c0, ref_7.l_suppkey AS c1, ref_13.c_acctbal AS c2, ref_1.ps_suppkey AS c3, ref_14.s_acctbal AS c4, ref_15.o_orderkey AS c5
                                                                        FROM
                                                                            main.orders AS ref_15
                                                                        WHERE
                                                                            ref_1.ps_comment IS NULL)))
                                                                OR (0))
                                                        LIMIT 152)))
                                        LIMIT 147)
                                LIMIT 82))
                    LIMIT 87)) AS subq_0
        WHERE ((EXISTS (
                    SELECT
                        ref_16.o_totalprice AS c0, subq_0.c11 AS c1, subq_0.c4 AS c2, 5 AS c3, subq_0.c6 AS c4, subq_0.c3 AS c5, subq_0.c9 AS c6, ref_16.o_comment AS c7, subq_0.c5 AS c8, ref_16.o_orderstatus AS c9, ref_16.o_orderkey AS c10, subq_0.c8 AS c11, subq_0.c6 AS c12, ref_16.o_orderstatus AS c13, subq_0.c11 AS c14, subq_0.c0 AS c15, (
                            SELECT
                                p_type
                            FROM
                                main.part
                            LIMIT 1 offset 95) AS c16,
                        (
                            SELECT
                                o_custkey
                            FROM
                                main.orders
                            LIMIT 1 offset 3) AS c17,
                        ref_16.o_comment AS c18
                    FROM
                        main.orders AS ref_16
                    WHERE ((EXISTS (
                                SELECT
                                    subq_0.c11 AS c0, ref_17.s_nationkey AS c1, ref_17.s_address AS c2, ref_17.s_name AS c3, subq_0.c1 AS c4, 94 AS c5, ref_17.s_address AS c6, subq_0.c6 AS c7, ref_16.o_custkey AS c8, ref_17.s_comment AS c9, ref_17.s_suppkey AS c10
                                FROM
                                    main.supplier AS ref_17
                                WHERE
                                    ref_16.o_comment IS NOT NULL
                                LIMIT 53))
                        AND (((1)
                                OR (((
                                            SELECT
                                                s_name
                                            FROM
                                                main.supplier
                                            LIMIT 1 offset 2)
                                        IS NOT NULL)
                                    OR (73 IS NULL)))
                            AND (((((1)
                                            AND (0))
                                        OR (EXISTS (
                                                SELECT
                                                    (
                                                        SELECT
                                                            n_comment
                                                        FROM
                                                            main.nation
                                                        LIMIT 1 offset 5) AS c0,
                                                    ref_16.o_orderstatus AS c1,
                                                    ref_16.o_clerk AS c2,
                                                    subq_0.c8 AS c3,
                                                    ref_16.o_comment AS c4,
                                                    ref_18.n_nationkey AS c5,
                                                    ref_16.o_clerk AS c6,
                                                    ref_18.n_comment AS c7,
                                                    ref_16.o_totalprice AS c8,
                                                    ref_16.o_orderdate AS c9,
                                                    subq_0.c4 AS c10,
                                                    subq_0.c8 AS c11,
                                                    ref_16.o_orderkey AS c12
                                                FROM
                                                    main.nation AS ref_18
                                                WHERE
                                                    0
                                                LIMIT 123)))
                                    AND (((EXISTS (
                                                    SELECT
                                                        subq_0.c4 AS c0,
                                                        ref_16.o_orderdate AS c1,
                                                        ref_16.o_totalprice AS c2,
                                                        ref_19.ps_supplycost AS c3,
                                                        ref_19.ps_availqty AS c4,
                                                        ref_16.o_shippriority AS c5,
                                                        100 AS c6,
                                                        ref_19.ps_supplycost AS c7,
                                                        ref_16.o_orderkey AS c8,
                                                        subq_0.c9 AS c9
                                                    FROM
                                                        main.partsupp AS ref_19
                                                    WHERE
                                                        1
                                                    LIMIT 105))
                                            OR (ref_16.o_clerk IS NOT NULL))
                                        OR (0)))
                                OR (subq_0.c4 IS NULL))))
                    OR (1)))
            AND (((subq_0.c9 IS NOT NULL)
                    AND (((subq_0.c2 IS NULL)
                            AND (0))
                        OR ((0)
                            OR (0))))
                OR (1)))
        OR ((subq_0.c0 IS NULL)
            OR (((0)
                    AND (subq_0.c1 IS NOT NULL))
                OR (((((subq_0.c6 IS NULL)
                                OR (subq_0.c9 IS NULL))
                            OR (EXISTS (
                                    SELECT
                                        ref_20.p_name AS c0,
                                        subq_0.c2 AS c1,
                                        ref_20.p_partkey AS c2,
                                        ref_20.p_size AS c3,
                                        ref_20.p_container AS c4,
                                        85 AS c5,
                                        ref_20.p_container AS c6
                                    FROM
                                        main.part AS ref_20
                                    WHERE
                                        ref_20.p_name IS NOT NULL)))
                            AND ((0)
                                AND (EXISTS (
                                        SELECT
                                            subq_0.c9 AS c0, subq_0.c3 AS c1, ref_21.o_comment AS c2, ref_21.o_orderkey AS c3
                                        FROM
                                            main.orders AS ref_21
                                        WHERE
                                            0
                                        LIMIT 108))))
                        AND ((subq_0.c11 IS NOT NULL)
                            AND (subq_0.c3 IS NOT NULL)))))
        LIMIT 137
),
jennifer_1 AS (
    SELECT
        ref_23.p_name AS c0,
        ref_22.c_phone AS c1,
        CAST(nullif (ref_24.o_totalprice, ref_24.o_totalprice) AS DECIMAL) AS c2,
        ref_23.p_mfgr AS c3,
        ref_24.o_clerk AS c4,
        ref_23.p_type AS c5,
        ref_25.n_regionkey AS c6,
        ref_22.c_name AS c7,
        ref_24.o_totalprice AS c8,
        ref_22.c_custkey AS c9,
        (
            SELECT
                l_discount
            FROM
                main.lineitem
            LIMIT 1 offset 4) AS c10,
        (
            SELECT
                l_shipinstruct
            FROM
                main.lineitem
            LIMIT 1 offset 2) AS c11,
        ref_23.p_brand AS c12,
        ref_25.n_comment AS c13,
        ref_23.p_size AS c14
    FROM
        main.customer AS ref_22
    LEFT JOIN main.part AS ref_23
    LEFT JOIN main.orders AS ref_24
    INNER JOIN main.nation AS ref_25 ON ((EXISTS (
                SELECT
                    ref_24.o_shippriority AS c0,
                    ref_25.n_name AS c1,
                    ref_24.o_shippriority AS c2,
                    (
                        SELECT
                            n_regionkey
                        FROM
                            main.nation
                        LIMIT 1 offset 4) AS c3,
                    ref_25.n_nationkey AS c4,
                    ref_26.o_custkey AS c5,
                    ref_24.o_shippriority AS c6,
                    ref_26.o_orderstatus AS c7,
                    ref_25.n_comment AS c8,
                    ref_26.o_shippriority AS c9,
                    ref_26.o_totalprice AS c10
                FROM
                    main.orders AS ref_26
                WHERE
                    1
                LIMIT 99))
        AND ((((EXISTS (
                            SELECT
                                ref_24.o_totalprice AS c0,
                                ref_24.o_clerk AS c1
                            FROM
                                main.supplier AS ref_27
                            WHERE
                                1))
                        AND (ref_24.o_custkey IS NULL))
                    AND ((((((1)
                                        OR ((((ref_25.n_regionkey IS NULL)
                                                    OR (1))
                                                AND (EXISTS (
                                                        SELECT
                                                            ref_24.o_orderpriority AS c0, ref_28.s_phone AS c1, ref_28.s_comment AS c2, ref_28.s_suppkey AS c3, ref_24.o_orderkey AS c4, ref_25.n_comment AS c5, ref_24.o_totalprice AS c6, ref_25.n_name AS c7, ref_24.o_totalprice AS c8, ref_24.o_orderstatus AS c9, ref_24.o_orderdate AS c10, (
                                                                SELECT
                                                                    o_custkey
                                                                FROM
                                                                    main.orders
                                                                LIMIT 1 offset 3) AS c11
                                                        FROM
                                                            main.supplier AS ref_28
                                                        WHERE ((0)
                                                            OR ((((0)
                                                                        AND (1))
                                                                    OR (1))
                                                                OR ((((
                                                                                SELECT
                                                                                    l_commitdate
                                                                                FROM
                                                                                    main.lineitem
                                                                                LIMIT 1 offset 3)
                                                                            IS NOT NULL)
                                                                        OR (0))
                                                                    AND (ref_25.n_comment IS NULL))))
                                                        AND (0)
                                                    LIMIT 117)))
                                        OR (((1)
                                                OR (0))
                                            OR (0))))
                                AND (1))
                            AND (85 IS NOT NULL))
                        OR (((ref_25.n_name IS NOT NULL)
                                OR (EXISTS (
                                        SELECT
                                            ref_25.n_name AS c0,
                                            ref_24.o_orderkey AS c1,
                                            ref_25.n_nationkey AS c2,
                                            14 AS c3,
                                            ref_24.o_orderkey AS c4,
                                            (
                                                SELECT
                                                    o_clerk
                                                FROM
                                                    main.orders
                                                LIMIT 1 offset 2) AS c5,
                                            ref_24.o_clerk AS c6
                                        FROM
                                            main.customer AS ref_29
                                        WHERE (1)
                                        AND (0))))
                            AND (((1)
                                    OR (0))
                                OR (EXISTS (
                                        SELECT
                                            ref_25.n_name AS c0, ref_30.r_comment AS c1, ref_25.n_comment AS c2, ref_30.r_regionkey AS c3, ref_30.r_comment AS c4, ref_25.n_regionkey AS c5, ref_25.n_comment AS c6
                                        FROM
                                            main.region AS ref_30
                                        WHERE (0)
                                        OR (ref_24.o_custkey IS NULL)
                                    LIMIT 124)))))
                AND (0)))
        AND ((((ref_25.n_name IS NOT NULL)
                    AND ((EXISTS (
                                SELECT
                                    ref_25.n_regionkey AS c0,
                                    ref_25.n_regionkey AS c1,
                                    ref_31.s_nationkey AS c2,
                                    ref_25.n_nationkey AS c3
                                FROM
                                    main.supplier AS ref_31
                                WHERE
                                    EXISTS (
                                        SELECT
                                            (
                                                SELECT
                                                    o_orderkey
                                                FROM
                                                    main.orders
                                                LIMIT 1 offset 4) AS c0,
                                            ref_32.c_acctbal AS c1,
                                            (
                                                SELECT
                                                    l_commitdate
                                                FROM
                                                    main.lineitem
                                                LIMIT 1 offset 4) AS c2,
                                            ref_25.n_name AS c3,
                                            ref_32.c_acctbal AS c4,
                                            ref_24.o_orderdate AS c5,
                                            ref_24.o_shippriority AS c6,
                                            ref_24.o_custkey AS c7,
                                            ref_25.n_regionkey AS c8,
                                            ref_31.s_nationkey AS c9
                                        FROM
                                            main.customer AS ref_32
                                        WHERE
                                            ref_31.s_name IS NULL)))
                                AND (EXISTS (
                                        SELECT
                                            ref_33.r_regionkey AS c0, ref_24.o_orderdate AS c1, ref_33.r_comment AS c2, ref_24.o_clerk AS c3, ref_24.o_shippriority AS c4, ref_24.o_orderstatus AS c5, ref_33.r_comment AS c6, ref_25.n_nationkey AS c7, (
                                                SELECT
                                                    p_type
                                                FROM
                                                    main.part
                                                LIMIT 1 offset 3) AS c8,
                                            ref_25.n_nationkey AS c9,
                                            ref_25.n_comment AS c10,
                                            ref_24.o_orderstatus AS c11,
                                            ref_25.n_nationkey AS c12,
                                            ref_24.o_orderpriority AS c13,
                                            ref_25.n_nationkey AS c14
                                        FROM
                                            main.region AS ref_33
                                        WHERE
                                            1
                                        LIMIT 39))))
                        OR (ref_25.n_name IS NULL))
                    OR ((1)
                        OR (EXISTS (
                                SELECT
                                    ref_24.o_orderdate AS c0,
                                    ref_24.o_orderkey AS c1,
                                    ref_34.o_comment AS c2
                                FROM
                                    main.orders AS ref_34
                                WHERE (1)
                                AND (ref_34.o_custkey IS NULL)
                            LIMIT 124)))))) ON (ref_24.o_shippriority IS NULL) ON (ref_22.c_nationkey = ref_25.n_nationkey)
WHERE ((ref_25.n_regionkey IS NOT NULL)
    OR (((EXISTS (
                    SELECT
                        ref_25.n_regionkey AS c0, ref_25.n_comment AS c1, (
                            SELECT
                                l_extendedprice
                            FROM
                                main.lineitem
                            LIMIT 1 offset 3) AS c2,
                        ref_23.p_retailprice AS c3,
                        ref_35.l_comment AS c4,
                        (
                            SELECT
                                c_address
                            FROM
                                main.customer
                            LIMIT 1 offset 4) AS c5,
                        (
                            SELECT
                                n_nationkey
                            FROM
                                main.nation
                            LIMIT 1 offset 6) AS c6,
                        ref_23.p_partkey AS c7,
                        ref_25.n_name AS c8,
                        ref_35.l_comment AS c9,
                        ref_35.l_partkey AS c10,
                        40 AS c11,
                        ref_22.c_address AS c12,
                        ref_35.l_receiptdate AS c13,
                        ref_22.c_phone AS c14,
                        ref_24.o_orderkey AS c15,
                        ref_35.l_comment AS c16,
                        ref_23.p_retailprice AS c17,
                        63 AS c18
                    FROM
                        main.lineitem AS ref_35
                    WHERE
                        0
                    LIMIT 104))
            OR (1))
        OR (((((((((1)
                                        AND (ref_23.p_type IS NULL))
                                    AND (0))
                                AND ((0)
                                    AND ((1)
                                        OR (((EXISTS (
                                                        SELECT
                                                            ref_24.o_clerk AS c0,
                                                            ref_23.p_container AS c1,
                                                            ref_22.c_custkey AS c2,
                                                            ref_24.o_custkey AS c3,
                                                            ref_25.n_regionkey AS c4,
                                                            ref_23.p_name AS c5,
                                                            53 AS c6,
                                                            ref_24.o_totalprice AS c7,
                                                            ref_23.p_container AS c8,
                                                            ref_25.n_name AS c9,
                                                            ref_22.c_mktsegment AS c10,
                                                            ref_23.p_name AS c11,
                                                            ref_36.o_orderdate AS c12,
                                                            ref_36.o_orderstatus AS c13,
                                                            ref_22.c_phone AS c14,
                                                            ref_22.c_name AS c15,
                                                            ref_25.n_name AS c16,
                                                            ref_25.n_name AS c17
                                                        FROM
                                                            main.orders AS ref_36
                                                        WHERE
                                                            0
                                                        LIMIT 124))
                                                AND (ref_23.p_retailprice IS NULL))
                                            OR (EXISTS (
                                                    SELECT
                                                        ref_25.n_name AS c0,
                                                        ref_22.c_mktsegment AS c1,
                                                        ref_37.c_comment AS c2,
                                                        ref_24.o_custkey AS c3,
                                                        ref_23.p_retailprice AS c4,
                                                        ref_37.c_comment AS c5,
                                                        (
                                                            SELECT
                                                                n_regionkey
                                                            FROM
                                                                main.nation
                                                            LIMIT 1 offset 1) AS c6,
                                                        ref_22.c_custkey AS c7,
                                                        ref_24.o_orderdate AS c8,
                                                        ref_25.n_nationkey AS c9
                                                    FROM
                                                        main.customer AS ref_37
                                                    WHERE (1)
                                                    OR (((1)
                                                            AND (ref_24.o_orderdate IS NOT NULL))
                                                        AND ((ref_23.p_partkey IS NULL)
                                                            AND (ref_25.n_regionkey IS NOT NULL)))
                                                LIMIT 117))))))
                        OR ((1)
                            AND (0)))
                    OR (ref_25.n_nationkey IS NULL))
                AND ((1)
                    AND (1)))
            OR (((ref_22.c_mktsegment IS NOT NULL)
                    OR ((ref_23.p_container IS NOT NULL)
                        OR (1)))
                AND (((((1)
                                AND ((ref_25.n_nationkey IS NOT NULL)
                                    OR (1)))
                            OR (1))
                        AND (ref_24.o_comment IS NULL))
                    OR (((
                                SELECT
                                    ps_partkey
                                FROM
                                    main.partsupp
                                LIMIT 1 offset 2)
                            IS NOT NULL)
                        AND (((ref_22.c_custkey IS NULL)
                                AND (1))
                            OR (EXISTS (
                                    SELECT
                                        ref_24.o_totalprice AS c0,
                                        ref_23.p_container AS c1
                                    FROM
                                        main.supplier AS ref_38
                                    WHERE (EXISTS (
                                            SELECT
                                                ref_25.n_name AS c0, ref_24.o_totalprice AS c1, ref_22.c_custkey AS c2, ref_38.s_suppkey AS c3, ref_39.r_regionkey AS c4, 25 AS c5, ref_39.r_regionkey AS c6, ref_22.c_mktsegment AS c7, 19 AS c8, ref_39.r_name AS c9, ref_22.c_acctbal AS c10, ref_25.n_comment AS c11, ref_22.c_comment AS c12, ref_39.r_comment AS c13, ref_39.r_comment AS c14, ref_24.o_custkey AS c15, ref_24.o_totalprice AS c16, ref_24.o_shippriority AS c17, (
                                                    SELECT
                                                        r_name
                                                    FROM
                                                        main.region
                                                    LIMIT 1 offset 1) AS c18
                                            FROM
                                                main.region AS ref_39
                                            WHERE (ref_23.p_type IS NULL)
                                            AND ((((1)
                                                        AND (ref_22.c_name IS NOT NULL))
                                                    OR (0))
                                                AND (ref_23.p_retailprice IS NULL))
                                        LIMIT 96))
                                AND (ref_25.n_name IS NULL))))))))
    OR (ref_24.o_orderdate IS NULL))))
OR (ref_23.p_partkey IS NULL)
LIMIT 17
)
SELECT
    subq_1.c0 AS c0,
    subq_1.c9 AS c1,
    CAST(nullif (
            CASE WHEN EXISTS (
                    SELECT
                        DISTINCT subq_1.c9 AS c0, subq_1.c3 AS c1, subq_1.c8 AS c2, ref_44.o_clerk AS c3, ref_44.o_orderpriority AS c4, (
                            SELECT
                                r_comment FROM main.region
                            LIMIT 1 offset 96) AS c5, (
        SELECT
            r_comment FROM main.region
        LIMIT 1 offset 5) AS c6, subq_1.c0 AS c7 FROM main.orders AS ref_44
WHERE
    0) THEN
subq_1.c5
ELSE
    subq_1.c5
END, subq_1.c4) AS VARCHAR) AS c2, (
        SELECT
            ps_availqty
        FROM
            main.partsupp
        LIMIT 1 offset 4) AS c3
FROM (
    SELECT
        ref_40.c_acctbal AS c0,
        ref_42.l_suppkey AS c1,
        ref_42.l_tax AS c2,
        (
            SELECT
                l_extendedprice
            FROM
                main.lineitem
            LIMIT 1 offset 3) AS c3,
        ref_43.p_comment AS c4,
        ref_40.c_name AS c5,
        ref_43.p_name AS c6,
        ref_42.l_discount AS c7,
        ref_43.p_brand AS c8,
        ref_42.l_suppkey AS c9
    FROM
        main.customer AS ref_40
        INNER JOIN main.part AS ref_41 ON (ref_40.c_phone IS NOT NULL)
        LEFT JOIN main.lineitem AS ref_42
        INNER JOIN main.part AS ref_43 ON (ref_43.p_brand IS NULL) ON (ref_40.c_mktsegment = ref_43.p_name)
    WHERE (
        SELECT
            p_partkey
        FROM
            main.part
        LIMIT 1 offset 5)
    IS NULL
LIMIT 92) AS subq_1
WHERE
    0
LIMIT 92
