WITH jennifer_0 AS (
    SELECT
        ref_1.c_name AS c0,
        (
            SELECT
                ps_supplycost
            FROM
                main.partsupp
            LIMIT 1 offset 73) AS c1,
        sample_1.r_comment AS c2,
        ref_0.l_linenumber AS c3,
        CASE WHEN ((EXISTS (
                    SELECT
                        sample_1.r_comment AS c0,
                        sample_0.o_orderkey AS c1,
                        ref_5.ps_partkey AS c2,
                        ref_0.l_shipmode AS c3,
                        ref_1.c_mktsegment AS c4,
                        ref_5.ps_partkey AS c5,
                        sample_2.r_comment AS c6,
                        sample_0.o_orderstatus AS c7
                    FROM
                        main.partsupp AS ref_5
                    WHERE
                        EXISTS (
                            SELECT
                                ref_1.c_custkey AS c0,
                                sample_1.r_comment AS c1,
                                sample_0.o_shippriority AS c2,
                                ref_0.l_comment AS c3,
                                15 AS c4,
                                sample_0.o_orderkey AS c5
                            FROM
                                main.orders AS ref_6
                            WHERE
                                1
                            LIMIT 96)
                    LIMIT 128))
            AND (ref_1.c_custkey IS NULL))
            AND ((0)
                AND (1)) THEN
            ref_1.c_name
        ELSE
            ref_1.c_name
        END AS c4
    FROM
        main.lineitem AS ref_0
    RIGHT JOIN main.orders AS sample_0 TABLESAMPLE SYSTEM (3.9) ON ((0)
            OR (1))
    RIGHT JOIN main.region AS sample_1 TABLESAMPLE SYSTEM (0.3)
    LEFT JOIN main.region AS sample_2 TABLESAMPLE SYSTEM (8.3)
    RIGHT JOIN main.customer AS ref_1 ON (sample_2.r_comment = ref_1.c_name) ON ((1)
            AND ((sample_1.r_name IS NOT NULL)
                OR ((((EXISTS (
                                    SELECT
                                        sample_1.r_regionkey AS c0,
                                        sample_2.r_name AS c1,
                                        sample_1.r_regionkey AS c2,
                                        sample_2.r_comment AS c3
                                    FROM
                                        main.region AS sample_3 TABLESAMPLE SYSTEM (1.3)
                                WHERE ((EXISTS (
                                            SELECT
                                                sample_2.r_regionkey AS c0
                                            FROM
                                                main.supplier AS sample_4 TABLESAMPLE SYSTEM (4.2)
                                            WHERE
                                                1
                                            LIMIT 81))
                                    AND (1))
                                AND (EXISTS (
                                        SELECT
                                            sample_2.r_name AS c0,
                                            ref_1.c_name AS c1,
                                            sample_5.n_name AS c2
                                        FROM
                                            main.nation AS sample_5 TABLESAMPLE SYSTEM (3.5)
                                        WHERE
                                            sample_2.r_name IS NOT NULL
                                        LIMIT 77))))
                        AND (sample_1.r_name IS NOT NULL))
                    OR (EXISTS (
                            SELECT
                                (
                                    SELECT
                                        ps_partkey
                                    FROM
                                        main.partsupp
                                    LIMIT 1 offset 4) AS c0
                            FROM
                                main.partsupp AS ref_2
                            WHERE
                                EXISTS (
                                    SELECT
                                        sample_2.r_regionkey AS c0,
                                        sample_6.p_partkey AS c1,
                                        ref_1.c_name AS c2,
                                        sample_1.r_comment AS c3,
                                        sample_6.p_type AS c4,
                                        (
                                            SELECT
                                                ps_comment
                                            FROM
                                                main.partsupp
                                            LIMIT 1 offset 6) AS c5,
                                        ref_2.ps_supplycost AS c6,
                                        sample_6.p_brand AS c7,
                                        sample_6.p_partkey AS c8
                                    FROM
                                        main.part AS sample_6 TABLESAMPLE BERNOULLI (7.3)
                                    WHERE
                                        1
                                    LIMIT 104)
                            LIMIT 34)))
                OR ((((EXISTS (
                                    SELECT
                                        sample_1.r_comment AS c0,
                                        sample_1.r_regionkey AS c1,
                                        ref_1.c_custkey AS c2,
                                        sample_2.r_comment AS c3,
                                        sample_1.r_regionkey AS c4,
                                        sample_7.c_acctbal AS c5,
                                        sample_2.r_name AS c6,
                                        sample_7.c_address AS c7
                                    FROM
                                        main.customer AS sample_7 TABLESAMPLE BERNOULLI (3.1)
                                    WHERE ((1)
                                        OR ((EXISTS (
                                                    SELECT
                                                        sample_2.r_regionkey AS c0,
                                                        sample_1.r_regionkey AS c1,
                                                        ref_1.c_acctbal AS c2,
                                                        ref_1.c_phone AS c3,
                                                        98 AS c4,
                                                        sample_1.r_regionkey AS c5,
                                                        sample_2.r_name AS c6
                                                    FROM
                                                        main.nation AS sample_8 TABLESAMPLE BERNOULLI (8.9)
                                                    WHERE (0)
                                                    OR ((0)
                                                        OR (1))
                                                LIMIT 180))
                                        OR (1)))
                                OR (1)))
                        AND ((0)
                            AND (EXISTS (
                                    SELECT
                                        sample_1.r_regionkey AS c0,
                                        sample_2.r_name AS c1
                                    FROM
                                        main.nation AS ref_3
                                    WHERE (((((ref_1.c_mktsegment IS NULL)
                                                    OR (1))
                                                OR (0))
                                            AND (sample_2.r_regionkey IS NOT NULL))
                                        OR (1))
                                    AND (0)
                                LIMIT 47))))
                AND (EXISTS (
                        SELECT
                            sample_1.r_comment AS c0
                        FROM
                            main.lineitem AS sample_9 TABLESAMPLE SYSTEM (5.2)
                        WHERE (sample_1.r_name IS NULL)
                        OR (((EXISTS (
                                        SELECT
                                            sample_10.o_orderdate AS c0,
                                            sample_9.l_comment AS c1,
                                            sample_10.o_comment AS c2,
                                            sample_10.o_orderpriority AS c3
                                        FROM
                                            main.orders AS sample_10 TABLESAMPLE BERNOULLI (7.1)
                                        WHERE
                                            0
                                        LIMIT 62))
                                OR ((1)
                                    OR (EXISTS (
                                            SELECT
                                                sample_2.r_name AS c0,
                                                ref_1.c_custkey AS c1,
                                                sample_1.r_comment AS c2
                                            FROM
                                                main.part AS sample_11 TABLESAMPLE SYSTEM (6)
                                            WHERE (1)
                                            AND ((1)
                                                OR (sample_1.r_comment IS NOT NULL))))))
                            OR (0))
                    LIMIT 77)))
        OR (EXISTS (
                SELECT
                    sample_2.r_comment AS c0,
                    (
                        SELECT
                            r_comment
                        FROM
                            main.region
                        LIMIT 1 offset 6) AS c1,
                    ref_1.c_address AS c2,
                    sample_2.r_name AS c3,
                    ref_1.c_mktsegment AS c4,
                    ref_1.c_address AS c5,
                    ref_1.c_phone AS c6,
                    sample_1.r_regionkey AS c7,
                    ref_1.c_nationkey AS c8,
                    ref_1.c_mktsegment AS c9,
                    ref_1.c_nationkey AS c10,
                    sample_1.r_comment AS c11,
                    (
                        SELECT
                            n_nationkey
                        FROM
                            main.nation
                        LIMIT 1 offset 1) AS c12
                FROM
                    main.lineitem AS ref_4
                WHERE
                    sample_2.r_comment IS NOT NULL
                LIMIT 107)))))) ON (sample_0.o_totalprice = ref_1.c_acctbal)
WHERE (EXISTS (
        SELECT
            sample_1.r_name AS c0,
            sample_15.s_comment AS c1,
            sample_1.r_name AS c2,
            ref_12.p_type AS c3,
            sample_16.p_partkey AS c4,
            sample_1.r_regionkey AS c5,
            ref_0.l_discount AS c6,
            ref_0.l_orderkey AS c7,
            sample_14.o_orderstatus AS c8,
            ref_1.c_acctbal AS c9,
            sample_14.o_custkey AS c10,
            sample_16.p_partkey AS c11,
            sample_15.s_comment AS c12,
            sample_1.r_comment AS c13,
            sample_1.r_name AS c14,
            sample_0.o_custkey AS c15
        FROM
            main.orders AS sample_14 TABLESAMPLE SYSTEM (8.1)
                INNER JOIN main.supplier AS sample_15 TABLESAMPLE SYSTEM (8.3) ON (sample_14.o_clerk = sample_15.s_name)
                INNER JOIN main.part AS ref_12
                RIGHT JOIN main.part AS sample_16 TABLESAMPLE SYSTEM (2.8)
                INNER JOIN main.customer AS sample_17 TABLESAMPLE BERNOULLI (1.6) ON (1) ON ((EXISTS (
                            SELECT
                                ref_1.c_mktsegment AS c0,
                                ref_0.l_shipdate AS c1
                            FROM
                                main.part AS sample_18 TABLESAMPLE BERNOULLI (0.7)
                            WHERE
                                1
                            LIMIT 45))
                    AND ((
                            SELECT
                                l_discount
                            FROM
                                main.lineitem
                            LIMIT 1 offset 3) IS NULL)) ON (sample_2.r_regionkey IS NOT NULL)
            WHERE
                sample_1.r_regionkey IS NOT NULL
            LIMIT 184))
    OR ((30 IS NOT NULL)
        AND (((sample_2.r_name IS NOT NULL)
                AND (EXISTS (
                        SELECT
                            ref_1.c_custkey AS c0,
                            sample_2.r_name AS c1,
                            sample_2.r_comment AS c2,
                            sample_2.r_name AS c3,
                            ref_1.c_comment AS c4,
                            ref_0.l_orderkey AS c5,
                            ref_1.c_phone AS c6,
                            sample_19.ps_supplycost AS c7,
                            sample_2.r_comment AS c8,
                            85 AS c9,
                            sample_19.ps_supplycost AS c10,
                            sample_0.o_orderpriority AS c11
                        FROM
                            main.partsupp AS sample_19 TABLESAMPLE BERNOULLI (8.2)
                        WHERE (((1)
                                AND (EXISTS (
                                        SELECT
                                            sample_0.o_orderpriority AS c0,
                                            ref_1.c_acctbal AS c1
                                        FROM
                                            main.nation AS ref_13
                                        WHERE ((((EXISTS (
                                                            SELECT
                                                                ref_1.c_address AS c0,
                                                                ref_1.c_custkey AS c1,
                                                                sample_1.r_name AS c2,
                                                                sample_20.r_regionkey AS c3,
                                                                ref_13.n_nationkey AS c4,
                                                                (
                                                                    SELECT
                                                                        p_brand
                                                                    FROM
                                                                        main.part
                                                                    LIMIT 1 offset 1) AS c5,
                                                                sample_1.r_name AS c6,
                                                                sample_1.r_comment AS c7,
                                                                sample_20.r_regionkey AS c8,
                                                                sample_1.r_comment AS c9,
                                                                sample_2.r_comment AS c10,
                                                                sample_20.r_regionkey AS c11,
                                                                sample_20.r_comment AS c12,
                                                                sample_0.o_orderdate AS c13,
                                                                (
                                                                    SELECT
                                                                        n_regionkey
                                                                    FROM
                                                                        main.nation
                                                                    LIMIT 1 offset 3) AS c14,
                                                                ref_0.l_extendedprice AS c15
                                                            FROM
                                                                main.region AS sample_20 TABLESAMPLE SYSTEM (8.9)
                                                            WHERE ((0)
                                                                OR ((0)
                                                                    AND ((1)
                                                                        OR (0))))
                                                            AND (0)))
                                                    OR (EXISTS (
                                                            SELECT
                                                                sample_1.r_regionkey AS c0,
                                                                ref_13.n_nationkey AS c1,
                                                                sample_0.o_custkey AS c2,
                                                                sample_0.o_orderdate AS c3,
                                                                ref_13.n_name AS c4,
                                                                ref_13.n_nationkey AS c5,
                                                                ref_0.l_shipmode AS c6,
                                                                ref_0.l_orderkey AS c7,
                                                                ref_0.l_suppkey AS c8,
                                                                ref_13.n_regionkey AS c9,
                                                                ref_1.c_acctbal AS c10,
                                                                ref_1.c_acctbal AS c11,
                                                                ref_13.n_regionkey AS c12,
                                                                sample_2.r_comment AS c13
                                                            FROM
                                                                main.region AS ref_14
                                                            WHERE
                                                                0)))
                                                    AND (0))
                                                OR ((sample_2.r_comment IS NOT NULL)
                                                    AND (((EXISTS (
                                                                    SELECT
                                                                        sample_1.r_comment AS c0,
                                                                        ref_1.c_custkey AS c1,
                                                                        sample_0.o_orderpriority AS c2,
                                                                        sample_1.r_comment AS c3
                                                                    FROM
                                                                        main.orders AS sample_21 TABLESAMPLE BERNOULLI (1.3)
                                                                    WHERE ((0)
                                                                        OR ((0)
                                                                            AND ((sample_1.r_regionkey IS NOT NULL)
                                                                                OR ((0)
                                                                                    AND (0)))))
                                                                    OR (sample_2.r_comment IS NULL)
                                                                LIMIT 145))
                                                        OR (1))
                                                    AND ((0)
                                                        OR ((ref_13.n_comment IS NOT NULL)
                                                            AND (1))))))
                                        AND (0))))
                            AND (EXISTS (
                                    SELECT
                                        ref_15.p_retailprice AS c0,
                                        sample_1.r_regionkey AS c1,
                                        89 AS c2,
                                        sample_1.r_name AS c3,
                                        sample_19.ps_supplycost AS c4,
                                        sample_2.r_name AS c5,
                                        sample_2.r_comment AS c6,
                                        sample_1.r_name AS c7,
                                        sample_2.r_comment AS c8,
                                        sample_19.ps_supplycost AS c9,
                                        sample_0.o_shippriority AS c10,
                                        62 AS c11,
                                        sample_0.o_custkey AS c12,
                                        ref_15.p_partkey AS c13,
                                        sample_1.r_regionkey AS c14,
                                        (
                                            SELECT
                                                p_name
                                            FROM
                                                main.part
                                            LIMIT 1 offset 1) AS c15,
                                        ref_15.p_mfgr AS c16,
                                        ref_0.l_partkey AS c17,
                                        ref_1.c_comment AS c18
                                    FROM
                                        main.part AS ref_15
                                    WHERE
                                        1
                                    LIMIT 108)))
                        OR (sample_0.o_orderstatus IS NOT NULL))))
            AND ((((1)
                        OR (((ref_0.l_partkey IS NOT NULL)
                                OR ((ref_1.c_address IS NOT NULL)
                                    OR (0)))
                            AND (sample_0.o_custkey IS NULL)))
                    OR (EXISTS (
                            SELECT
                                ref_0.l_shipinstruct AS c0,
                                sample_0.o_orderkey AS c1,
                                29 AS c2
                            FROM
                                main.orders AS sample_22 TABLESAMPLE SYSTEM (8.8)
                            WHERE (EXISTS (
                                    SELECT
                                        sample_22.o_clerk AS c0
                                    FROM
                                        main.partsupp AS sample_23 TABLESAMPLE SYSTEM (0.1)
                                    WHERE
                                        sample_23.ps_supplycost IS NULL
                                    LIMIT 122))
                            AND (sample_1.r_regionkey IS NULL)
                        LIMIT 103)))
            AND (sample_1.r_name IS NULL))))
LIMIT 12
),
jennifer_1 AS (
    SELECT
        (
            SELECT
                r_comment
            FROM
                main.region
            LIMIT 1 offset 26) AS c0,
        CASE WHEN (((subq_0.c0 IS NOT NULL)
                AND ((0)
                    AND (EXISTS (
                            SELECT
                                subq_0.c1 AS c0,
                                82 AS c1,
                                ref_19.r_regionkey AS c2,
                                subq_0.c0 AS c3,
                                ref_19.r_comment AS c4,
                                ref_19.r_comment AS c5,
                                ref_19.r_name AS c6,
                                ref_19.r_regionkey AS c7,
                                ref_19.r_regionkey AS c8
                            FROM
                                main.region AS ref_19
                            WHERE
                                1))))
                OR (0))
            OR (EXISTS (
                    SELECT
                        29 AS c0,
                        subq_0.c0 AS c1,
                        ref_20.o_clerk AS c2,
                        14 AS c3,
                        (
                            SELECT
                                n_nationkey
                            FROM
                                main.nation
                            LIMIT 1 offset 2) AS c4,
                        ref_20.o_shippriority AS c5,
                        subq_0.c1 AS c6,
                        subq_0.c1 AS c7,
                        ref_20.o_orderpriority AS c8,
                        ref_20.o_clerk AS c9
                    FROM
                        main.orders AS ref_20
                    WHERE (ref_20.o_custkey IS NOT NULL)
                    AND (1)
                LIMIT 129)) THEN
            subq_0.c0
        ELSE
            subq_0.c0
        END AS c1,
        subq_0.c0 AS c2
    FROM (
        SELECT
            ref_16.ps_comment AS c0,
            ref_16.ps_availqty AS c1
        FROM
            main.partsupp AS ref_16
        WHERE ((1)
            OR ((((EXISTS (
                                SELECT
                                    sample_24.p_partkey AS c0,
                                    sample_24.p_retailprice AS c1,
                                    sample_24.p_comment AS c2,
                                    ref_16.ps_availqty AS c3,
                                    sample_24.p_retailprice AS c4,
                                    sample_24.p_mfgr AS c5,
                                    ref_16.ps_partkey AS c6,
                                    sample_24.p_name AS c7,
                                    ref_16.ps_availqty AS c8
                                FROM
                                    main.part AS sample_24 TABLESAMPLE SYSTEM (1.8)
                                WHERE (1)
                                AND (ref_16.ps_suppkey IS NOT NULL)))
                        OR (1))
                    AND (0))
                AND (EXISTS (
                        SELECT
                            ref_16.ps_suppkey AS c0
                        FROM
                            main.supplier AS ref_17
                        WHERE
                            EXISTS (
                                SELECT
                                    sample_25.ps_suppkey AS c0
                                FROM
                                    main.partsupp AS sample_25 TABLESAMPLE SYSTEM (9.6)
                                WHERE
                                    1)
                            LIMIT 100))))
            OR ((((EXISTS (
                                SELECT
                                    ref_18.o_comment AS c0
                                FROM
                                    main.orders AS ref_18
                                WHERE (ref_18.o_orderdate IS NULL)
                                OR ((
                                        SELECT
                                            o_orderstatus
                                        FROM
                                            main.orders
                                        LIMIT 1 offset 68) IS NOT NULL)
                            LIMIT 46))
                    OR ((((ref_16.ps_comment IS NULL)
                                OR (ref_16.ps_availqty IS NULL))
                            OR (1))
                        AND (((ref_16.ps_availqty IS NULL)
                                OR (1))
                            AND (ref_16.ps_suppkey IS NOT NULL))))
                AND ((((0)
                            AND (1))
                        OR (ref_16.ps_comment IS NOT NULL))
                    AND ((ref_16.ps_suppkey IS NULL)
                        AND (EXISTS (
                                SELECT
                                    sample_26.r_name AS c0,
                                    (
                                        SELECT
                                            l_receiptdate
                                        FROM
                                            main.lineitem
                                        LIMIT 1 offset 1) AS c1
                                FROM
                                    main.region AS sample_26 TABLESAMPLE SYSTEM (8.7)
                                WHERE
                                    ref_16.ps_comment IS NOT NULL
                                LIMIT 127)))))
            AND (EXISTS (
                    SELECT
                        ref_16.ps_comment AS c0,
                        sample_27.o_orderkey AS c1
                    FROM
                        main.orders AS sample_27 TABLESAMPLE BERNOULLI (6.5)
                    WHERE
                        ref_16.ps_suppkey IS NULL
                    LIMIT 117)))) AS subq_0
WHERE
    subq_0.c0 IS NULL
LIMIT 184
),
jennifer_2 AS (
    SELECT
        sample_28.r_regionkey AS c0,
        sample_28.r_name AS c1,
        sample_28.r_comment AS c2,
        sample_28.r_comment AS c3,
        31 AS c4,
        sample_28.r_regionkey AS c5,
        sample_28.r_comment AS c6
    FROM
        main.region AS sample_28 TABLESAMPLE SYSTEM (6)
    WHERE (((1)
            OR ((sample_28.r_name IS NULL)
                OR ((1)
                    OR ((((((EXISTS (
                                                SELECT
                                                    sample_28.r_comment AS c0,
                                                    sample_28.r_comment AS c1
                                                FROM
                                                    main.partsupp AS sample_29 TABLESAMPLE BERNOULLI (2.1)
                                                WHERE (0)
                                                OR ((0)
                                                    OR ((EXISTS (
                                                                SELECT
                                                                    sample_29.ps_partkey AS c0,
                                                                    sample_29.ps_partkey AS c1,
                                                                    sample_29.ps_partkey AS c2
                                                                FROM
                                                                    main.region AS sample_30 TABLESAMPLE SYSTEM (1.6)
                                                                WHERE (((1)
                                                                        OR (1))
                                                                    OR ((sample_30.r_name IS NULL)
                                                                        OR (1)))
                                                                AND (sample_29.ps_suppkey IS NULL)
                                                            LIMIT 158))
                                                    AND (1)))
                                        LIMIT 90))
                                AND (sample_28.r_regionkey IS NULL))
                            OR (0))
                        AND (EXISTS (
                                SELECT
                                    sample_28.r_name AS c0,
                                    sample_31.n_name AS c1,
                                    sample_31.n_regionkey AS c2,
                                    sample_28.r_regionkey AS c3,
                                    sample_28.r_name AS c4,
                                    sample_28.r_comment AS c5,
                                    sample_31.n_regionkey AS c6,
                                    sample_28.r_regionkey AS c7,
                                    sample_28.r_comment AS c8,
                                    sample_28.r_comment AS c9,
                                    sample_31.n_regionkey AS c10,
                                    sample_28.r_name AS c11,
                                    sample_31.n_regionkey AS c12,
                                    sample_28.r_name AS c13,
                                    sample_31.n_regionkey AS c14,
                                    sample_31.n_nationkey AS c15,
                                    sample_31.n_comment AS c16,
                                    sample_28.r_name AS c17,
                                    sample_31.n_regionkey AS c18,
                                    sample_28.r_name AS c19
                                FROM
                                    main.nation AS sample_31 TABLESAMPLE SYSTEM (9.9)
                                WHERE
                                    sample_31.n_nationkey IS NULL)))
                        OR ((1)
                            AND (EXISTS (
                                    SELECT
                                        ref_21.s_name AS c0,
                                        sample_28.r_comment AS c1,
                                        ref_21.s_name AS c2,
                                        sample_28.r_comment AS c3,
                                        sample_28.r_comment AS c4,
                                        ref_21.s_comment AS c5,
                                        sample_28.r_comment AS c6,
                                        sample_28.r_regionkey AS c7,
                                        sample_28.r_name AS c8,
                                        sample_28.r_comment AS c9
                                    FROM
                                        main.supplier AS ref_21
                                    WHERE
                                        1
                                    LIMIT 181))))
                    AND (0)))))
    AND (sample_28.r_name IS NOT NULL))
    OR ((((((EXISTS (
                                SELECT
                                    sample_28.r_name AS c0,
                                    ref_22.l_orderkey AS c1,
                                    (
                                        SELECT
                                            n_nationkey
                                        FROM
                                            main.nation
                                        LIMIT 1 offset 38) AS c2,
                                    ref_22.l_linenumber AS c3
                                FROM
                                    main.lineitem AS ref_22
                                WHERE ((((0)
                                            OR (0))
                                        OR ((0)
                                            OR (((((EXISTS (
                                                                    SELECT
                                                                        ref_22.l_quantity AS c0,
                                                                        sample_28.r_regionkey AS c1,
                                                                        ref_23.n_comment AS c2,
                                                                        sample_28.r_name AS c3,
                                                                        ref_22.l_partkey AS c4,
                                                                        sample_28.r_comment AS c5,
                                                                        sample_28.r_name AS c6,
                                                                        ref_23.n_name AS c7,
                                                                        sample_28.r_name AS c8,
                                                                        ref_22.l_quantity AS c9,
                                                                        ref_22.l_tax AS c10,
                                                                        ref_22.l_quantity AS c11
                                                                    FROM
                                                                        main.nation AS ref_23
                                                                    WHERE
                                                                        1))
                                                                OR (EXISTS (
                                                                        SELECT
                                                                            sample_32.l_tax AS c0,
                                                                            sample_28.r_regionkey AS c1,
                                                                            sample_28.r_regionkey AS c2,
                                                                            sample_32.l_tax AS c3
                                                                        FROM
                                                                            main.lineitem AS sample_32 TABLESAMPLE BERNOULLI (6.5)
                                                                        WHERE
                                                                            0
                                                                        LIMIT 58)))
                                                            OR (((EXISTS (
                                                                            SELECT
                                                                                sample_28.r_regionkey AS c0
                                                                            FROM
                                                                                main.orders AS ref_24
                                                                            WHERE
                                                                                1))
                                                                        OR (1))
                                                                    AND (sample_28.r_regionkey IS NULL)))
                                                            OR ((sample_28.r_regionkey IS NULL)
                                                                OR (1)))
                                                        AND ((((0)
                                                                    AND (((
                                                                                SELECT
                                                                                    n_name
                                                                                FROM
                                                                                    main.nation
                                                                                LIMIT 1 offset 3) IS NOT NULL)
                                                                        OR (sample_28.r_comment IS NOT NULL)))
                                                                AND ((EXISTS (
                                                                            SELECT
                                                                                ref_22.l_suppkey AS c0,
                                                                                ref_22.l_shipmode AS c1,
                                                                                ref_22.l_discount AS c2,
                                                                                sample_28.r_name AS c3,
                                                                                sample_28.r_name AS c4,
                                                                                sample_28.r_name AS c5,
                                                                                ref_22.l_returnflag AS c6,
                                                                                ref_25.n_comment AS c7
                                                                            FROM
                                                                                main.nation AS ref_25
                                                                            WHERE
                                                                                1
                                                                            LIMIT 62))
                                                                    OR ((0)
                                                                        AND (sample_28.r_name IS NULL))))
                                                            AND (((1)
                                                                    OR ((1)
                                                                        AND (sample_28.r_regionkey IS NULL)))
                                                                OR ((1)
                                                                    AND (sample_28.r_name IS NOT NULL)))))))
                                            AND (((0)
                                                    OR (((ref_22.l_comment IS NOT NULL)
                                                            AND (EXISTS (
                                                                    SELECT
                                                                        sample_28.r_comment AS c0
                                                                    FROM
                                                                        main.region AS ref_26
                                                                    WHERE (ref_22.l_tax IS NULL)
                                                                    OR (0)
                                                                LIMIT 132)))
                                                    OR (((EXISTS (
                                                                    SELECT
                                                                        sample_28.r_name AS c0,
                                                                        sample_28.r_comment AS c1
                                                                    FROM
                                                                        main.lineitem AS ref_27
                                                                    WHERE
                                                                        EXISTS (
                                                                            SELECT
                                                                                ref_22.l_tax AS c0,
                                                                                ref_22.l_linenumber AS c1,
                                                                                (
                                                                                    SELECT
                                                                                        n_name
                                                                                    FROM
                                                                                        main.nation
                                                                                    LIMIT 1 offset 24) AS c2,
                                                                                ref_27.l_discount AS c3,
                                                                                ref_27.l_suppkey AS c4
                                                                            FROM
                                                                                main.orders AS ref_28
                                                                            WHERE
                                                                                0)
                                                                        LIMIT 55))
                                                                OR ((ref_22.l_quantity IS NULL)
                                                                    OR (0)))
                                                            OR ((1)
                                                                OR (((
                                                                            SELECT
                                                                                p_comment
                                                                            FROM
                                                                                main.part
                                                                            LIMIT 1 offset 9) IS NOT NULL)
                                                                    OR (EXISTS (
                                                                            SELECT
                                                                                (
                                                                                    SELECT
                                                                                        n_comment
                                                                                    FROM
                                                                                        main.nation
                                                                                    LIMIT 1 offset 5) AS c0,
                                                                                sample_28.r_name AS c1
                                                                            FROM
                                                                                main.customer AS ref_29
                                                                            WHERE
                                                                                1)))))))
                                                    OR (EXISTS (
                                                            SELECT
                                                                ref_30.o_clerk AS c0,
                                                                ref_22.l_receiptdate AS c1,
                                                                ref_22.l_quantity AS c2
                                                            FROM
                                                                main.orders AS ref_30
                                                            WHERE
                                                                1
                                                            LIMIT 65))))
                                            OR (((sample_28.r_name IS NOT NULL)
                                                    AND ((0)
                                                        AND ((ref_22.l_suppkey IS NULL)
                                                            OR (0))))
                                                OR ((1)
                                                    AND (ref_22.l_tax IS NOT NULL)))))
                                    AND ((sample_28.r_name IS NOT NULL)
                                        OR (sample_28.r_comment IS NULL)))
                                AND (EXISTS (
                                        SELECT
                                            ref_31.n_comment AS c0,
                                            sample_28.r_regionkey AS c1,
                                            sample_28.r_comment AS c2,
                                            ref_31.n_comment AS c3,
                                            ref_31.n_regionkey AS c4
                                        FROM
                                            main.nation AS ref_31
                                        WHERE
                                            ref_31.n_regionkey IS NULL
                                        LIMIT 109)))
                            AND (((EXISTS (
                                            SELECT
                                                sample_28.r_regionkey AS c0,
                                                sample_28.r_comment AS c1,
                                                sample_28.r_name AS c2,
                                                sample_28.r_name AS c3,
                                                ref_32.r_name AS c4,
                                                ref_32.r_regionkey AS c5,
                                                ref_32.r_regionkey AS c6,
                                                ref_32.r_regionkey AS c7,
                                                sample_28.r_comment AS c8,
                                                ref_32.r_name AS c9,
                                                ref_32.r_regionkey AS c10,
                                                ref_32.r_comment AS c11
                                            FROM
                                                main.region AS ref_32
                                            WHERE
                                                1
                                            LIMIT 102))
                                    OR ((1)
                                        OR (0)))
                                AND ((((sample_28.r_regionkey IS NULL)
                                            OR ((
                                                    SELECT
                                                        s_address
                                                    FROM
                                                        main.supplier
                                                    LIMIT 1 offset 1) IS NOT NULL))
                                        AND (EXISTS (
                                                SELECT
                                                    sample_28.r_comment AS c0,
                                                    sample_33.p_name AS c1,
                                                    (
                                                        SELECT
                                                            c_nationkey
                                                        FROM
                                                            main.customer
                                                        LIMIT 1 offset 3) AS c2,
                                                    sample_33.p_mfgr AS c3
                                                FROM
                                                    main.part AS sample_33 TABLESAMPLE SYSTEM (0.5)
                                                WHERE (sample_33.p_size IS NOT NULL)
                                                OR (1)
                                            LIMIT 86)))
                                OR (((((sample_28.r_regionkey IS NOT NULL)
                                                AND ((
                                                        SELECT
                                                            s_nationkey
                                                        FROM
                                                            main.supplier
                                                        LIMIT 1 offset 77) IS NULL))
                                            OR (EXISTS (
                                                    SELECT
                                                        sample_28.r_name AS c0
                                                    FROM
                                                        main.part AS sample_34 TABLESAMPLE SYSTEM (3.8)
                                                    WHERE (
                                                        SELECT
                                                            s_acctbal
                                                        FROM
                                                            main.supplier
                                                        LIMIT 1 offset 1) IS NULL
                                                LIMIT 169)))
                                    OR (((((0)
                                                    AND (EXISTS (
                                                            SELECT
                                                                sample_28.r_regionkey AS c0,
                                                                ref_33.o_shippriority AS c1,
                                                                (
                                                                    SELECT
                                                                        r_regionkey
                                                                    FROM
                                                                        main.region
                                                                    LIMIT 1 offset 4) AS c2,
                                                                sample_28.r_name AS c3,
                                                                sample_28.r_regionkey AS c4
                                                            FROM
                                                                main.orders AS ref_33
                                                            WHERE
                                                                sample_28.r_comment IS NULL
                                                            LIMIT 140)))
                                                OR ((0)
                                                    OR ((sample_28.r_comment IS NULL)
                                                        AND (sample_28.r_regionkey IS NOT NULL))))
                                            AND (((sample_28.r_comment IS NOT NULL)
                                                    OR ((0)
                                                        AND (77 IS NOT NULL)))
                                                AND ((sample_28.r_comment IS NOT NULL)
                                                    AND ((0)
                                                        OR (sample_28.r_name IS NULL)))))
                                        OR (0)))
                                AND (EXISTS (
                                        SELECT
                                            sample_28.r_regionkey AS c0,
                                            sample_35.c_name AS c1,
                                            sample_35.c_mktsegment AS c2,
                                            sample_35.c_nationkey AS c3,
                                            sample_28.r_regionkey AS c4,
                                            sample_35.c_nationkey AS c5,
                                            sample_28.r_comment AS c6,
                                            sample_35.c_acctbal AS c7,
                                            sample_28.r_name AS c8
                                        FROM
                                            main.customer AS sample_35 TABLESAMPLE BERNOULLI (1.2)
                                        WHERE (sample_28.r_name IS NOT NULL)
                                        AND ((sample_35.c_mktsegment IS NOT NULL)
                                            AND (((sample_28.r_name IS NOT NULL)
                                                    AND ((0)
                                                        AND (EXISTS (
                                                                SELECT
                                                                    86 AS c0,
                                                                    sample_35.c_phone AS c1
                                                                FROM
                                                                    main.region AS sample_36 TABLESAMPLE SYSTEM (7.8)
                                                                WHERE (0)
                                                                AND (sample_36.r_comment IS NOT NULL)
                                                            LIMIT 120))))
                                            AND (((EXISTS (
                                                            SELECT
                                                                sample_35.c_address AS c0,
                                                                sample_37.c_custkey AS c1,
                                                                sample_35.c_comment AS c2,
                                                                sample_28.r_comment AS c3
                                                            FROM
                                                                main.customer AS sample_37 TABLESAMPLE BERNOULLI (2.1)
                                                            WHERE
                                                                sample_28.r_name IS NOT NULL
                                                            LIMIT 117))
                                                    OR ((0)
                                                        AND (EXISTS (
                                                                SELECT
                                                                    sample_28.r_name AS c0
                                                                FROM
                                                                    main.lineitem AS sample_38 TABLESAMPLE SYSTEM (1.8)
                                                                WHERE (EXISTS (
                                                                        SELECT
                                                                            sample_35.c_name AS c0,
                                                                            sample_28.r_comment AS c1,
                                                                            sample_28.r_name AS c2,
                                                                            ref_34.c_custkey AS c3,
                                                                            ref_34.c_custkey AS c4,
                                                                            ref_34.c_acctbal AS c5,
                                                                            sample_28.r_comment AS c6,
                                                                            sample_35.c_address AS c7,
                                                                            75 AS c8,
                                                                            sample_38.l_orderkey AS c9,
                                                                            sample_38.l_orderkey AS c10,
                                                                            (
                                                                                SELECT
                                                                                    l_quantity
                                                                                FROM
                                                                                    main.lineitem
                                                                                LIMIT 1 offset 4) AS c11,
                                                                            sample_35.c_address AS c12,
                                                                            sample_35.c_mktsegment AS c13,
                                                                            sample_28.r_regionkey AS c14,
                                                                            sample_28.r_comment AS c15,
                                                                            (
                                                                                SELECT
                                                                                    s_comment
                                                                                FROM
                                                                                    main.supplier
                                                                                LIMIT 1 offset 5) AS c16,
                                                                            ref_34.c_address AS c17,
                                                                            sample_28.r_name AS c18,
                                                                            sample_38.l_commitdate AS c19,
                                                                            sample_28.r_regionkey AS c20,
                                                                            sample_38.l_quantity AS c21,
                                                                            ref_34.c_phone AS c22,
                                                                            sample_35.c_nationkey AS c23,
                                                                            (
                                                                                SELECT
                                                                                    p_container
                                                                                FROM
                                                                                    main.part
                                                                                LIMIT 1 offset 1) AS c24,
                                                                            sample_38.l_shipdate AS c25,
                                                                            sample_28.r_comment AS c26
                                                                        FROM
                                                                            main.customer AS ref_34
                                                                        WHERE
                                                                            1))
                                                                    AND (((EXISTS (
                                                                                    SELECT
                                                                                        (
                                                                                            SELECT
                                                                                                p_container
                                                                                            FROM
                                                                                                main.part
                                                                                            LIMIT 1 offset 46) AS c0,
                                                                                        sample_38.l_shipmode AS c1,
                                                                                        ref_35.l_orderkey AS c2,
                                                                                        3 AS c3,
                                                                                        sample_35.c_name AS c4,
                                                                                        sample_35.c_address AS c5,
                                                                                        sample_35.c_address AS c6,
                                                                                        sample_28.r_regionkey AS c7,
                                                                                        sample_35.c_nationkey AS c8,
                                                                                        sample_28.r_regionkey AS c9,
                                                                                        sample_38.l_linestatus AS c10,
                                                                                        sample_35.c_address AS c11
                                                                                    FROM
                                                                                        main.lineitem AS ref_35
                                                                                    WHERE (1)
                                                                                    OR (EXISTS (
                                                                                            SELECT
                                                                                                sample_35.c_comment AS c0,
                                                                                                sample_28.r_regionkey AS c1,
                                                                                                sample_28.r_name AS c2
                                                                                            FROM
                                                                                                main.region AS sample_39 TABLESAMPLE SYSTEM (1.7)
                                                                                            WHERE
                                                                                                ref_35.l_receiptdate IS NULL
                                                                                            LIMIT 95))
                                                                                LIMIT 73))
                                                                        OR (0))
                                                                    AND ((1)
                                                                        AND (sample_35.c_comment IS NULL)))))))
                                                AND (1))))))))))
            OR ((1)
                OR (0)))
        OR (sample_28.r_comment IS NULL))
LIMIT 61
)
SELECT
    CAST(nullif (ref_36.c_phone, ref_36.c_phone) AS VARCHAR) AS c0,
    ref_36.c_phone AS c1
FROM
    main.customer AS ref_36
WHERE (((ref_36.c_phone IS NULL)
        AND (ref_36.c_acctbal IS NOT NULL))
    AND ((
            SELECT
                l_orderkey
            FROM
                main.lineitem
            LIMIT 1 offset 3) IS NULL))
    OR (ref_36.c_address IS NOT NULL)
LIMIT 127
