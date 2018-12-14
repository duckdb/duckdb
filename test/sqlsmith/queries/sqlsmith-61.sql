SELECT
    ref_0.l_linenumber AS c0,
    ref_0.l_shipmode AS c1,
    ref_0.l_receiptdate AS c2,
    CASE WHEN (((EXISTS (
                        SELECT
                            34 AS c0,
                            ref_1.l_partkey AS c1
                        FROM
                            main.lineitem AS ref_1
                        WHERE (EXISTS (
                                SELECT
                                    ref_1.l_shipdate AS c0, ref_2.n_name AS c1, ref_0.l_suppkey AS c2, ref_2.n_name AS c3, ref_1.l_suppkey AS c4, ref_1.l_quantity AS c5, ref_2.n_comment AS c6, (
                                        SELECT
                                            o_clerk
                                        FROM
                                            main.orders
                                        LIMIT 1 offset 2) AS c7,
                                    ref_2.n_nationkey AS c8,
                                    ref_1.l_discount AS c9,
                                    ref_0.l_discount AS c10
                                FROM
                                    main.nation AS ref_2
                                WHERE ((((1)
                                            OR (0))
                                        OR ((1)
                                            OR (1)))
                                    AND ((1)
                                        OR (ref_0.l_shipinstruct IS NULL)))
                                OR (0)
                            LIMIT 119))
                    AND (EXISTS (
                            SELECT
                                ref_0.l_extendedprice AS c0,
                                ref_0.l_extendedprice AS c1,
                                ref_1.l_shipdate AS c2
                            FROM
                                main.orders AS ref_3
                            WHERE ((EXISTS (
                                        SELECT
                                            ref_4.ps_comment AS c0, 60 AS c1, ref_4.ps_partkey AS c2, ref_4.ps_suppkey AS c3, ref_1.l_linenumber AS c4, ref_1.l_receiptdate AS c5, ref_1.l_discount AS c6, ref_4.ps_partkey AS c7, ref_0.l_linestatus AS c8, ref_3.o_shippriority AS c9, ref_4.ps_supplycost AS c10, ref_3.o_clerk AS c11, ref_0.l_comment AS c12, ref_4.ps_comment AS c13, ref_4.ps_availqty AS c14, ref_1.l_partkey AS c15, ref_1.l_returnflag AS c16, ref_3.o_totalprice AS c17, ref_3.o_shippriority AS c18, ref_3.o_orderpriority AS c19, ref_3.o_shippriority AS c20, ref_0.l_quantity AS c21, 88 AS c22, ref_1.l_shipdate AS c23, ref_0.l_receiptdate AS c24, ref_0.l_suppkey AS c25, ref_1.l_partkey AS c26, ref_1.l_linestatus AS c27, ref_4.ps_availqty AS c28
                                        FROM
                                            main.partsupp AS ref_4
                                        WHERE
                                            ref_3.o_custkey IS NULL
                                        LIMIT 161))
                                OR (1))
                            AND ((ref_3.o_totalprice IS NOT NULL)
                                AND ((ref_3.o_shippriority IS NOT NULL)
                                    AND ((
                                            SELECT
                                                ps_supplycost
                                            FROM
                                                main.partsupp
                                            LIMIT 1 offset 2)
                                        IS NOT NULL)))
                        LIMIT 82))
            LIMIT 113))
    AND (1))
OR (ref_0.l_tax IS NULL))
OR (ref_0.l_shipdate IS NULL) THEN
ref_0.l_partkey
ELSE
    ref_0.l_partkey
END AS c3,
ref_0.l_shipdate AS c4,
ref_0.l_linestatus AS c5,
CASE WHEN EXISTS (
        SELECT
            ref_5.p_comment AS c0
        FROM
            main.part AS ref_5
        WHERE
            EXISTS (
                SELECT
                    37 AS c0, ref_0.l_partkey AS c1, ref_6.ps_comment AS c2, ref_5.p_retailprice AS c3
                FROM
                    main.partsupp AS ref_6
                WHERE
                    1
                LIMIT 114)) THEN
        ref_0.l_linestatus
    ELSE
        ref_0.l_linestatus
    END AS c6,
    ref_0.l_shipinstruct AS c7,
    ref_0.l_extendedprice AS c8,
    ref_0.l_shipdate AS c9,
    ref_0.l_shipinstruct AS c10,
    ref_0.l_commitdate AS c11,
    CASE WHEN (EXISTS (
                SELECT
                    ref_7.s_comment AS c0,
                    ref_0.l_shipinstruct AS c1
                FROM
                    main.supplier AS ref_7
                WHERE ((1)
                    OR (0))
                OR (EXISTS (
                        SELECT
                            ref_7.s_suppkey AS c0, ref_0.l_receiptdate AS c1, 82 AS c2, ref_8.ps_partkey AS c3, ref_8.ps_comment AS c4
                        FROM
                            main.partsupp AS ref_8
                        WHERE ((((1)
                                    AND (ref_8.ps_supplycost IS NULL))
                                OR ((ref_0.l_shipinstruct IS NOT NULL)
                                    OR ((((
                                                    SELECT
                                                        c_address
                                                    FROM
                                                        main.customer
                                                    LIMIT 1 offset 25)
                                                IS NOT NULL)
                                            OR (1))
                                        AND (1))))
                            OR ((((1)
                                        AND (ref_7.s_suppkey IS NOT NULL))
                                    OR (0))
                                OR ((1)
                                    OR (EXISTS (
                                            SELECT
                                                ref_9.n_nationkey AS c0,
                                                ref_9.n_comment AS c1,
                                                ref_7.s_nationkey AS c2,
                                                ref_8.ps_partkey AS c3
                                            FROM
                                                main.nation AS ref_9
                                            WHERE ((ref_8.ps_partkey IS NULL)
                                                OR (1))
                                            AND (1)
                                        LIMIT 74)))))
                    OR (1)
                LIMIT 73))
    LIMIT 48))
AND ((ref_0.l_shipinstruct IS NULL)
    OR ((EXISTS (
                SELECT
                    ref_0.l_discount AS c0,
                    ref_0.l_shipdate AS c1,
                    ref_10.s_name AS c2
                FROM
                    main.supplier AS ref_10
                WHERE ((
                        SELECT
                            p_container
                        FROM
                            main.part
                        LIMIT 1 offset 29)
                    IS NULL)
                OR (EXISTS (
                        SELECT
                            ref_10.s_name AS c0,
                            ref_0.l_shipmode AS c1,
                            ref_0.l_suppkey AS c2,
                            ref_11.c_custkey AS c3,
                            ref_0.l_linenumber AS c4,
                            ref_0.l_commitdate AS c5,
                            ref_10.s_suppkey AS c6
                        FROM
                            main.customer AS ref_11
                        WHERE (ref_10.s_nationkey IS NOT NULL)
                        AND (EXISTS (
                                SELECT
                                    ref_0.l_linestatus AS c0, ref_10.s_address AS c1
                                FROM
                                    main.lineitem AS ref_12
                                WHERE
                                    1))
                        LIMIT 150))
            LIMIT 48))
    AND (0))) THEN
ref_0.l_extendedprice
ELSE
    ref_0.l_extendedprice
END AS c12,
CAST(nullif (CAST(coalesce(ref_0.l_shipinstruct, ref_0.l_shipinstruct) AS VARCHAR), ref_0.l_shipinstruct) AS VARCHAR) AS c13,
    CASE WHEN ((((0)
                    OR (1))
                OR (EXISTS (
                        SELECT
                            ref_13.ps_supplycost AS c0
                        FROM
                            main.partsupp AS ref_13
                        WHERE ((((ref_0.l_linestatus IS NOT NULL)
                                    OR ((((1)
                                                OR (((ref_13.ps_suppkey IS NULL)
                                                        OR (0))
                                                    AND (1)))
                                            AND (ref_13.ps_partkey IS NOT NULL))
                                        AND (ref_0.l_shipmode IS NULL)))
                                AND (ref_13.ps_supplycost IS NOT NULL))
                            OR ((ref_13.ps_comment IS NULL)
                                AND ((ref_13.ps_partkey IS NOT NULL)
                                    AND (ref_13.ps_partkey IS NULL))))
                        AND ((1)
                            OR ((ref_13.ps_comment IS NULL)
                                AND ((1)
                                    AND (EXISTS (
                                            SELECT
                                                ref_14.l_extendedprice AS c0, 38 AS c1, ref_13.ps_supplycost AS c2, ref_13.ps_supplycost AS c3, ref_13.ps_partkey AS c4
                                            FROM
                                                main.lineitem AS ref_14
                                            WHERE (
                                                SELECT
                                                    s_suppkey
                                                FROM
                                                    main.supplier
                                                LIMIT 1 offset 82)
                                            IS NOT NULL
                                        LIMIT 122)))))
                LIMIT 108)))
    AND (((((
                        SELECT
                            s_phone
                        FROM
                            main.supplier
                        LIMIT 1 offset 2)
                    IS NOT NULL)
                AND (EXISTS (
                        SELECT
                            ref_15.l_discount AS c0
                        FROM
                            main.lineitem AS ref_15
                        WHERE
                            EXISTS (
                                SELECT
                                    ref_0.l_commitdate AS c0, ref_15.l_shipdate AS c1, ref_16.r_regionkey AS c2, ref_15.l_orderkey AS c3, ref_16.r_regionkey AS c4, ref_16.r_name AS c5, ref_15.l_shipmode AS c6, ref_15.l_quantity AS c7, ref_0.l_returnflag AS c8, ref_16.r_regionkey AS c9, ref_15.l_partkey AS c10, ref_16.r_comment AS c11, 40 AS c12, ref_0.l_tax AS c13, ref_15.l_comment AS c14, ref_16.r_comment AS c15, ref_16.r_name AS c16, ref_16.r_name AS c17, ref_16.r_regionkey AS c18, ref_16.r_name AS c19
                                FROM
                                    main.region AS ref_16
                                WHERE
                                    EXISTS (
                                        SELECT
                                            ref_15.l_shipinstruct AS c0
                                        FROM
                                            main.orders AS ref_17
                                        WHERE
                                            ref_16.r_name IS NULL)
                                    LIMIT 44))))
                    OR (((
                                SELECT
                                    s_name
                                FROM
                                    main.supplier
                                LIMIT 1 offset 2)
                            IS NOT NULL)
                        AND (0)))
                AND ((ref_0.l_shipdate IS NULL)
                    OR (0))))
        AND ((0)
            OR (0)) THEN
        ref_0.l_commitdate
    ELSE
        ref_0.l_commitdate
    END AS c14,
    ref_0.l_orderkey AS c15,
    16 AS c16,
    ref_0.l_discount AS c17
FROM
    main.lineitem AS ref_0
WHERE
    0
LIMIT 117
