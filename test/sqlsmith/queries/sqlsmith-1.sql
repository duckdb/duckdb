SELECT
    subq_0.c2 AS c0
FROM (
    SELECT
        ref_4.n_nationkey AS c0,
        ref_4.n_regionkey AS c1,
        ref_4.n_nationkey AS c2,
        ref_4.n_name AS c3,
        ref_0.s_address AS c4,
        ref_4.n_name AS c5,
        ref_4.n_name AS c6,
        ref_0.s_address AS c7
    FROM
        main.supplier AS ref_0
    INNER JOIN main.nation AS ref_1 ON ((((0)
                    AND ((ref_0.s_phone IS NULL)
                        AND (ref_0.s_nationkey IS NULL)))
                OR (EXISTS (
                        SELECT
                            ref_0.s_address AS c0,
                            ref_1.n_regionkey AS c1,
                            ref_2.p_name AS c2,
                            ref_0.s_address AS c3,
                            ref_2.p_comment AS c4,
                            ref_2.p_mfgr AS c5,
                            ref_2.p_type AS c6,
                            ref_0.s_comment AS c7,
                            79 AS c8,
                            ref_1.n_name AS c9, (
                                SELECT
                                    c_acctbal
                                FROM
                                    main.customer
                                LIMIT 1 offset 1) AS c10,
                            ref_2.p_mfgr AS c11,
                            ref_0.s_acctbal AS c12,
                            ref_0.s_nationkey AS c13
                        FROM
                            main.part AS ref_2
                        WHERE
                            EXISTS (
                                SELECT
                                    25 AS c0, ref_3.n_comment AS c1
                                FROM
                                    main.nation AS ref_3
                                WHERE
                                    1)
                            LIMIT 161)))
                OR (((ref_1.n_regionkey IS NULL)
                        OR (ref_0.s_nationkey IS NULL))
                    OR (ref_1.n_comment IS NOT NULL)))
            RIGHT JOIN main.nation AS ref_4 ON (((ref_0.s_acctbal IS NULL)
                        OR ((ref_4.n_regionkey IS NULL)
                            OR (((ref_1.n_nationkey IS NOT NULL)
                                    OR ((1)
                                        AND (((((1)
                                                        OR (0))
                                                    AND (ref_0.s_address IS NULL))
                                                AND (1))
                                            OR (1))))
                                OR (0))))
                    AND ((1)
                        AND ((ref_4.n_nationkey IS NULL)
                            AND ((
                                    SELECT
                                        r_comment
                                    FROM
                                        main.region
                                    LIMIT 1 offset 37)
                                IS NULL))))
            WHERE (((EXISTS (
                            SELECT
                                ref_5.o_orderkey AS c0, ref_0.s_address AS c1
                            FROM
                                main.orders AS ref_5
                            WHERE ((0)
                                AND (1))
                            OR ((((((1)
                                                OR (EXISTS (
                                                        SELECT
                                                            ref_4.n_name AS c0, ref_4.n_regionkey AS c1
                                                        FROM
                                                            main.lineitem AS ref_6
                                                        WHERE
                                                            EXISTS (
                                                                SELECT
                                                                    ref_6.l_receiptdate AS c0, ref_1.n_regionkey AS c1
                                                                FROM
                                                                    main.supplier AS ref_7
                                                                WHERE
                                                                    EXISTS (
                                                                        SELECT
                                                                            ref_7.s_comment AS c0, ref_7.s_nationkey AS c1, ref_6.l_shipdate AS c2, ref_8.s_suppkey AS c3
                                                                        FROM
                                                                            main.supplier AS ref_8
                                                                        WHERE
                                                                            ref_5.o_clerk IS NULL)
                                                                    LIMIT 82)
                                                            LIMIT 112)))
                                                OR (1))
                                            AND (0))
                                        OR (((EXISTS (
                                                        SELECT
                                                            82 AS c0,
                                                            ref_4.n_comment AS c1,
                                                            ref_1.n_name AS c2,
                                                            ref_5.o_totalprice AS c3,
                                                            ref_0.s_nationkey AS c4,
                                                            ref_4.n_name AS c5,
                                                            ref_4.n_name AS c6,
                                                            ref_5.o_orderpriority AS c7,
                                                            ref_5.o_orderstatus AS c8, (
                                                                SELECT
                                                                    r_regionkey
                                                                FROM
                                                                    main.region
                                                                LIMIT 1 offset 6) AS c9
                                                        FROM
                                                            main.part AS ref_9
                                                        WHERE (EXISTS (
                                                                SELECT
                                                                    ref_10.c_phone AS c0, ref_5.o_orderkey AS c1, (
                                                                        SELECT
                                                                            o_comment
                                                                        FROM
                                                                            main.orders
                                                                        LIMIT 1 offset 1) AS c2,
                                                                    ref_5.o_shippriority AS c3,
                                                                    ref_10.c_address AS c4,
                                                                    ref_9.p_type AS c5,
                                                                    ref_1.n_regionkey AS c6,
                                                                    ref_10.c_address AS c7
                                                                FROM
                                                                    main.customer AS ref_10
                                                                WHERE
                                                                    ref_1.n_regionkey IS NOT NULL
                                                                LIMIT 173))
                                                        OR (1)
                                                    LIMIT 105))
                                            OR (EXISTS (
                                                    SELECT
                                                        46 AS c0,
                                                        ref_0.s_acctbal AS c1
                                                    FROM
                                                        main.nation AS ref_11
                                                    WHERE
                                                        EXISTS (
                                                            SELECT
                                                                ref_0.s_name AS c0
                                                            FROM
                                                                main.region AS ref_12
                                                            WHERE
                                                                ref_1.n_name IS NULL
                                                            LIMIT 49)
                                                    LIMIT 96)))
                                        OR ((0)
                                            OR (ref_5.o_comment IS NULL))))
                                OR (1))))
                    OR (EXISTS (
                            SELECT
                                ref_1.n_regionkey AS c0,
                                ref_0.s_nationkey AS c1,
                                ref_1.n_regionkey AS c2
                            FROM
                                main.nation AS ref_13
                            WHERE 0
                        LIMIT 134)))
            AND (1))
        AND (EXISTS (
                SELECT
                    ref_1.n_name AS c0,
                    ref_14.o_totalprice AS c1
                FROM
                    main.orders AS ref_14
                WHERE (EXISTS (
                        SELECT
                            ref_0.s_phone AS c0, ref_14.o_clerk AS c1, ref_15.ps_availqty AS c2, ref_0.s_acctbal AS c3, ref_0.s_suppkey AS c4, ref_15.ps_supplycost AS c5, ref_0.s_name AS c6, ref_1.n_comment AS c7, ref_14.o_orderpriority AS c8, ref_4.n_comment AS c9, ref_14.o_clerk AS c10, ref_14.o_custkey AS c11, 7 AS c12
                        FROM
                            main.partsupp AS ref_15
                        WHERE ((((0)
                                    AND (0))
                                AND (ref_15.ps_suppkey IS NULL))
                            AND (1))
                        AND (1)))
                AND (ref_14.o_shippriority IS NOT NULL)
            LIMIT 106))
LIMIT 105) AS subq_0
    RIGHT JOIN main.customer AS ref_16 ON ((((0)
                    AND (1))
                OR (((1)
                        AND (0))
                    AND (ref_16.c_acctbal IS NULL)))
            AND ((subq_0.c6 IS NULL)
                OR ((ref_16.c_address IS NULL)
                    OR ((0)
                        OR (1)))))
    WHERE
        1
    LIMIT 153
