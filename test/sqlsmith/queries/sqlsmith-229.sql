SELECT
    subq_0.c0 AS c0
FROM (
    SELECT
        ref_1.l_linenumber AS c0,
        ref_0.p_size AS c1
    FROM
        main.part AS ref_0
        INNER JOIN main.lineitem AS ref_1 ON (ref_1.l_returnflag IS NULL)
        RIGHT JOIN main.partsupp AS ref_2 ON (ref_0.p_retailprice IS NULL)
    WHERE
        1) AS subq_0
    LEFT JOIN (
        SELECT
            ref_5.n_name AS c0, ref_3.s_address AS c1, CAST(nullif (ref_3.s_nationkey, ref_6.l_linenumber) AS INTEGER) AS c2, ref_6.l_suppkey AS c3, ref_5.n_nationkey AS c4, ref_4.o_comment AS c5, ref_4.o_clerk AS c6, ref_5.n_nationkey AS c7, ref_3.s_nationkey AS c8, ref_3.s_suppkey AS c9, ref_4.o_totalprice AS c10, ref_5.n_regionkey AS c11, ref_4.o_shippriority AS c12
        FROM
            main.supplier AS ref_3
            INNER JOIN main.orders AS ref_4
            INNER JOIN main.nation AS ref_5 ON ((ref_4.o_orderkey IS NOT NULL)
                    OR (44 IS NULL))
                INNER JOIN main.lineitem AS ref_6 ON (ref_4.o_comment IS NOT NULL) ON (ref_3.s_phone = ref_5.n_name)
            WHERE (ref_4.o_custkey IS NOT NULL)
            OR (ref_6.l_discount IS NOT NULL)
        LIMIT 96) AS subq_1 ON (subq_0.c1 = subq_1.c2)
WHERE
    EXISTS (
        SELECT
            subq_1.c10 AS c0, subq_0.c1 AS c1
        FROM
            main.lineitem AS ref_7
        WHERE ((((subq_0.c0 IS NOT NULL)
                    OR (subq_0.c0 IS NULL))
                OR (((1)
                        OR (1))
                    OR ((0)
                        AND (EXISTS (
                                SELECT
                                    subq_1.c6 AS c0, subq_0.c0 AS c1, ref_7.l_shipinstruct AS c2, subq_1.c7 AS c3, ref_8.r_regionkey AS c4, ref_7.l_shipdate AS c5, ref_7.l_shipmode AS c6, ref_8.r_name AS c7, ref_7.l_shipinstruct AS c8, ref_7.l_suppkey AS c9, subq_0.c1 AS c10, (
                                        SELECT
                                            r_regionkey
                                        FROM
                                            main.region
                                        LIMIT 1 offset 3) AS c11,
                                    ref_7.l_linestatus AS c12,
                                    48 AS c13,
                                    ref_8.r_regionkey AS c14,
                                    ref_8.r_name AS c15,
                                    ref_8.r_name AS c16,
                                    (
                                        SELECT
                                            n_regionkey
                                        FROM
                                            main.nation
                                        LIMIT 1 offset 4) AS c17,
                                    ref_8.r_comment AS c18,
                                    50 AS c19,
                                    ref_8.r_name AS c20
                                FROM
                                    main.region AS ref_8
                                WHERE
                                    EXISTS (
                                        SELECT
                                            subq_0.c1 AS c0, subq_1.c3 AS c1, subq_1.c11 AS c2, subq_0.c1 AS c3, subq_0.c1 AS c4
                                        FROM
                                            main.region AS ref_9
                                        WHERE
                                            1
                                        LIMIT 93)
                                LIMIT 47)))))
            OR (subq_0.c0 IS NULL))
        AND (((((0)
                        AND ((
                                SELECT
                                    ps_availqty
                                FROM
                                    main.partsupp
                                LIMIT 1 offset 4)
                            IS NULL))
                    AND ((subq_1.c5 IS NULL)
                        OR ((((EXISTS (
                                            SELECT
                                                subq_1.c0 AS c0
                                            FROM
                                                main.customer AS ref_10
                                            WHERE (EXISTS (
                                                    SELECT
                                                        ref_10.c_acctbal AS c0
                                                    FROM
                                                        main.orders AS ref_11
                                                    WHERE
                                                        EXISTS (
                                                            SELECT
                                                                ref_7.l_partkey AS c0, ref_12.o_shippriority AS c1, ref_10.c_acctbal AS c2, ref_10.c_nationkey AS c3
                                                            FROM
                                                                main.orders AS ref_12
                                                            WHERE (ref_12.o_shippriority IS NOT NULL)
                                                            AND (42 IS NULL)
                                                        LIMIT 165)))
                                            OR ((((subq_1.c6 IS NOT NULL)
                                                        OR (((EXISTS (
                                                                        SELECT
                                                                            subq_0.c1 AS c0,
                                                                            subq_0.c0 AS c1,
                                                                            ref_7.l_linenumber AS c2,
                                                                            ref_7.l_discount AS c3,
                                                                            (
                                                                                SELECT
                                                                                    p_retailprice
                                                                                FROM
                                                                                    main.part
                                                                                LIMIT 1 offset 2) AS c4
                                                                        FROM
                                                                            main.region AS ref_13
                                                                        WHERE
                                                                            1
                                                                        LIMIT 77))
                                                                OR (1))
                                                            OR (0)))
                                                    AND ((subq_1.c3 IS NOT NULL)
                                                        AND (1)))
                                                AND (subq_0.c0 IS NULL))
                                        LIMIT 141))
                                AND (EXISTS (
                                        SELECT
                                            ref_14.c_address AS c0,
                                            ref_7.l_quantity AS c1,
                                            ref_14.c_mktsegment AS c2,
                                            ref_14.c_acctbal AS c3,
                                            6 AS c4,
                                            subq_0.c0 AS c5,
                                            ref_14.c_address AS c6,
                                            ref_7.l_extendedprice AS c7
                                        FROM
                                            main.customer AS ref_14
                                        WHERE
                                            ref_14.c_phone IS NULL
                                        LIMIT 87)))
                            OR (0))
                        OR (subq_0.c1 IS NULL))))
            AND ((
                    SELECT
                        r_regionkey
                    FROM
                        main.region
                    LIMIT 1 offset 86)
                IS NULL))
        AND (EXISTS (
                SELECT
                    (
                        SELECT
                            l_shipdate
                        FROM
                            main.lineitem
                        LIMIT 1 offset 53) AS c0,
                    ref_7.l_linestatus AS c1,
                    subq_0.c1 AS c2,
                    ref_7.l_tax AS c3,
                    subq_0.c1 AS c4,
                    ref_7.l_orderkey AS c5
                FROM
                    main.customer AS ref_15
                WHERE
                    ref_15.c_custkey IS NULL
                LIMIT 76)))
LIMIT 84)
LIMIT 177
