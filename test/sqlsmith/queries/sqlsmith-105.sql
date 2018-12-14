SELECT
    CASE WHEN ((((0)
                    OR (1))
                AND (((1)
                        OR (EXISTS (
                                SELECT
                                    subq_0.c0 AS c0,
                                    subq_0.c1 AS c1
                                FROM
                                    main.orders AS ref_4
                                WHERE
                                    1)))
                        OR ((EXISTS (
                                    SELECT
                                        ref_5.o_totalprice AS c0, ref_5.o_orderstatus AS c1, subq_0.c0 AS c2, subq_0.c1 AS c3, 31 AS c4, ref_5.o_totalprice AS c5, ref_5.o_orderkey AS c6, subq_0.c0 AS c7, (
                                            SELECT
                                                s_nationkey
                                            FROM
                                                main.supplier
                                            LIMIT 1 offset 5) AS c8,
                                        subq_0.c0 AS c9,
                                        subq_0.c0 AS c10,
                                        ref_5.o_totalprice AS c11
                                    FROM
                                        main.orders AS ref_5
                                    WHERE
                                        1))
                                OR (((EXISTS (
                                                SELECT
                                                    ref_6.o_orderstatus AS c0, ref_6.o_comment AS c1, ref_6.o_totalprice AS c2, ref_6.o_clerk AS c3, ref_6.o_clerk AS c4, ref_6.o_custkey AS c5, subq_0.c0 AS c6, subq_0.c1 AS c7, ref_6.o_orderstatus AS c8
                                                FROM
                                                    main.orders AS ref_6
                                                WHERE (EXISTS (
                                                        SELECT
                                                            ref_6.o_orderpriority AS c0, subq_0.c0 AS c1, ref_7.c_address AS c2
                                                        FROM
                                                            main.customer AS ref_7
                                                        WHERE (EXISTS (
                                                                SELECT
                                                                    ref_8.s_nationkey AS c0, 57 AS c1, ref_8.s_name AS c2, ref_8.s_suppkey AS c3, ref_6.o_shippriority AS c4, ref_7.c_custkey AS c5, subq_0.c0 AS c6, subq_0.c0 AS c7, subq_0.c1 AS c8, ref_7.c_address AS c9, ref_8.s_suppkey AS c10, subq_0.c1 AS c11, ref_6.o_custkey AS c12, ref_6.o_shippriority AS c13, ref_6.o_orderkey AS c14, ref_7.c_address AS c15, (
                                                                        SELECT
                                                                            p_name
                                                                        FROM
                                                                            main.part
                                                                        LIMIT 1 offset 1) AS c16,
                                                                    ref_6.o_clerk AS c17,
                                                                    subq_0.c1 AS c18,
                                                                    ref_8.s_nationkey AS c19,
                                                                    ref_8.s_address AS c20,
                                                                    ref_7.c_address AS c21,
                                                                    ref_8.s_comment AS c22,
                                                                    ref_7.c_comment AS c23,
                                                                    74 AS c24,
                                                                    subq_0.c0 AS c25,
                                                                    subq_0.c0 AS c26,
                                                                    ref_7.c_mktsegment AS c27,
                                                                    ref_7.c_address AS c28,
                                                                    ref_7.c_acctbal AS c29,
                                                                    ref_8.s_phone AS c30,
                                                                    ref_6.o_orderdate AS c31,
                                                                    subq_0.c1 AS c32,
                                                                    ref_7.c_address AS c33,
                                                                    ref_8.s_acctbal AS c34,
                                                                    ref_8.s_address AS c35,
                                                                    ref_6.o_orderkey AS c36,
                                                                    subq_0.c1 AS c37
                                                                FROM
                                                                    main.supplier AS ref_8
                                                                WHERE (1)
                                                                OR ((
                                                                        SELECT
                                                                            c_acctbal
                                                                        FROM
                                                                            main.customer
                                                                        LIMIT 1 offset 5)
                                                                    IS NULL)
                                                            LIMIT 101))
                                                    AND ((0)
                                                        AND (0))))
                                            AND (EXISTS (
                                                    SELECT
                                                        ref_6.o_totalprice AS c0,
                                                        ref_6.o_orderstatus AS c1
                                                    FROM
                                                        main.orders AS ref_9
                                                    WHERE (ref_6.o_orderpriority IS NULL)
                                                    OR (subq_0.c0 IS NOT NULL)))
                                        LIMIT 144))
                                OR (1))
                            OR (1 IS NOT NULL)))))
            AND ((((((((0)
                                        OR (((subq_0.c0 IS NULL)
                                                AND ((1)
                                                    OR ((subq_0.c1 IS NULL)
                                                        AND ((subq_0.c1 IS NOT NULL)
                                                            AND ((0)
                                                                AND ((EXISTS (
                                                                            SELECT
                                                                                ref_10.l_suppkey AS c0,
                                                                                subq_0.c1 AS c1,
                                                                                subq_0.c1 AS c2,
                                                                                ref_10.l_discount AS c3,
                                                                                (
                                                                                    SELECT
                                                                                        l_shipdate
                                                                                    FROM
                                                                                        main.lineitem
                                                                                    LIMIT 1 offset 3) AS c4,
                                                                                ref_10.l_shipdate AS c5,
                                                                                ref_10.l_discount AS c6,
                                                                                subq_0.c1 AS c7,
                                                                                (
                                                                                    SELECT
                                                                                        p_comment
                                                                                    FROM
                                                                                        main.part
                                                                                    LIMIT 1 offset 4) AS c8,
                                                                                subq_0.c0 AS c9,
                                                                                ref_10.l_tax AS c10
                                                                            FROM
                                                                                main.lineitem AS ref_10
                                                                            WHERE ((ref_10.l_quantity IS NOT NULL)
                                                                                AND (1))
                                                                            AND (subq_0.c1 IS NOT NULL)
                                                                        LIMIT 65))
                                                                AND ((0)
                                                                    OR (0))))))))
                                        OR (1)))
                                AND ((EXISTS (
                                            SELECT
                                                ref_11.p_name AS c0,
                                                subq_0.c1 AS c1
                                            FROM
                                                main.part AS ref_11
                                            WHERE (37 IS NULL)
                                            OR ((1)
                                                AND ((1)
                                                    OR (0)))))
                                    AND (0)))
                            AND (subq_0.c0 IS NOT NULL))
                        OR (1))
                    AND ((((subq_0.c1 IS NULL)
                                OR (subq_0.c0 IS NULL))
                            AND (subq_0.c1 IS NOT NULL))
                        AND ((1)
                            AND ((subq_0.c1 IS NULL)
                                OR ((((1)
                                            AND (0))
                                        OR (1))
                                    OR ((((subq_0.c0 IS NOT NULL)
                                                AND ((0)
                                                    OR (1)))
                                            OR (EXISTS (
                                                    SELECT
                                                        ref_12.o_orderkey AS c0, ref_12.o_shippriority AS c1
                                                    FROM
                                                        main.orders AS ref_12
                                                    WHERE (subq_0.c0 IS NULL)
                                                    AND (((ref_12.o_clerk IS NOT NULL)
                                                            AND (1))
                                                        OR (subq_0.c1 IS NOT NULL)))))
                                        AND (0)))))))
                AND (subq_0.c1 IS NULL))
            AND ((0)
                OR (EXISTS (
                        SELECT
                            ref_13.p_partkey AS c0, subq_0.c1 AS c1
                        FROM
                            main.part AS ref_13
                        WHERE
                            1
                        LIMIT 133)))))
    OR (69 IS NULL) THEN
    subq_0.c1
ELSE
    subq_0.c1
END AS c0,
subq_0.c1 AS c1,
subq_0.c0 AS c2,
subq_0.c0 AS c3,
subq_0.c0 AS c4,
subq_0.c1 AS c5
FROM (
    SELECT
        ref_3.l_quantity AS c0,
        ref_2.s_name AS c1
    FROM
        main.partsupp AS ref_0
        INNER JOIN main.orders AS ref_1
        INNER JOIN main.supplier AS ref_2
        RIGHT JOIN main.lineitem AS ref_3 ON (ref_3.l_discount IS NOT NULL) ON (ref_1.o_orderstatus = ref_2.s_name) ON ((ref_2.s_acctbal IS NOT NULL)
                AND ((21 IS NULL)
                    OR (1)))
    WHERE
        ref_2.s_nationkey IS NOT NULL
    LIMIT 37) AS subq_0
WHERE ((EXISTS (
            SELECT
                subq_0.c0 AS c0
            FROM
                main.customer AS ref_14
            WHERE (1)
            OR (((1)
                    OR (0))
                AND (ref_14.c_mktsegment IS NULL))))
    AND ((
            SELECT
                r_comment
            FROM
                main.region
            LIMIT 1 offset 4)
        IS NOT NULL))
OR ((EXISTS (
            SELECT
                (
                    SELECT
                        s_suppkey
                    FROM
                        main.supplier
                    LIMIT 1 offset 2) AS c0,
                ref_15.c_custkey AS c1
            FROM
                main.customer AS ref_15
            WHERE
                subq_0.c1 IS NOT NULL
            LIMIT 134))
    OR (((0)
            OR (subq_0.c0 IS NOT NULL))
        AND ((EXISTS (
                    SELECT
                        ref_16.s_suppkey AS c0,
                        ref_16.s_address AS c1,
                        (
                            SELECT
                                n_name
                            FROM
                                main.nation
                            LIMIT 1 offset 4) AS c2,
                        ref_16.s_address AS c3,
                        ref_16.s_comment AS c4,
                        subq_0.c1 AS c5,
                        subq_0.c1 AS c6,
                        subq_0.c1 AS c7,
                        ref_16.s_nationkey AS c8,
                        subq_0.c1 AS c9,
                        ref_16.s_nationkey AS c10,
                        ref_16.s_address AS c11,
                        ref_16.s_suppkey AS c12,
                        subq_0.c1 AS c13,
                        (
                            SELECT
                                r_comment
                            FROM
                                main.region
                            LIMIT 1 offset 5) AS c14,
                        subq_0.c0 AS c15,
                        subq_0.c0 AS c16,
                        ref_16.s_phone AS c17,
                        15 AS c18
                    FROM
                        main.supplier AS ref_16
                    WHERE (ref_16.s_acctbal IS NULL)
                    OR (subq_0.c1 IS NULL)
                LIMIT 72))
        AND (0))))
