SELECT
    subq_0.c2 AS c0,
    subq_0.c1 AS c1,
    subq_0.c1 AS c2,
    subq_0.c0 AS c3,
    subq_0.c1 AS c4,
    subq_0.c2 AS c5
FROM (
    SELECT
        ref_3.o_totalprice AS c0,
        ref_0.r_regionkey AS c1,
        ref_2.p_comment AS c2
    FROM
        main.region AS ref_0
        INNER JOIN main.nation AS ref_1
        RIGHT JOIN main.part AS ref_2 ON ((((1)
                        OR (0))
                    OR (1))
                OR (ref_2.p_size IS NULL)) ON (ref_0.r_comment = ref_1.n_name)
        LEFT JOIN main.orders AS ref_3 ON (ref_0.r_comment = ref_3.o_orderstatus)
    WHERE (0)
    OR (((EXISTS (
                    SELECT
                        ref_1.n_regionkey AS c0
                    FROM
                        main.customer AS ref_4
                    WHERE (((((0)
                                    OR (ref_1.n_name IS NULL))
                                AND (ref_2.p_retailprice IS NOT NULL))
                            OR (ref_4.c_phone IS NOT NULL))
                        AND (ref_1.n_regionkey IS NOT NULL))
                    AND (1)
                LIMIT 136))
        AND (((ref_1.n_regionkey IS NOT NULL)
                AND (ref_1.n_nationkey IS NOT NULL))
            OR (((EXISTS (
                            SELECT
                                ref_0.r_comment AS c0,
                                ref_5.r_comment AS c1,
                                ref_0.r_comment AS c2,
                                ref_0.r_name AS c3,
                                ref_5.r_comment AS c4,
                                ref_5.r_name AS c5,
                                ref_5.r_name AS c6,
                                (
                                    SELECT
                                        n_comment
                                    FROM
                                        main.nation
                                    LIMIT 1 offset 4) AS c7,
                                ref_0.r_comment AS c8,
                                (
                                    SELECT
                                        l_receiptdate
                                    FROM
                                        main.lineitem
                                    LIMIT 1 offset 6) AS c9,
                                ref_2.p_type AS c10,
                                ref_5.r_name AS c11
                            FROM
                                main.region AS ref_5
                            WHERE
                                90 IS NULL
                            LIMIT 84))
                    AND ((0)
                        OR (0)))
                OR (ref_1.n_nationkey IS NULL))))
    AND (EXISTS (
            SELECT
                ref_0.r_comment AS c0,
                ref_3.o_totalprice AS c1
            FROM
                main.part AS ref_6
            WHERE
                ref_1.n_nationkey IS NOT NULL)))
LIMIT 68) AS subq_0
WHERE ((((0)
            OR ((0)
                AND (0)))
        OR ((((((0)
                            OR (EXISTS (
                                    SELECT
                                        ref_7.r_regionkey AS c0, subq_0.c0 AS c1, 1 AS c2, ref_7.r_comment AS c3, (
                                            SELECT
                                                o_shippriority
                                            FROM
                                                main.orders
                                            LIMIT 1 offset 4) AS c4,
                                        ref_7.r_comment AS c5,
                                        subq_0.c0 AS c6,
                                        ref_7.r_comment AS c7,
                                        subq_0.c2 AS c8,
                                        ref_7.r_comment AS c9,
                                        ref_7.r_comment AS c10,
                                        (
                                            SELECT
                                                o_orderstatus
                                            FROM
                                                main.orders
                                            LIMIT 1 offset 61) AS c11,
                                        ref_7.r_comment AS c12,
                                        subq_0.c1 AS c13,
                                        subq_0.c1 AS c14,
                                        subq_0.c0 AS c15,
                                        subq_0.c0 AS c16,
                                        ref_7.r_regionkey AS c17,
                                        subq_0.c1 AS c18,
                                        (
                                            SELECT
                                                r_regionkey
                                            FROM
                                                main.region
                                            LIMIT 1 offset 2) AS c19,
                                        34 AS c20,
                                        ref_7.r_name AS c21,
                                        ref_7.r_regionkey AS c22,
                                        subq_0.c0 AS c23,
                                        subq_0.c2 AS c24,
                                        ref_7.r_name AS c25,
                                        ref_7.r_name AS c26,
                                        ref_7.r_regionkey AS c27,
                                        ref_7.r_comment AS c28,
                                        ref_7.r_name AS c29,
                                        subq_0.c0 AS c30,
                                        subq_0.c0 AS c31,
                                        subq_0.c0 AS c32,
                                        ref_7.r_comment AS c33,
                                        ref_7.r_comment AS c34,
                                        (
                                            SELECT
                                                r_comment
                                            FROM
                                                main.region
                                            LIMIT 1 offset 3) AS c35,
                                        subq_0.c2 AS c36,
                                        ref_7.r_name AS c37,
                                        subq_0.c2 AS c38,
                                        subq_0.c0 AS c39,
                                        (
                                            SELECT
                                                n_comment
                                            FROM
                                                main.nation
                                            LIMIT 1 offset 5) AS c40,
                                        subq_0.c1 AS c41,
                                        ref_7.r_comment AS c42
                                    FROM
                                        main.region AS ref_7
                                    WHERE
                                        0
                                    LIMIT 45)))
                        OR (subq_0.c0 IS NOT NULL))
                    OR (((subq_0.c0 IS NOT NULL)
                            OR (EXISTS (
                                    SELECT
                                        subq_0.c0 AS c0
                                    FROM
                                        main.orders AS ref_8
                                    WHERE
                                        subq_0.c0 IS NULL)))
                            AND (1)))
                    OR (1))
                OR ((subq_0.c2 IS NULL)
                    OR (EXISTS (
                            SELECT
                                subq_0.c0 AS c0
                            FROM
                                main.orders AS ref_9
                            WHERE (0)
                            AND (ref_9.o_orderkey IS NOT NULL))))))
        AND ((EXISTS (
                    SELECT
                        subq_0.c2 AS c0, ref_10.r_name AS c1, ref_10.r_regionkey AS c2, subq_0.c1 AS c3, ref_10.r_comment AS c4, ref_10.r_regionkey AS c5, subq_0.c0 AS c6, (
                            SELECT
                                ps_availqty
                            FROM
                                main.partsupp
                            LIMIT 1 offset 1) AS c7,
                        subq_0.c0 AS c8,
                        subq_0.c1 AS c9,
                        subq_0.c0 AS c10,
                        subq_0.c0 AS c11,
                        ref_10.r_name AS c12,
                        subq_0.c2 AS c13,
                        ref_10.r_name AS c14
                    FROM
                        main.region AS ref_10
                    WHERE
                        0))
                AND (
                    CASE WHEN subq_0.c1 IS NOT NULL THEN
                        subq_0.c1
                    ELSE
                        subq_0.c1
                    END IS NULL)))
        OR ((((((((EXISTS (
                                            SELECT
                                                ref_11.s_nationkey AS c0, ref_11.s_suppkey AS c1, ref_11.s_nationkey AS c2, ref_11.s_address AS c3, ref_11.s_comment AS c4, subq_0.c0 AS c5, subq_0.c2 AS c6
                                            FROM
                                                main.supplier AS ref_11
                                            WHERE
                                                EXISTS (
                                                    SELECT
                                                        ref_11.s_phone AS c0, ref_11.s_name AS c1, ref_12.r_regionkey AS c2, ref_11.s_acctbal AS c3
                                                    FROM
                                                        main.region AS ref_12
                                                    WHERE (((((1)
                                                                    AND ((1)
                                                                        AND (1)))
                                                                OR (0))
                                                            OR (((ref_11.s_suppkey IS NOT NULL)
                                                                    AND (EXISTS (
                                                                            SELECT
                                                                                ref_13.c_acctbal AS c0, ref_13.c_comment AS c1, ref_12.r_comment AS c2, ref_11.s_nationkey AS c3, (
                                                                                    SELECT
                                                                                        c_mktsegment
                                                                                    FROM
                                                                                        main.customer
                                                                                    LIMIT 1 offset 5) AS c4,
                                                                                ref_12.r_regionkey AS c5,
                                                                                ref_13.c_name AS c6,
                                                                                ref_13.c_comment AS c7,
                                                                                ref_12.r_comment AS c8,
                                                                                ref_11.s_address AS c9,
                                                                                ref_11.s_acctbal AS c10,
                                                                                (
                                                                                    SELECT
                                                                                        c_nationkey
                                                                                    FROM
                                                                                        main.customer
                                                                                    LIMIT 1 offset 65) AS c11,
                                                                                subq_0.c0 AS c12,
                                                                                subq_0.c0 AS c13,
                                                                                subq_0.c1 AS c14,
                                                                                ref_13.c_acctbal AS c15,
                                                                                ref_13.c_mktsegment AS c16,
                                                                                (
                                                                                    SELECT
                                                                                        c_nationkey
                                                                                    FROM
                                                                                        main.customer
                                                                                    LIMIT 1 offset 2) AS c17,
                                                                                ref_12.r_comment AS c18,
                                                                                ref_12.r_regionkey AS c19
                                                                            FROM
                                                                                main.customer AS ref_13
                                                                            WHERE
                                                                                1)))
                                                                    AND (0)))
                                                            OR (ref_11.s_acctbal IS NULL))
                                                        AND (1))
                                                LIMIT 122))
                                        OR ((((1)
                                                    AND (1))
                                                OR (EXISTS (
                                                        SELECT
                                                            subq_0.c0 AS c0,
                                                            62 AS c1,
                                                            subq_0.c0 AS c2
                                                        FROM
                                                            main.supplier AS ref_14
                                                        WHERE
                                                            1
                                                        LIMIT 102)))
                                            OR (subq_0.c1 IS NULL)))
                                    AND ((0)
                                        AND (1)))
                                AND ((EXISTS (
                                            SELECT
                                                ref_15.c_comment AS c0,
                                                ref_15.c_comment AS c1
                                            FROM
                                                main.customer AS ref_15
                                            WHERE
                                                ref_15.c_mktsegment IS NOT NULL
                                            LIMIT 53))
                                    OR (((1)
                                            AND ((1)
                                                OR (1)))
                                        AND (EXISTS (
                                                SELECT
                                                    ref_16.p_type AS c0,
                                                    subq_0.c0 AS c1,
                                                    subq_0.c0 AS c2,
                                                    ref_16.p_type AS c3,
                                                    subq_0.c1 AS c4,
                                                    ref_16.p_size AS c5,
                                                    subq_0.c2 AS c6,
                                                    subq_0.c1 AS c7,
                                                    ref_16.p_mfgr AS c8,
                                                    ref_16.p_size AS c9,
                                                    subq_0.c0 AS c10,
                                                    subq_0.c0 AS c11,
                                                    subq_0.c1 AS c12,
                                                    ref_16.p_retailprice AS c13,
                                                    subq_0.c1 AS c14,
                                                    subq_0.c2 AS c15,
                                                    (
                                                        SELECT
                                                            n_comment
                                                        FROM
                                                            main.nation
                                                        LIMIT 1 offset 3) AS c16,
                                                    ref_16.p_retailprice AS c17,
                                                    subq_0.c0 AS c18,
                                                    subq_0.c1 AS c19,
                                                    subq_0.c1 AS c20,
                                                    (
                                                        SELECT
                                                            ps_comment
                                                        FROM
                                                            main.partsupp
                                                        LIMIT 1 offset 6) AS c21,
                                                    ref_16.p_comment AS c22,
                                                    ref_16.p_partkey AS c23,
                                                    ref_16.p_container AS c24,
                                                    5 AS c25,
                                                    subq_0.c1 AS c26,
                                                    ref_16.p_name AS c27,
                                                    ref_16.p_size AS c28,
                                                    ref_16.p_partkey AS c29,
                                                    subq_0.c0 AS c30,
                                                    subq_0.c2 AS c31,
                                                    subq_0.c0 AS c32,
                                                    subq_0.c2 AS c33,
                                                    subq_0.c2 AS c34
                                                FROM
                                                    main.part AS ref_16
                                                WHERE
                                                    EXISTS (
                                                        SELECT
                                                            ref_16.p_type AS c0, ref_17.ps_partkey AS c1, subq_0.c2 AS c2
                                                        FROM
                                                            main.partsupp AS ref_17
                                                        WHERE
                                                            1
                                                        LIMIT 86))))))
                                AND (0))
                            AND (subq_0.c1 IS NOT NULL))
                        AND (subq_0.c0 IS NULL))
                    OR (EXISTS (
                            SELECT
                                (
                                    SELECT
                                        ps_availqty
                                    FROM
                                        main.partsupp
                                    LIMIT 1 offset 3) AS c0,
                                ref_19.l_orderkey AS c1,
                                ref_19.l_discount AS c2,
                                (
                                    SELECT
                                        l_linestatus
                                    FROM
                                        main.lineitem
                                    LIMIT 1 offset 2) AS c3
                            FROM
                                main.partsupp AS ref_18
                            LEFT JOIN main.lineitem AS ref_19 ON (ref_18.ps_availqty IS NOT NULL)
                        WHERE
                            subq_0.c1 IS NOT NULL)))
            LIMIT 93
