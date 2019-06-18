SELECT
    subq_1.c1 AS c0,
    subq_1.c0 AS c1,
    subq_1.c1 AS c2,
    subq_1.c1 AS c3,
    subq_1.c0 AS c4,
    subq_1.c0 AS c5,
    CASE WHEN ((EXISTS (
                SELECT
                    86 AS c0
                FROM
                    main.supplier AS ref_6
                WHERE (0)
                OR (0)
            LIMIT 102))
        OR ((subq_1.c0 IS NULL)
            OR (subq_1.c0 IS NOT NULL)))
    OR ((EXISTS (
                SELECT
                    ref_8.l_linenumber AS c0,
                    (
                        SELECT
                            o_custkey
                        FROM
                            main.orders
                        LIMIT 1 offset 4) AS c1,
                    ref_7.p_mfgr AS c2,
                    subq_1.c0 AS c3,
                    subq_1.c0 AS c4,
                    ref_7.p_container AS c5,
                    ref_8.l_receiptdate AS c6,
                    ref_7.p_brand AS c7,
                    ref_7.p_container AS c8,
                    ref_7.p_type AS c9,
                    subq_1.c2 AS c10,
                    ref_7.p_mfgr AS c11,
                    ref_7.p_brand AS c12,
                    ref_8.l_linenumber AS c13,
                    ref_8.l_receiptdate AS c14,
                    subq_1.c0 AS c15,
                    subq_1.c1 AS c16,
                    subq_1.c2 AS c17,
                    ref_7.p_container AS c18,
                    subq_1.c2 AS c19,
                    subq_1.c1 AS c20,
                    ref_7.p_mfgr AS c21,
                    ref_7.p_partkey AS c22
                FROM
                    main.part AS ref_7
                RIGHT JOIN main.lineitem AS ref_8 ON (ref_7.p_size = ref_8.l_orderkey)
            WHERE
                ref_8.l_linenumber IS NOT NULL))
        AND (EXISTS (
                SELECT
                    subq_1.c0 AS c0, 1 AS c1, subq_1.c0 AS c2, subq_1.c2 AS c3, subq_1.c1 AS c4, ref_9.r_name AS c5, subq_1.c0 AS c6, ref_9.r_comment AS c7, subq_1.c2 AS c8, ref_9.r_comment AS c9, ref_9.r_comment AS c10, ref_9.r_regionkey AS c11, ref_9.r_name AS c12, ref_9.r_comment AS c13, ref_9.r_name AS c14
                FROM
                    main.region AS ref_9
                WHERE
                    ref_9.r_regionkey IS NULL))) THEN
    (
        SELECT
            c_address
        FROM
            main.customer
        LIMIT 1 offset 2)
ELSE
    (
        SELECT
            c_address
        FROM
            main.customer
        LIMIT 1 offset 2)
    END AS c6,
    subq_1.c1 AS c7,
    subq_1.c0 AS c8,
    subq_1.c1 AS c9,
    subq_1.c2 AS c10,
    subq_1.c0 AS c11
FROM (
    SELECT
        subq_0.c26 AS c0,
        subq_0.c7 AS c1,
        subq_0.c21 AS c2
    FROM (
        SELECT
            ref_0.s_suppkey AS c0,
            ref_0.s_nationkey AS c1,
            ref_0.s_acctbal AS c2,
            ref_0.s_phone AS c3,
            ref_0.s_phone AS c4,
            ref_0.s_phone AS c5,
            ref_0.s_name AS c6,
            ref_0.s_address AS c7,
            38 AS c8,
            ref_0.s_nationkey AS c9,
            ref_0.s_name AS c10,
            ref_0.s_nationkey AS c11,
            ref_0.s_phone AS c12,
            ref_0.s_name AS c13,
            ref_0.s_acctbal AS c14,
            ref_0.s_phone AS c15,
            ref_0.s_name AS c16,
            ref_0.s_acctbal AS c17,
            ref_0.s_comment AS c18,
            ref_0.s_address AS c19,
            ref_0.s_comment AS c20,
            ref_0.s_address AS c21,
            ref_0.s_comment AS c22,
            ref_0.s_comment AS c23,
            ref_0.s_name AS c24,
            ref_0.s_suppkey AS c25,
            ref_0.s_address AS c26,
            ref_0.s_phone AS c27,
            ref_0.s_name AS c28,
            ref_0.s_name AS c29,
            ref_0.s_address AS c30,
            ref_0.s_address AS c31,
            ref_0.s_nationkey AS c32,
            ref_0.s_comment AS c33
        FROM
            main.supplier AS ref_0
        WHERE (
            SELECT
                r_regionkey
            FROM
                main.region
            LIMIT 1 offset 1) IS NOT NULL
    LIMIT 106) AS subq_0
    RIGHT JOIN main.part AS ref_1 ON (subq_0.c13 = ref_1.p_name)
WHERE ((0)
    AND ((EXISTS (
                SELECT
                    subq_0.c31 AS c0, ref_1.p_type AS c1
                FROM
                    main.nation AS ref_2
                WHERE
                    EXISTS (
                        SELECT
                            ref_1.p_name AS c0, ref_3.o_orderkey AS c1, ref_2.n_comment AS c2, subq_0.c3 AS c3
                        FROM
                            main.orders AS ref_3
                        WHERE (0)
                        AND (((0)
                                AND ((ref_3.o_orderdate IS NULL)
                                    AND ((
                                            SELECT
                                                n_nationkey
                                            FROM
                                                main.nation
                                            LIMIT 1 offset 6) IS NOT NULL)))
                            OR (1)))
                LIMIT 65))
        OR ((subq_0.c16 IS NOT NULL)
            AND (1))))
    OR (EXISTS (
            SELECT
                ref_1.p_comment AS c0,
                subq_0.c26 AS c1,
                ref_1.p_retailprice AS c2,
                ref_4.o_comment AS c3,
                subq_0.c9 AS c4,
                subq_0.c3 AS c5
            FROM
                main.orders AS ref_4
            WHERE ((0)
                AND (0))
            OR ((0)
                AND ((1)
                    AND (EXISTS (
                            SELECT
                                ref_4.o_clerk AS c0, ref_4.o_shippriority AS c1, ref_5.o_custkey AS c2, ref_5.o_orderpriority AS c3
                            FROM
                                main.orders AS ref_5
                            WHERE (ref_5.o_orderkey IS NOT NULL)
                            OR (1)
                        LIMIT 128))))))
LIMIT 26) AS subq_1
WHERE (subq_1.c0 IS NOT NULL)
AND ((0)
    OR ((0)
        OR (EXISTS (
                SELECT
                    subq_1.c2 AS c0,
                    ref_10.l_tax AS c1,
                    ref_10.l_shipmode AS c2
                FROM
                    main.lineitem AS ref_10
                WHERE
                    1
                LIMIT 128))))
LIMIT 53
