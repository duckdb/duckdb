WITH jennifer_0 AS (
    SELECT
        subq_0.c3 AS c0
    FROM (
        SELECT
            ref_0.r_comment AS c0,
            ref_0.r_name AS c1,
            ref_0.r_regionkey AS c2,
            ref_0.r_name AS c3
        FROM
            main.region AS ref_0
        WHERE
            ref_0.r_regionkey IS NULL) AS subq_0
    WHERE
        EXISTS (
            SELECT
                CASE WHEN ((EXISTS (
                                SELECT
                                    ref_2.r_name AS c0, subq_0.c1 AS c1, subq_0.c0 AS c2, subq_0.c0 AS c3, ref_1.l_extendedprice AS c4, 79 AS c5, ref_2.r_name AS c6, ref_1.l_commitdate AS c7, ref_1.l_extendedprice AS c8, subq_0.c0 AS c9, subq_0.c0 AS c10, ref_2.r_regionkey AS c11, ref_1.l_receiptdate AS c12, ref_2.r_regionkey AS c13, ref_2.r_name AS c14
                                FROM
                                    main.region AS ref_2
                                WHERE ((EXISTS (
                                            SELECT
                                                3 AS c0, ref_1.l_shipdate AS c1, subq_0.c0 AS c2, ref_2.r_name AS c3, ref_2.r_comment AS c4, ref_2.r_regionkey AS c5, ref_2.r_name AS c6, subq_0.c0 AS c7, ref_1.l_discount AS c8
                                            FROM
                                                main.region AS ref_3
                                            WHERE
                                                subq_0.c3 IS NULL))
                                        OR (EXISTS (
                                                SELECT
                                                    ref_1.l_partkey AS c0, ref_4.l_receiptdate AS c1, subq_0.c0 AS c2, subq_0.c3 AS c3, ref_2.r_name AS c4, ref_2.r_comment AS c5, ref_4.l_suppkey AS c6, subq_0.c3 AS c7, (
                                                        SELECT
                                                            n_nationkey
                                                        FROM
                                                            main.nation
                                                        LIMIT 1 offset 32) AS c8,
                                                    65 AS c9,
                                                    ref_1.l_returnflag AS c10,
                                                    ref_2.r_comment AS c11,
                                                    34 AS c12,
                                                    (
                                                        SELECT
                                                            s_acctbal
                                                        FROM
                                                            main.supplier
                                                        LIMIT 1 offset 4) AS c13,
                                                    99 AS c14,
                                                    66 AS c15,
                                                    ref_2.r_regionkey AS c16,
                                                    subq_0.c1 AS c17
                                                FROM
                                                    main.lineitem AS ref_4
                                                WHERE
                                                    EXISTS (
                                                        SELECT
                                                            ref_4.l_quantity AS c0
                                                        FROM
                                                            main.customer AS ref_5
                                                        WHERE
                                                            0))))
                                            AND (((EXISTS (
                                                            SELECT
                                                                ref_2.r_regionkey AS c0
                                                            FROM
                                                                main.region AS ref_6
                                                            WHERE (1)
                                                            OR (1)))
                                                    OR ((0)
                                                        AND (subq_0.c2 IS NOT NULL)))
                                                AND ((((1)
                                                            AND (0))
                                                        OR (((1)
                                                                OR ((subq_0.c0 IS NOT NULL)
                                                                    AND (((0)
                                                                            AND (1))
                                                                        OR (1))))
                                                            AND (ref_1.l_commitdate IS NULL)))
                                                    AND (0)))))
                                    AND (0))
                                OR (EXISTS (
                                        SELECT
                                            subq_0.c2 AS c0, ref_7.n_name AS c1, subq_0.c1 AS c2, ref_1.l_extendedprice AS c3
                                        FROM
                                            main.nation AS ref_7
                                        WHERE
                                            ref_1.l_returnflag IS NULL
                                        LIMIT 170)) THEN
                                ref_1.l_shipdate
                            ELSE
                                ref_1.l_shipdate
                            END AS c0,
                            ref_1.l_shipdate AS c1,
                            subq_0.c1 AS c2,
                            subq_0.c3 AS c3,
                            ref_1.l_discount AS c4,
                            subq_0.c1 AS c5,
                            ref_1.l_discount AS c6,
                            ref_1.l_tax AS c7,
                            subq_0.c1 AS c8,
                            ref_1.l_orderkey AS c9,
                            subq_0.c1 AS c10,
                            ref_1.l_quantity AS c11
                        FROM
                            main.lineitem AS ref_1
                        WHERE (((((EXISTS (
                                                SELECT
                                                    ref_8.p_retailprice AS c0, ref_1.l_comment AS c1, subq_0.c1 AS c2, ref_1.l_commitdate AS c3, ref_1.l_returnflag AS c4, subq_0.c3 AS c5, ref_8.p_partkey AS c6, ref_1.l_linenumber AS c7, ref_1.l_partkey AS c8, (
                                                        SELECT
                                                            c_name
                                                        FROM
                                                            main.customer
                                                        LIMIT 1 offset 85) AS c9,
                                                    22 AS c10
                                                FROM
                                                    main.part AS ref_8
                                                WHERE
                                                    1))
                                            OR ((subq_0.c2 IS NOT NULL)
                                                OR (((((((((0)
                                                                                OR (subq_0.c3 IS NOT NULL))
                                                                            OR ((0)
                                                                                OR (1)))
                                                                        AND ((1)
                                                                            OR (EXISTS (
                                                                                    SELECT
                                                                                        ref_1.l_shipmode AS c0, ref_1.l_orderkey AS c1, subq_0.c3 AS c2, subq_0.c2 AS c3, subq_0.c3 AS c4, subq_0.c3 AS c5, ref_9.p_container AS c6, ref_1.l_shipmode AS c7, ref_1.l_linenumber AS c8, ref_9.p_type AS c9, subq_0.c2 AS c10, (
                                                                                            SELECT
                                                                                                o_orderkey
                                                                                            FROM
                                                                                                main.orders
                                                                                            LIMIT 1 offset 3) AS c11,
                                                                                        ref_1.l_extendedprice AS c12,
                                                                                        subq_0.c2 AS c13,
                                                                                        ref_1.l_linestatus AS c14,
                                                                                        ref_1.l_shipdate AS c15,
                                                                                        ref_1.l_extendedprice AS c16,
                                                                                        ref_1.l_shipmode AS c17,
                                                                                        subq_0.c1 AS c18,
                                                                                        ref_9.p_size AS c19,
                                                                                        ref_1.l_shipmode AS c20,
                                                                                        ref_9.p_type AS c21,
                                                                                        subq_0.c3 AS c22
                                                                                    FROM
                                                                                        main.part AS ref_9
                                                                                    WHERE
                                                                                        EXISTS (
                                                                                            SELECT
                                                                                                96 AS c0, ref_9.p_name AS c1
                                                                                            FROM
                                                                                                main.region AS ref_10
                                                                                            WHERE ((1)
                                                                                                OR ((((((1)
                                                                                                                    OR ((subq_0.c1 IS NOT NULL)
                                                                                                                        OR ((0)
                                                                                                                            OR (((ref_9.p_brand IS NOT NULL)
                                                                                                                                    AND ((subq_0.c3 IS NULL)
                                                                                                                                        OR (0)))
                                                                                                                                AND (EXISTS (
                                                                                                                                        SELECT
                                                                                                                                            (
                                                                                                                                                SELECT
                                                                                                                                                    c_acctbal
                                                                                                                                                FROM
                                                                                                                                                    main.customer
                                                                                                                                                LIMIT 1 offset 4) AS c0
                                                                                                                                        FROM
                                                                                                                                            main.region AS ref_11
                                                                                                                                        WHERE (1)
                                                                                                                                        OR (0)))))))
                                                                                                                AND ((0)
                                                                                                                    OR (0)))
                                                                                                            AND ((0)
                                                                                                                AND (EXISTS (
                                                                                                                        SELECT
                                                                                                                            ref_12.n_name AS c0, ref_1.l_tax AS c1, ref_1.l_extendedprice AS c2, (
                                                                                                                                SELECT
                                                                                                                                    s_acctbal
                                                                                                                                FROM
                                                                                                                                    main.supplier
                                                                                                                                LIMIT 1 offset 5) AS c3,
                                                                                                                            subq_0.c3 AS c4,
                                                                                                                            subq_0.c0 AS c5
                                                                                                                        FROM
                                                                                                                            main.nation AS ref_12
                                                                                                                        WHERE
                                                                                                                            1
                                                                                                                        LIMIT 105))))
                                                                                                        OR (1))
                                                                                                    OR (0)))
                                                                                            AND (0)
                                                                                        LIMIT 35)))))
                                                                    OR (0))
                                                                OR (((1)
                                                                        OR (1))
                                                                    OR (ref_1.l_receiptdate IS NULL)))
                                                            AND (ref_1.l_suppkey IS NULL))
                                                        AND (0))
                                                    OR (EXISTS (
                                                            SELECT
                                                                ref_1.l_shipdate AS c0,
                                                                14 AS c1,
                                                                subq_0.c3 AS c2,
                                                                (
                                                                    SELECT
                                                                        ps_suppkey
                                                                    FROM
                                                                        main.partsupp
                                                                    LIMIT 1 offset 1) AS c3,
                                                                82 AS c4
                                                            FROM
                                                                main.part AS ref_13
                                                            WHERE ((((subq_0.c1 IS NULL)
                                                                        OR (0))
                                                                    AND (0))
                                                                AND (1))
                                                            OR ((1)
                                                                AND (((ref_1.l_commitdate IS NOT NULL)
                                                                        AND (0))
                                                                    AND ((subq_0.c3 IS NOT NULL)
                                                                        OR (0)))))))))
                                        AND (1))
                                    AND ((1)
                                        OR ((EXISTS (
                                                    SELECT
                                                        ref_14.c_custkey AS c0, ref_1.l_shipinstruct AS c1, subq_0.c1 AS c2, ref_1.l_orderkey AS c3, subq_0.c0 AS c4, ref_1.l_shipinstruct AS c5, ref_14.c_acctbal AS c6, subq_0.c0 AS c7, subq_0.c1 AS c8, subq_0.c2 AS c9, ref_14.c_mktsegment AS c10, subq_0.c2 AS c11, ref_14.c_address AS c12, ref_1.l_linenumber AS c13, ref_14.c_name AS c14, ref_14.c_comment AS c15
                                                    FROM
                                                        main.customer AS ref_14
                                                    WHERE
                                                        1))
                                                AND (subq_0.c1 IS NOT NULL))))
                                    OR (1))
                                AND (ref_1.l_receiptdate IS NOT NULL)
                            LIMIT 58)
                    LIMIT 129
),
jennifer_1 AS (
    SELECT
        (
            SELECT
                s_comment
            FROM
                main.supplier
            LIMIT 1 offset 4) AS c0,
        subq_1.c6 AS c1,
        subq_1.c1 AS c2,
        subq_1.c3 AS c3,
        subq_1.c4 AS c4,
        subq_1.c2 AS c5,
        subq_1.c5 AS c6,
        subq_1.c6 AS c7,
        subq_1.c4 AS c8
    FROM (
        SELECT
            ref_15.s_comment AS c0,
            (
                SELECT
                    p_name
                FROM
                    main.part
                LIMIT 1 offset 3) AS c1,
            ref_15.s_suppkey AS c2,
            ref_15.s_address AS c3,
            ref_15.s_acctbal AS c4,
            ref_15.s_suppkey AS c5,
            ref_15.s_comment AS c6
        FROM
            main.supplier AS ref_15
        WHERE (EXISTS (
                SELECT
                    ref_16.ps_partkey AS c0, 31 AS c1, ref_17.l_linenumber AS c2, ref_17.l_returnflag AS c3, ref_17.l_quantity AS c4, ref_17.l_commitdate AS c5, (
                        SELECT
                            s_acctbal
                        FROM
                            main.supplier
                        LIMIT 1 offset 50) AS c6,
                    (
                        SELECT
                            o_clerk
                        FROM
                            main.orders
                        LIMIT 1 offset 4) AS c7,
                    ref_17.l_discount AS c8
                FROM
                    main.partsupp AS ref_16
                    INNER JOIN main.lineitem AS ref_17 ON (ref_16.ps_supplycost = ref_17.l_extendedprice)
                WHERE
                    0))
            AND (1)) AS subq_1
    WHERE (subq_1.c0 IS NOT NULL)
    AND (subq_1.c5 IS NOT NULL)
LIMIT 107
)
SELECT
    subq_2.c1 AS c0, subq_2.c1 AS c1, subq_2.c0 AS c2
FROM (
    SELECT
        ref_18.c3 AS c0,
        ref_18.c6 AS c1
    FROM
        jennifer_1 AS ref_18
        INNER JOIN main.lineitem AS ref_19
        INNER JOIN main.nation AS ref_20
        LEFT JOIN main.lineitem AS ref_21 ON (ref_20.n_name = ref_21.l_returnflag) ON (ref_20.n_nationkey IS NOT NULL) ON (ref_18.c0 = ref_19.l_returnflag)
    WHERE
        ref_18.c6 IS NULL
    LIMIT 101) AS subq_2
WHERE (subq_2.c0 IS NULL)
AND (subq_2.c0 IS NOT NULL)
LIMIT 120
