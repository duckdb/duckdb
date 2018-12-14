SELECT
    subq_1.c6 AS c0,
    (
        SELECT
            p_container
        FROM
            main.part
        LIMIT 1 offset 34) AS c1,
    subq_1.c4 AS c2,
    (
        SELECT
            s_name
        FROM
            main.supplier
        LIMIT 1 offset 1) AS c3,
    subq_1.c4 AS c4,
    subq_1.c5 AS c5,
    subq_1.c6 AS c6,
    subq_1.c2 AS c7,
    subq_1.c0 AS c8,
    subq_1.c2 AS c9,
    CAST(nullif (subq_1.c0, subq_1.c0) AS INTEGER) AS c10
FROM (
    SELECT
        87 AS c0,
        ref_1.l_returnflag AS c1,
        ref_1.l_commitdate AS c2,
        ref_1.l_extendedprice AS c3,
        ref_2.c_name AS c4,
        CASE WHEN ref_1.l_linenumber IS NULL THEN
            subq_0.c0
        ELSE
            subq_0.c0
        END AS c5,
        subq_0.c1 AS c6
    FROM (
        SELECT
            ref_0.n_comment AS c0,
            67 AS c1
        FROM
            main.nation AS ref_0
        WHERE
            1) AS subq_0
    RIGHT JOIN main.lineitem AS ref_1
    INNER JOIN main.customer AS ref_2 ON (ref_2.c_acctbal IS NULL) ON (subq_0.c0 = ref_1.l_returnflag)
WHERE ((((((EXISTS (
                            SELECT
                                subq_0.c1 AS c0
                            FROM
                                main.part AS ref_3
                            WHERE (1)
                            AND ((ref_3.p_container IS NULL)
                                AND (EXISTS (
                                        SELECT
                                            ref_3.p_type AS c0, ref_4.r_comment AS c1, ref_1.l_tax AS c2
                                        FROM
                                            main.region AS ref_4
                                        WHERE
                                            0
                                        LIMIT 46)))
                        LIMIT 35))
                AND (0))
            OR (((((
                                SELECT
                                    c_mktsegment
                                FROM
                                    main.customer
                                LIMIT 1 offset 4)
                            IS NULL)
                        AND (((EXISTS (
                                        SELECT
                                            ref_1.l_shipdate AS c0,
                                            ref_1.l_commitdate AS c1,
                                            subq_0.c1 AS c2,
                                            (
                                                SELECT
                                                    n_nationkey
                                                FROM
                                                    main.nation
                                                LIMIT 1 offset 24) AS c3,
                                            ref_1.l_commitdate AS c4,
                                            ref_5.o_orderpriority AS c5,
                                            ref_2.c_address AS c6,
                                            ref_2.c_name AS c7,
                                            ref_1.l_quantity AS c8,
                                            subq_0.c0 AS c9,
                                            (
                                                SELECT
                                                    r_name
                                                FROM
                                                    main.region
                                                LIMIT 1 offset 3) AS c10
                                        FROM
                                            main.orders AS ref_5
                                        WHERE
                                            EXISTS (
                                                SELECT
                                                    subq_0.c0 AS c0, ref_1.l_tax AS c1, ref_5.o_clerk AS c2, ref_5.o_totalprice AS c3, subq_0.c0 AS c4, ref_1.l_linenumber AS c5, ref_6.s_phone AS c6, ref_2.c_nationkey AS c7, ref_1.l_shipmode AS c8, ref_1.l_discount AS c9, ref_2.c_phone AS c10, ref_1.l_orderkey AS c11, ref_2.c_mktsegment AS c12, ref_1.l_suppkey AS c13, ref_6.s_acctbal AS c14, ref_5.o_orderdate AS c15, ref_1.l_shipmode AS c16, ref_6.s_nationkey AS c17, ref_5.o_orderkey AS c18
                                                FROM
                                                    main.supplier AS ref_6
                                                WHERE (1)
                                                AND (0)
                                            LIMIT 125)
                                    LIMIT 80))
                            AND ((0)
                                OR ((EXISTS (
                                            SELECT
                                                ref_1.l_linenumber AS c0,
                                                ref_7.n_comment AS c1,
                                                ref_1.l_comment AS c2,
                                                ref_1.l_returnflag AS c3,
                                                subq_0.c0 AS c4,
                                                ref_1.l_shipmode AS c5,
                                                ref_2.c_custkey AS c6,
                                                ref_2.c_custkey AS c7,
                                                subq_0.c0 AS c8,
                                                ref_7.n_regionkey AS c9,
                                                ref_7.n_nationkey AS c10
                                            FROM
                                                main.nation AS ref_7
                                            WHERE
                                                1
                                            LIMIT 97))
                                    AND (0))))
                        AND (1)))
                OR (0))
            AND (((0)
                    AND (1))
                AND (0))))
    AND ((0)
        OR ((((0)
                    OR (subq_0.c0 IS NOT NULL))
                OR (ref_1.l_commitdate IS NOT NULL))
            AND (EXISTS (
                    SELECT
                        ref_1.l_suppkey AS c0,
                        ref_2.c_phone AS c1,
                        ref_8.s_address AS c2,
                        subq_0.c1 AS c3,
                        subq_0.c1 AS c4
                    FROM
                        main.supplier AS ref_8
                    WHERE ((0)
                        OR ((ref_8.s_name IS NULL)
                            OR ((ref_8.s_address IS NULL)
                                AND (ref_1.l_receiptdate IS NOT NULL))))
                    OR (((0)
                            OR (1))
                        AND (0)))))))
OR ((1)
    AND ((((subq_0.c1 IS NULL)
                OR (((ref_2.c_comment IS NULL)
                        OR ((subq_0.c0 IS NOT NULL)
                            OR (0)))
                    OR (1)))
            AND ((ref_1.l_returnflag IS NULL)
                OR ((0)
                    OR ((1)
                        AND ((ref_2.c_mktsegment IS NULL)
                            AND (((ref_2.c_name IS NULL)
                                    AND (((ref_2.c_acctbal IS NOT NULL)
                                            OR (((EXISTS (
                                                            SELECT
                                                                subq_0.c0 AS c0
                                                            FROM
                                                                main.region AS ref_9
                                                            WHERE (0)
                                                            AND (EXISTS (
                                                                    SELECT
                                                                        ref_1.l_receiptdate AS c0, ref_10.p_brand AS c1, 52 AS c2, ref_9.r_comment AS c3, ref_2.c_address AS c4, ref_2.c_mktsegment AS c5, ref_1.l_extendedprice AS c6, ref_9.r_regionkey AS c7, ref_10.p_size AS c8, ref_2.c_name AS c9, ref_2.c_acctbal AS c10, ref_10.p_container AS c11, ref_9.r_comment AS c12, ref_10.p_comment AS c13
                                                                    FROM
                                                                        main.part AS ref_10
                                                                    WHERE (0)
                                                                    OR (subq_0.c1 IS NOT NULL)))))
                                                    OR (0))
                                                AND (0)))
                                        OR ((0)
                                            OR (ref_2.c_custkey IS NULL))))
                                AND (EXISTS (
                                        SELECT
                                            ref_11.l_linestatus AS c0, ref_11.l_extendedprice AS c1
                                        FROM
                                            main.lineitem AS ref_11
                                        WHERE ((1)
                                            AND ((((0)
                                                        AND (0))
                                                    AND (EXISTS (
                                                            SELECT
                                                                ref_12.r_regionkey AS c0, subq_0.c1 AS c1
                                                            FROM
                                                                main.region AS ref_12
                                                            WHERE (1)
                                                            AND (ref_12.r_regionkey IS NOT NULL))))
                                                AND (EXISTS (
                                                        SELECT
                                                            ref_1.l_tax AS c0, ref_1.l_quantity AS c1, ref_1.l_suppkey AS c2, ref_13.r_name AS c3, ref_11.l_shipinstruct AS c4, subq_0.c1 AS c5
                                                        FROM
                                                            main.region AS ref_13
                                                        WHERE
                                                            EXISTS (
                                                                SELECT
                                                                    subq_0.c0 AS c0, (
                                                                        SELECT
                                                                            c_custkey
                                                                        FROM
                                                                            main.customer
                                                                        LIMIT 1 offset 23) AS c1
                                                                FROM
                                                                    main.customer AS ref_14
                                                                WHERE
                                                                    1)
                                                            LIMIT 50))))
                                            AND (ref_2.c_acctbal IS NOT NULL)))))))))
            OR ((EXISTS (
                        SELECT
                            ref_1.l_shipmode AS c0,
                            subq_0.c0 AS c1,
                            ref_15.ps_partkey AS c2,
                            ref_15.ps_comment AS c3,
                            ref_15.ps_partkey AS c4,
                            ref_2.c_name AS c5,
                            ref_15.ps_supplycost AS c6,
                            ref_1.l_shipdate AS c7
                        FROM
                            main.partsupp AS ref_15
                        WHERE (1)
                        OR ((1)
                            AND (1))
                    LIMIT 108))
            AND ((0)
                OR (subq_0.c1 IS NOT NULL))))))
AND (1))
OR (((0)
        OR (subq_0.c0 IS NULL))
    OR ((1)
        OR (0)))) AS subq_1
WHERE (subq_1.c4 IS NULL)
OR ((subq_1.c3 IS NULL)
    AND (((1)
            OR ((((1)
                        OR (1))
                    AND (subq_1.c3 IS NULL))
                OR ((subq_1.c6 IS NOT NULL)
                    OR (subq_1.c3 IS NOT NULL))))
        AND (1)))
LIMIT 111
