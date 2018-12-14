SELECT
    ref_9.l_shipmode AS c0
FROM (
    SELECT
        ref_0.o_clerk AS c0,
        ref_0.o_orderpriority AS c1,
        ref_0.o_shippriority AS c2,
        ref_0.o_shippriority AS c3,
        7 AS c4,
        ref_0.o_totalprice AS c5,
        ref_0.o_orderkey AS c6,
        ref_0.o_orderdate AS c7,
        ref_0.o_orderdate AS c8,
        ref_0.o_shippriority AS c9,
        (
            SELECT
                o_orderkey
            FROM
                main.orders
            LIMIT 1 offset 3) AS c10,
        ref_0.o_orderstatus AS c11,
        ref_0.o_orderdate AS c12,
        (
            SELECT
                p_mfgr
            FROM
                main.part
            LIMIT 1 offset 1) AS c13,
        ref_0.o_orderkey AS c14,
        ref_0.o_shippriority AS c15,
        ref_0.o_comment AS c16,
        ref_0.o_shippriority AS c17
    FROM
        main.orders AS ref_0
    WHERE (((EXISTS (
                    SELECT
                        ref_1.n_regionkey AS c0, ref_0.o_clerk AS c1, ref_1.n_regionkey AS c2, ref_0.o_shippriority AS c3, ref_0.o_custkey AS c4, ref_0.o_orderpriority AS c5, ref_0.o_orderkey AS c6, ref_0.o_totalprice AS c7, 76 AS c8, ref_0.o_totalprice AS c9
                    FROM
                        main.nation AS ref_1
                    WHERE
                        ref_0.o_custkey IS NULL
                    LIMIT 103))
            AND ((0)
                AND ((ref_0.o_totalprice IS NULL)
                    AND (((((1)
                                    OR (ref_0.o_custkey IS NULL))
                                AND (1))
                            OR (0))
                        AND (((1)
                                AND (ref_0.o_orderkey IS NULL))
                            AND ((EXISTS (
                                        SELECT
                                            ref_2.r_regionkey AS c0,
                                            ref_0.o_comment AS c1,
                                            ref_2.r_regionkey AS c2,
                                            ref_2.r_regionkey AS c3,
                                            ref_2.r_regionkey AS c4,
                                            ref_0.o_orderpriority AS c5,
                                            ref_0.o_orderkey AS c6,
                                            ref_2.r_name AS c7,
                                            ref_0.o_orderdate AS c8
                                        FROM
                                            main.region AS ref_2
                                        WHERE
                                            ref_0.o_orderstatus IS NULL
                                        LIMIT 182))
                                AND (1)))))))
        OR (ref_0.o_totalprice IS NOT NULL))
    AND ((ref_0.o_totalprice IS NULL)
        AND (ref_0.o_shippriority IS NOT NULL))
LIMIT 99) AS subq_0
    LEFT JOIN (
        SELECT
            ref_3.l_extendedprice AS c0,
            ref_3.l_linenumber AS c1,
            ref_3.l_orderkey AS c2,
            ref_3.l_extendedprice AS c3,
            ref_4.o_custkey AS c4
        FROM
            main.lineitem AS ref_3
            INNER JOIN main.orders AS ref_4 ON (ref_3.l_quantity = ref_4.o_orderkey)
        WHERE (EXISTS (
                SELECT
                    ref_4.o_orderkey AS c0, (
                        SELECT
                            c_mktsegment
                        FROM
                            main.customer
                        LIMIT 1 offset 6) AS c1,
                    ref_3.l_tax AS c2,
                    ref_5.o_orderkey AS c3,
                    (
                        SELECT
                            c_address
                        FROM
                            main.customer
                        LIMIT 1 offset 5) AS c4,
                    ref_4.o_clerk AS c5,
                    ref_4.o_comment AS c6,
                    ref_3.l_shipinstruct AS c7,
                    ref_3.l_orderkey AS c8,
                    ref_4.o_clerk AS c9,
                    ref_4.o_shippriority AS c10,
                    ref_4.o_orderdate AS c11,
                    ref_3.l_shipinstruct AS c12,
                    ref_5.o_orderstatus AS c13,
                    ref_5.o_custkey AS c14,
                    ref_5.o_orderpriority AS c15,
                    ref_5.o_orderdate AS c16,
                    ref_4.o_orderdate AS c17,
                    ref_4.o_shippriority AS c18,
                    ref_4.o_orderpriority AS c19,
                    ref_3.l_tax AS c20
                FROM
                    main.orders AS ref_5
                WHERE
                    ref_4.o_orderstatus IS NOT NULL
                LIMIT 65))
        AND (EXISTS (
                SELECT
                    ref_3.l_shipdate AS c0,
                    ref_3.l_shipdate AS c1
                FROM
                    main.part AS ref_6
                WHERE
                    ref_6.p_partkey IS NOT NULL))) AS subq_1
    LEFT JOIN main.customer AS ref_7 ON (subq_1.c1 IS NOT NULL) ON (subq_1.c4 IS NULL)
    INNER JOIN main.customer AS ref_8
    LEFT JOIN main.lineitem AS ref_9 ON ((ref_8.c_phone IS NOT NULL)
            AND (ref_9.l_suppkey IS NOT NULL)) ON (ref_7.c_name = ref_8.c_name)
WHERE (1)
AND ((ref_7.c_address IS NULL)
    AND (0))
LIMIT 128
