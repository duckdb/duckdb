SELECT
    subq_1.c0 AS c0,
    subq_1.c1 AS c1,
    subq_1.c1 AS c2,
    subq_1.c1 AS c3,
    subq_1.c1 AS c4,
    subq_1.c1 AS c5
FROM (
    SELECT
        DISTINCT ref_0.c_nationkey AS c0,
        ref_0.c_address AS c1
    FROM
        main.customer AS ref_0
    LEFT JOIN (
        SELECT
            ref_1.r_name AS c0,
            ref_1.r_comment AS c1,
            ref_1.r_name AS c2,
            ref_1.r_regionkey AS c3,
            ref_1.r_comment AS c4,
            ref_1.r_name AS c5,
            ref_1.r_comment AS c6,
            ref_1.r_comment AS c7,
            ref_1.r_name AS c8, (
                SELECT
                    n_name
                FROM
                    main.nation
                LIMIT 1 offset 26) AS c9,
            ref_1.r_name AS c10,
            ref_1.r_regionkey AS c11,
            ref_1.r_name AS c12,
            ref_1.r_comment AS c13,
            ref_1.r_comment AS c14
        FROM
            main.region AS ref_1
        WHERE
            1
        LIMIT 142) AS subq_0 ON ((ref_0.c_custkey IS NOT NULL)
        OR ((ref_0.c_comment IS NULL)
            AND ((((1)
                        OR (0))
                    AND (ref_0.c_name IS NULL))
                OR (1))))
WHERE
    ref_0.c_address IS NOT NULL) AS subq_1
WHERE ((((0)
            AND (EXISTS (
                    SELECT
                        ref_2.o_orderpriority AS c0, ref_2.o_orderkey AS c1
                    FROM
                        main.orders AS ref_2
                    WHERE ((0)
                        OR (0))
                    OR (subq_1.c1 IS NULL))))
        OR ((EXISTS (
                    SELECT
                        subq_1.c0 AS c0
                    FROM
                        main.lineitem AS ref_3
                    WHERE (EXISTS (
                            SELECT
                                ref_4.o_orderdate AS c0, ref_4.o_orderpriority AS c1, ref_3.l_suppkey AS c2, subq_1.c1 AS c3, ref_4.o_shippriority AS c4, ref_3.l_comment AS c5, 66 AS c6, ref_3.l_receiptdate AS c7
                            FROM
                                main.orders AS ref_4
                            WHERE ((((1)
                                        OR ((0)
                                            OR (1)))
                                    OR (subq_1.c1 IS NOT NULL))
                                OR ((ref_3.l_shipdate IS NOT NULL)
                                    AND (EXISTS (
                                            SELECT
                                                ref_3.l_linestatus AS c0
                                            FROM
                                                main.part AS ref_5
                                            WHERE
                                                ref_4.o_shippriority IS NULL
                                            LIMIT 98))))
                            OR ((subq_1.c0 IS NULL)
                                AND (((1)
                                        AND (((EXISTS (
                                                        SELECT
                                                            ref_6.l_quantity AS c0, ref_4.o_orderdate AS c1, ref_3.l_shipinstruct AS c2, ref_4.o_orderkey AS c3, ref_3.l_commitdate AS c4, subq_1.c1 AS c5
                                                        FROM
                                                            main.lineitem AS ref_6
                                                        WHERE 0
                                                    LIMIT 121))
                                            AND ((0)
                                                OR (0)))
                                        AND (1)))
                                AND ((ref_3.l_shipinstruct IS NOT NULL)
                                    OR (ref_4.o_orderkey IS NULL))))))
                OR (ref_3.l_linenumber IS NOT NULL)))
        OR (subq_1.c1 IS NOT NULL)))
OR ((subq_1.c0 IS NULL)
    OR (subq_1.c1 IS NOT NULL)))
AND (((EXISTS (
                SELECT
                    ref_7.l_quantity AS c0,
                    36 AS c1,
                    ref_7.l_partkey AS c2
                FROM
                    main.lineitem AS ref_7
                WHERE
                    EXISTS (
                        SELECT
                            3 AS c0
                        FROM
                            main.region AS ref_8
                        WHERE (ref_8.r_regionkey IS NULL)
                        OR (0))
                LIMIT 105))
        OR ((subq_1.c0 IS NULL)
            OR ((1)
                OR ((89 IS NULL)
                    OR (0)))))
    OR ((((((1)
                        OR ((subq_1.c0 IS NOT NULL)
                            AND (1)))
                    OR ((1)
                        AND (((subq_1.c1 IS NOT NULL)
                                OR (subq_1.c0 IS NULL))
                            AND (subq_1.c0 IS NULL))))
                OR (18 IS NOT NULL))
            OR (((EXISTS (
                            SELECT
                                subq_1.c0 AS c0,
                                ref_9.o_orderstatus AS c1,
                                ref_9.o_orderpriority AS c2, (
                                    SELECT
                                        c_mktsegment
                                    FROM
                                        main.customer
                                    LIMIT 1 offset 90) AS c3,
                                ref_9.o_orderdate AS c4,
                                subq_1.c1 AS c5
                            FROM
                                main.orders AS ref_9
                            WHERE
                                1
                            LIMIT 90))
                    OR (subq_1.c0 IS NOT NULL))
                OR (subq_1.c0 IS NULL)))
        OR ((1)
            OR (subq_1.c0 IS NULL))))
LIMIT 186
