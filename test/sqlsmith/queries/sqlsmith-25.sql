SELECT
    ref_0.c_name AS c0,
    ref_0.c_comment AS c1, (
        SELECT
            c_comment
        FROM
            main.customer
        LIMIT 1 offset 5) AS c2, (
        SELECT
            s_suppkey
        FROM
            main.supplier
        LIMIT 1 offset 3) AS c3,
    ref_0.c_mktsegment AS c4,
    55 AS c5,
    CASE WHEN ((EXISTS (
                    SELECT
                        (
                            SELECT
                                p_brand
                            FROM
                                main.part
                            LIMIT 1 offset 19) AS c0,
                        ref_0.c_acctbal AS c1
                    FROM
                        main.part AS ref_10
                    WHERE
                        EXISTS (
                            SELECT
                                ref_10.p_container AS c0, ref_11.r_regionkey AS c1, ref_11.r_regionkey AS c2, ref_0.c_name AS c3, subq_0.c0 AS c4
                            FROM
                                main.region AS ref_11
                            WHERE
                                1
                            LIMIT 39)))
                AND (EXISTS (
                        SELECT
                            ref_0.c_address AS c0,
                            ref_0.c_comment AS c1,
                            ref_0.c_name AS c2
                        FROM
                            main.orders AS ref_12
                        WHERE
                            EXISTS (
                                SELECT
                                    ref_0.c_phone AS c0
                                FROM
                                    main.customer AS ref_13
                                WHERE
                                    ref_13.c_custkey IS NOT NULL
                                LIMIT 109))))
                AND ((EXISTS (
                            SELECT
                                ref_0.c_comment AS c0,
                                14 AS c1,
                                ref_14.p_type AS c2,
                                subq_0.c0 AS c3,
                                79 AS c4,
                                ref_0.c_nationkey AS c5
                            FROM
                                main.part AS ref_14
                            WHERE (0)
                            AND (ref_14.p_type IS NULL)
                        LIMIT 46))
                OR ((1)
                    AND ((subq_0.c1 IS NULL)
                        OR (((0)
                                OR ((EXISTS (
                                            SELECT
                                                ref_15.l_shipinstruct AS c0,
                                                subq_0.c1 AS c1,
                                                subq_0.c0 AS c2
                                            FROM
                                                main.lineitem AS ref_15
                                            WHERE
                                                1
                                            LIMIT 143))
                                    AND (1)))
                            OR (((
                                        SELECT
                                            s_comment
                                        FROM
                                            main.supplier
                                        LIMIT 1 offset 5)
                                    IS NULL)
                                AND (1)))))) THEN
            subq_0.c1
        ELSE
            subq_0.c1
        END AS c6,
        ref_0.c_acctbal AS c7,
        ref_0.c_address AS c8,
        ref_0.c_mktsegment AS c9,
        subq_0.c0 AS c10,
        ref_0.c_phone AS c11,
        subq_0.c1 AS c12,
        ref_0.c_address AS c13,
        subq_0.c1 AS c14,
        ref_0.c_nationkey AS c15
    FROM
        main.customer AS ref_0
    RIGHT JOIN (
        SELECT
            (
                SELECT
                    o_orderdate
                FROM
                    main.orders
                LIMIT 1 offset 4) AS c0, (
                SELECT
                    s_comment
                FROM
                    main.supplier
                LIMIT 1 offset 4) AS c1
        FROM
            main.lineitem AS ref_1
        LEFT JOIN main.region AS ref_2 ON ((EXISTS (
                    SELECT
                        ref_2.r_name AS c0,
                        ref_2.r_regionkey AS c1,
                        ref_3.p_type AS c2,
                        ref_1.l_suppkey AS c3,
                        ref_2.r_regionkey AS c4
                    FROM
                        main.part AS ref_3
                    WHERE (1)
                    OR (1)
                LIMIT 131))
        AND ((ref_1.l_quantity IS NULL)
            OR (((1)
                    AND ((0)
                        AND (1)))
                OR (1))))
WHERE
    EXISTS (
        SELECT
            ref_4.l_suppkey AS c0, ref_1.l_suppkey AS c1, ref_5.r_name AS c2, 22 AS c3, ref_2.r_regionkey AS c4, ref_5.r_name AS c5, ref_2.r_comment AS c6
        FROM
            main.lineitem AS ref_4
        INNER JOIN main.region AS ref_5 ON (((((0)
                            AND (((35 IS NOT NULL)
                                    AND ((((EXISTS (
                                                        SELECT
                                                            ref_1.l_receiptdate AS c0,
                                                            ref_5.r_name AS c1,
                                                            ref_6.p_comment AS c2,
                                                            ref_1.l_orderkey AS c3
                                                        FROM
                                                            main.part AS ref_6
                                                        WHERE
                                                            ref_2.r_comment IS NOT NULL
                                                        LIMIT 54))
                                                OR (1))
                                            AND (0))
                                        AND (ref_4.l_returnflag IS NOT NULL)))
                                AND ((ref_4.l_discount IS NULL)
                                    OR (ref_1.l_receiptdate IS NOT NULL))))
                        OR (1))
                    OR ((EXISTS (
                                SELECT
                                    ref_4.l_partkey AS c0
                                FROM
                                    main.customer AS ref_7
                                WHERE (EXISTS (
                                        SELECT
                                            (
                                                SELECT
                                                    ps_availqty
                                                FROM
                                                    main.partsupp
                                                LIMIT 1 offset 6) AS c0,
                                            ref_4.l_comment AS c1, (
                                                SELECT
                                                    r_regionkey
                                                FROM
                                                    main.region
                                                LIMIT 1 offset 1) AS c2,
                                            ref_4.l_quantity AS c3
                                        FROM
                                            main.partsupp AS ref_8
                                        WHERE
                                            1))
                                    OR (ref_4.l_commitdate IS NULL)
                                LIMIT 102))
                        OR (1)))
                OR ((1)
                    OR ((1)
                        AND ((1)
                            AND (EXISTS (
                                    SELECT
                                        ref_5.r_comment AS c0, (
                                            SELECT
                                                c_mktsegment
                                            FROM
                                                main.customer
                                            LIMIT 1 offset 2) AS c1,
                                        25 AS c2,
                                        ref_4.l_shipmode AS c3,
                                        ref_2.r_name AS c4,
                                        ref_2.r_regionkey AS c5
                                    FROM
                                        main.supplier AS ref_9
                                    WHERE 0
                                LIMIT 85))))))
    WHERE (ref_4.l_suppkey IS NULL)
    AND (1)
LIMIT 137)) AS subq_0 ON (((0)
        AND (subq_0.c0 IS NULL))
    AND (ref_0.c_address IS NOT NULL))
WHERE
    ref_0.c_phone IS NOT NULL
