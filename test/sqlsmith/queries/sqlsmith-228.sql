SELECT
    ref_0.o_shippriority AS c0
FROM
    main.orders AS ref_0
    INNER JOIN main.orders AS ref_1 ON ((1)
            OR (ref_1.o_orderdate IS NOT NULL))
WHERE (((((1)
                AND (ref_1.o_totalprice IS NULL))
            AND ((ref_0.o_orderkey IS NULL)
                AND (0)))
        OR (ref_1.o_custkey IS NOT NULL))
    OR ((0)
        OR ((ref_0.o_totalprice IS NOT NULL)
            OR ((0)
                AND (((ref_1.o_totalprice IS NOT NULL)
                        AND (0))
                    OR (1))))))
OR (EXISTS (
        SELECT
            ref_0.o_clerk AS c0
        FROM
            main.customer AS ref_2
        WHERE ((EXISTS (
                    SELECT
                        ref_2.c_comment AS c0, ref_3.c_comment AS c1, ref_3.c_custkey AS c2, ref_1.o_clerk AS c3
                    FROM
                        main.customer AS ref_3
                    WHERE
                        1
                    LIMIT 44))
            AND (ref_2.c_nationkey IS NOT NULL))
        OR ((ref_1.o_shippriority IS NOT NULL)
            OR (EXISTS (
                    SELECT
                        DISTINCT ref_0.o_orderdate AS c0,
                        ref_1.o_orderdate AS c1,
                        (
                            SELECT
                                o_orderdate
                            FROM
                                main.orders
                            LIMIT 1 offset 2) AS c2,
                        ref_4.r_comment AS c3,
                        ref_1.o_orderkey AS c4,
                        ref_1.o_clerk AS c5,
                        ref_2.c_acctbal AS c6
                    FROM
                        main.region AS ref_4
                    WHERE (1)
                    AND (EXISTS (
                            SELECT
                                ref_1.o_orderpriority AS c0, ref_4.r_name AS c1, ref_2.c_acctbal AS c2, ref_0.o_shippriority AS c3, ref_5.ps_comment AS c4, ref_2.c_name AS c5, (
                                    SELECT
                                        r_regionkey
                                    FROM
                                        main.region
                                    LIMIT 1 offset 2) AS c6,
                                ref_0.o_orderpriority AS c7,
                                ref_0.o_comment AS c8
                            FROM
                                main.partsupp AS ref_5
                            WHERE
                                1
                            LIMIT 172))
                LIMIT 130)))))
