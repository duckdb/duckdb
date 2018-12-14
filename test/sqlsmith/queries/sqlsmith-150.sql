SELECT
    subq_1.c1 AS c0,
    subq_1.c1 AS c1,
    subq_1.c0 AS c2,
    subq_1.c1 AS c3,
    subq_1.c1 AS c4,
    subq_1.c1 AS c5,
    subq_1.c0 AS c6,
    subq_1.c1 AS c7,
    CAST(coalesce(subq_1.c1, CASE WHEN subq_1.c1 IS NOT NULL THEN
                subq_1.c1
            ELSE
                subq_1.c1
            END) AS DECIMAL) AS c8,
    subq_1.c0 AS c9
FROM (
    SELECT
        subq_0.c0 AS c0,
        subq_0.c5 AS c1
    FROM (
        SELECT
            ref_1.p_size AS c0,
            ref_2.s_suppkey AS c1,
            ref_2.s_address AS c2,
            ref_1.p_retailprice AS c3,
            ref_1.p_container AS c4,
            ref_0.p_retailprice AS c5,
            ref_0.p_name AS c6,
            ref_1.p_container AS c7
        FROM
            main.part AS ref_0
        LEFT JOIN main.part AS ref_1
        INNER JOIN main.supplier AS ref_2 ON (ref_1.p_brand IS NOT NULL) ON (ref_0.p_mfgr = ref_1.p_name)
    WHERE
        ref_2.s_name IS NOT NULL) AS subq_0
WHERE ((((((EXISTS (
                            SELECT
                                subq_0.c6 AS c0, 63 AS c1, ref_3.ps_supplycost AS c2, subq_0.c7 AS c3, ref_3.ps_supplycost AS c4, ref_3.ps_partkey AS c5, (
                                    SELECT
                                        l_orderkey
                                    FROM
                                        main.lineitem
                                    LIMIT 1 offset 4) AS c6,
                                subq_0.c1 AS c7,
                                subq_0.c4 AS c8
                            FROM
                                main.partsupp AS ref_3
                            WHERE
                                0
                            LIMIT 78))
                    OR ((
                            SELECT
                                o_totalprice
                            FROM
                                main.orders
                            LIMIT 1 offset 6)
                        IS NOT NULL))
                AND (1))
            OR (((1)
                    OR ((subq_0.c4 IS NOT NULL)
                        OR (0)))
                AND ((EXISTS (
                            SELECT
                                ref_4.o_orderpriority AS c0,
                                subq_0.c4 AS c1,
                                ref_4.o_totalprice AS c2,
                                subq_0.c7 AS c3,
                                ref_4.o_orderdate AS c4
                            FROM
                                main.orders AS ref_4
                            WHERE
                                7 IS NULL))
                        AND ((((subq_0.c2 IS NOT NULL)
                                    OR ((1)
                                        OR (subq_0.c0 IS NULL)))
                                OR (0))
                            OR ((subq_0.c1 IS NOT NULL)
                                AND (1))))))
            AND (EXISTS (
                    SELECT
                        ref_5.n_name AS c0, ref_5.n_nationkey AS c1, subq_0.c2 AS c2
                    FROM
                        main.nation AS ref_5
                    WHERE (subq_0.c0 IS NULL)
                    OR (((((0)
                                    OR ((ref_5.n_name IS NOT NULL)
                                        OR (EXISTS (
                                                SELECT
                                                    ref_5.n_comment AS c0, ref_6.n_nationkey AS c1, ref_6.n_name AS c2, 72 AS c3, ref_6.n_name AS c4, ref_6.n_name AS c5, ref_6.n_regionkey AS c6, ref_6.n_comment AS c7, ref_6.n_name AS c8, ref_5.n_regionkey AS c9, ref_5.n_regionkey AS c10, ref_6.n_name AS c11, subq_0.c0 AS c12
                                                FROM
                                                    main.nation AS ref_6
                                                WHERE (1)
                                                AND (((
                                                            SELECT
                                                                c_name
                                                            FROM
                                                                main.customer
                                                            LIMIT 1 offset 1)
                                                        IS NULL)
                                                    AND (1))
                                            LIMIT 61))))
                            AND (ref_5.n_name IS NOT NULL))
                        AND (EXISTS (
                                SELECT
                                    subq_0.c4 AS c0,
                                    subq_0.c7 AS c1
                                FROM
                                    main.orders AS ref_7
                                WHERE (
                                    SELECT
                                        l_comment
                                    FROM
                                        main.lineitem
                                    LIMIT 1 offset 5)
                                IS NULL
                            LIMIT 80)))
                AND (EXISTS (
                        SELECT
                            ref_8.s_acctbal AS c0,
                            subq_0.c1 AS c1,
                            ref_8.s_acctbal AS c2,
                            subq_0.c3 AS c3,
                            ref_8.s_comment AS c4
                        FROM
                            main.supplier AS ref_8
                        WHERE (
                            SELECT
                                ps_comment
                            FROM
                                main.partsupp
                            LIMIT 1 offset 6)
                        IS NOT NULL
                    LIMIT 126)))
    LIMIT 107)))
OR (subq_0.c0 IS NULL))
AND ((EXISTS (
            SELECT
                subq_0.c7 AS c0,
                ref_9.o_shippriority AS c1
            FROM
                main.orders AS ref_9
            WHERE (EXISTS (
                    SELECT
                        ref_10.n_nationkey AS c0, subq_0.c7 AS c1, ref_10.n_nationkey AS c2, ref_9.o_orderkey AS c3, ref_9.o_orderkey AS c4, ref_10.n_comment AS c5, ref_10.n_regionkey AS c6, subq_0.c1 AS c7
                    FROM
                        main.nation AS ref_10
                    WHERE (ref_9.o_orderdate IS NULL)
                    OR (0)
                LIMIT 129))
        OR ((ref_9.o_custkey IS NULL)
            OR (subq_0.c5 IS NOT NULL))
    LIMIT 106))
AND (1))) AS subq_1
WHERE
    subq_1.c1 IS NOT NULL
