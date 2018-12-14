SELECT
    subq_1.c0 AS c0,
    CASE WHEN (((EXISTS (
                        SELECT
                            subq_1.c3 AS c0,
                            subq_1.c2 AS c1,
                            subq_1.c3 AS c2,
                            ref_8.l_linenumber AS c3,
                            ref_8.l_linestatus AS c4,
                            ref_8.l_extendedprice AS c5,
                            ref_8.l_linestatus AS c6,
                            ref_8.l_shipinstruct AS c7,
                            subq_1.c8 AS c8,
                            (
                                SELECT
                                    p_name
                                FROM
                                    main.part
                                LIMIT 1 offset 4) AS c9,
                            subq_1.c9 AS c10,
                            ref_8.l_shipmode AS c11,
                            subq_1.c4 AS c12,
                            ref_8.l_shipmode AS c13,
                            61 AS c14,
                            subq_1.c6 AS c15,
                            subq_1.c3 AS c16,
                            subq_1.c4 AS c17,
                            ref_8.l_orderkey AS c18,
                            ref_8.l_quantity AS c19,
                            subq_1.c0 AS c20,
                            subq_1.c1 AS c21
                        FROM
                            main.lineitem AS ref_8
                        WHERE (0)
                        AND (1)))
                AND (((subq_1.c3 IS NOT NULL)
                        AND (0))
                    OR ((subq_1.c0 IS NOT NULL)
                        AND ((((0)
                                    OR ((((1)
                                                OR ((EXISTS (
                                                            SELECT
                                                                ref_9.l_receiptdate AS c0, subq_1.c0 AS c1, subq_1.c2 AS c2, subq_1.c5 AS c3, ref_9.l_orderkey AS c4, ref_9.l_quantity AS c5, ref_9.l_extendedprice AS c6, subq_1.c10 AS c7, subq_1.c8 AS c8, subq_1.c7 AS c9
                                                            FROM
                                                                main.lineitem AS ref_9
                                                            WHERE ((EXISTS (
                                                                        SELECT
                                                                            ref_9.l_extendedprice AS c0
                                                                        FROM
                                                                            main.customer AS ref_10
                                                                        WHERE (((ref_10.c_acctbal IS NULL)
                                                                                OR ((0)
                                                                                    OR (ref_9.l_partkey IS NULL)))
                                                                            OR ((0)
                                                                                OR (1)))
                                                                        OR (subq_1.c7 IS NOT NULL)
                                                                    LIMIT 81))
                                                            OR (0))
                                                        AND ((0)
                                                            AND (EXISTS (
                                                                    SELECT
                                                                        ref_11.r_name AS c0
                                                                    FROM
                                                                        main.region AS ref_11
                                                                    WHERE (ref_11.r_name IS NOT NULL)
                                                                    AND (0))))))
                                                OR ((0)
                                                    OR (1))))
                                        AND ((subq_1.c6 IS NOT NULL)
                                            AND (0)))
                                    OR (subq_1.c3 IS NULL)))
                            OR (0))
                        OR (subq_1.c1 IS NULL)))))
        OR ((subq_1.c2 IS NULL)
            AND (((0)
                    AND ((EXISTS (
                                SELECT
                                    ref_12.n_regionkey AS c0, 75 AS c1
                                FROM
                                    main.nation AS ref_12
                                WHERE
                                    0
                                LIMIT 104))
                        AND (((EXISTS (
                                        SELECT
                                            subq_1.c10 AS c0
                                        FROM
                                            main.customer AS ref_13
                                        WHERE
                                            1
                                        LIMIT 40))
                                OR (1))
                            AND (((1)
                                    OR ((1)
                                        AND ((subq_1.c5 IS NOT NULL)
                                            AND (((1)
                                                    OR (1))
                                                AND (((1)
                                                        OR (1))
                                                    OR (1))))))
                                AND ((
                                        SELECT
                                            s_comment
                                        FROM
                                            main.supplier
                                        LIMIT 1 offset 1)
                                    IS NULL)))))
                OR (1))))
    AND (0) THEN
    subq_1.c4
ELSE
    subq_1.c4
END AS c1,
subq_1.c8 AS c2,
subq_1.c9 AS c3,
subq_1.c0 AS c4,
subq_1.c10 AS c5,
CASE WHEN (subq_1.c0 IS NOT NULL)
    AND (subq_1.c7 IS NULL) THEN
    subq_1.c9
ELSE
    subq_1.c9
END AS c6,
subq_1.c7 AS c7,
4 AS c8,
subq_1.c9 AS c9
FROM (
    SELECT
        ref_2.c_nationkey AS c0,
        33 AS c1,
        ref_2.c_acctbal AS c2,
        ref_2.c_name AS c3,
        subq_0.c8 AS c4,
        subq_0.c7 AS c5,
        ref_1.n_nationkey AS c6,
        subq_0.c9 AS c7,
        ref_1.n_nationkey AS c8,
        subq_0.c8 AS c9,
        ref_1.n_name AS c10
    FROM (
        SELECT
            ref_0.n_regionkey AS c0,
            ref_0.n_comment AS c1,
            ref_0.n_comment AS c2,
            ref_0.n_name AS c3,
            ref_0.n_comment AS c4,
            ref_0.n_comment AS c5,
            ref_0.n_comment AS c6,
            ref_0.n_name AS c7,
            71 AS c8,
            ref_0.n_comment AS c9
        FROM
            main.nation AS ref_0
        WHERE ((ref_0.n_comment IS NOT NULL)
            OR ((0)
                AND (1)))
        AND (ref_0.n_comment IS NULL)
    LIMIT 143) AS subq_0
    INNER JOIN main.nation AS ref_1
    INNER JOIN main.customer AS ref_2 ON ((1)
            OR (1)) ON (subq_0.c2 = ref_1.n_name)
WHERE
    EXISTS (
        SELECT
            69 AS c0, subq_0.c3 AS c1, ref_2.c_comment AS c2, ref_1.n_regionkey AS c3, ref_3.c_mktsegment AS c4, subq_0.c7 AS c5
        FROM
            main.customer AS ref_3
        WHERE (1)
        OR ((((((ref_1.n_regionkey IS NOT NULL)
                            AND ((0)
                                AND ((EXISTS (
                                            SELECT
                                                ref_1.n_name AS c0, subq_0.c2 AS c1
                                            FROM
                                                main.orders AS ref_4
                                            WHERE (0)
                                            OR (EXISTS (
                                                    SELECT
                                                        subq_0.c7 AS c0, ref_4.o_orderdate AS c1, ref_4.o_orderstatus AS c2, ref_2.c_name AS c3, (
                                                            SELECT
                                                                ps_partkey
                                                            FROM
                                                                main.partsupp
                                                            LIMIT 1 offset 89) AS c4,
                                                        ref_3.c_custkey AS c5,
                                                        ref_5.c_address AS c6,
                                                        ref_2.c_phone AS c7,
                                                        (
                                                            SELECT
                                                                r_name
                                                            FROM
                                                                main.region
                                                            LIMIT 1 offset 1) AS c8,
                                                        ref_4.o_orderdate AS c9
                                                    FROM
                                                        main.customer AS ref_5
                                                    WHERE
                                                        subq_0.c4 IS NULL
                                                    LIMIT 92))
                                        LIMIT 35))
                                AND ((0)
                                    AND (EXISTS (
                                            SELECT
                                                ref_2.c_mktsegment AS c0,
                                                ref_1.n_regionkey AS c1,
                                                subq_0.c5 AS c2,
                                                (
                                                    SELECT
                                                        s_phone
                                                    FROM
                                                        main.supplier
                                                    LIMIT 1 offset 1) AS c3
                                            FROM
                                                main.partsupp AS ref_6
                                            WHERE
                                                0
                                            LIMIT 29))))))
                    AND (ref_2.c_phone IS NOT NULL))
                OR (67 IS NULL))
            AND (subq_0.c1 IS NOT NULL))
        OR (EXISTS (
                SELECT
                    ref_2.c_nationkey AS c0,
                    (
                        SELECT
                            l_receiptdate
                        FROM
                            main.lineitem
                        LIMIT 1 offset 5) AS c1,
                    (
                        SELECT
                            l_extendedprice
                        FROM
                            main.lineitem
                        LIMIT 1 offset 68) AS c2,
                    ref_7.p_mfgr AS c3,
                    57 AS c4,
                    subq_0.c9 AS c5,
                    ref_7.p_retailprice AS c6,
                    ref_7.p_name AS c7
                FROM
                    main.part AS ref_7
                WHERE
                    1
                LIMIT 87)))
LIMIT 71)) AS subq_1
WHERE (1)
AND (subq_1.c7 IS NULL)
LIMIT 134
