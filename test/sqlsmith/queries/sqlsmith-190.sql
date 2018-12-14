SELECT
    subq_0.c2 AS c0,
    subq_0.c2 AS c1,
    subq_1.c4 AS c2,
    CASE WHEN ((subq_1.c0 IS NOT NULL)
            AND ((((EXISTS (
                                SELECT
                                    subq_1.c6 AS c0,
                                    subq_0.c2 AS c1,
                                    subq_1.c3 AS c2,
                                    ref_4.s_name AS c3,
                                    subq_0.c1 AS c4,
                                    subq_1.c5 AS c5,
                                    ref_4.s_suppkey AS c6,
                                    subq_1.c2 AS c7,
                                    15 AS c8,
                                    ref_4.s_phone AS c9
                                FROM
                                    main.supplier AS ref_4
                                WHERE (0)
                                AND ((0)
                                    AND (0))
                            LIMIT 143))
                    OR (EXISTS (
                            SELECT
                                subq_1.c3 AS c0,
                                subq_1.c0 AS c1
                            FROM
                                main.part AS ref_5
                            WHERE (subq_1.c4 IS NULL)
                            OR (ref_5.p_brand IS NOT NULL))))
                AND ((0)
                    AND ((0)
                        AND (((EXISTS (
                                        SELECT
                                            subq_0.c0 AS c0, subq_1.c5 AS c1, subq_0.c1 AS c2, subq_0.c0 AS c3, 30 AS c4, (
                                                SELECT
                                                    l_partkey
                                                FROM
                                                    main.lineitem
                                                LIMIT 1 offset 6) AS c5,
                                            subq_1.c0 AS c6,
                                            ref_6.p_mfgr AS c7,
                                            subq_0.c2 AS c8,
                                            ref_6.p_container AS c9,
                                            ref_6.p_comment AS c10,
                                            subq_0.c2 AS c11,
                                            subq_1.c2 AS c12,
                                            subq_1.c5 AS c13,
                                            ref_6.p_partkey AS c14,
                                            (
                                                SELECT
                                                    o_orderkey
                                                FROM
                                                    main.orders
                                                LIMIT 1 offset 5) AS c15,
                                            subq_0.c1 AS c16,
                                            subq_1.c1 AS c17,
                                            subq_0.c2 AS c18,
                                            subq_0.c2 AS c19,
                                            subq_0.c2 AS c20,
                                            ref_6.p_name AS c21,
                                            ref_6.p_retailprice AS c22,
                                            ref_6.p_comment AS c23,
                                            subq_1.c3 AS c24,
                                            subq_0.c1 AS c25,
                                            (
                                                SELECT
                                                    p_container
                                                FROM
                                                    main.part
                                                LIMIT 1 offset 5) AS c26,
                                            subq_0.c0 AS c27,
                                            subq_1.c6 AS c28,
                                            subq_1.c1 AS c29
                                        FROM
                                            main.part AS ref_6
                                        WHERE (((0)
                                                AND ((((0)
                                                            OR (0))
                                                        OR (EXISTS (
                                                                SELECT
                                                                    subq_1.c1 AS c0, subq_0.c2 AS c1, subq_0.c0 AS c2, subq_1.c4 AS c3, subq_0.c1 AS c4, subq_1.c4 AS c5, subq_0.c2 AS c6
                                                                FROM
                                                                    main.nation AS ref_7
                                                                WHERE
                                                                    subq_0.c0 IS NOT NULL
                                                                LIMIT 179)))
                                                    OR ((EXISTS (
                                                                SELECT
                                                                    ref_8.n_nationkey AS c0,
                                                                    subq_0.c2 AS c1,
                                                                    subq_0.c0 AS c2,
                                                                    subq_1.c4 AS c3,
                                                                    ref_6.p_name AS c4,
                                                                    92 AS c5,
                                                                    ref_8.n_comment AS c6,
                                                                    subq_1.c4 AS c7,
                                                                    subq_0.c2 AS c8,
                                                                    subq_1.c4 AS c9,
                                                                    subq_0.c0 AS c10,
                                                                    subq_1.c2 AS c11,
                                                                    subq_0.c1 AS c12,
                                                                    subq_0.c1 AS c13
                                                                FROM
                                                                    main.nation AS ref_8
                                                                WHERE
                                                                    1))
                                                            AND (1))))
                                                AND ((1)
                                                    OR (ref_6.p_comment IS NULL)))
                                            OR ((1)
                                                AND (subq_0.c0 IS NULL))
                                        LIMIT 99))
                                AND (EXISTS (
                                        SELECT
                                            23 AS c0
                                        FROM
                                            main.part AS ref_9
                                        WHERE
                                            0
                                        LIMIT 103)))
                            OR (EXISTS (
                                    SELECT
                                        subq_0.c2 AS c0,
                                        subq_0.c0 AS c1,
                                        subq_1.c6 AS c2
                                    FROM
                                        main.region AS ref_10
                                    WHERE (0)
                                    AND ((((EXISTS (
                                                        SELECT
                                                            (
                                                                SELECT
                                                                    p_name
                                                                FROM
                                                                    main.part
                                                                LIMIT 1 offset 70) AS c0
                                                        FROM
                                                            main.orders AS ref_11
                                                        WHERE (EXISTS (
                                                                SELECT
                                                                    subq_0.c1 AS c0, ref_10.r_comment AS c1, ref_11.o_orderkey AS c2, ref_10.r_regionkey AS c3, (
                                                                        SELECT
                                                                            c_phone
                                                                        FROM
                                                                            main.customer
                                                                        LIMIT 1 offset 5) AS c4,
                                                                    ref_10.r_name AS c5,
                                                                    ref_11.o_orderkey AS c6,
                                                                    subq_0.c0 AS c7,
                                                                    (
                                                                        SELECT
                                                                            l_shipinstruct
                                                                        FROM
                                                                            main.lineitem
                                                                        LIMIT 1 offset 6) AS c8,
                                                                    ref_12.r_name AS c9,
                                                                    subq_0.c2 AS c10,
                                                                    ref_12.r_regionkey AS c11,
                                                                    ref_10.r_name AS c12,
                                                                    ref_10.r_name AS c13,
                                                                    ref_12.r_regionkey AS c14,
                                                                    83 AS c15,
                                                                    ref_11.o_orderkey AS c16
                                                                FROM
                                                                    main.region AS ref_12
                                                                WHERE (1)
                                                                OR (1)
                                                            LIMIT 48))
                                                    AND (0)))
                                            OR ((0)
                                                AND ((subq_1.c1 IS NOT NULL)
                                                    AND (0))))
                                        OR (0))
                                    AND (EXISTS (
                                            SELECT
                                                subq_1.c4 AS c0,
                                                subq_0.c1 AS c1,
                                                subq_1.c0 AS c2
                                            FROM
                                                main.region AS ref_13
                                            WHERE (1)
                                            OR ((0)
                                                AND (0)))))))))))
        OR ((EXISTS (
                    SELECT
                        subq_1.c5 AS c0, subq_0.c1 AS c1, ref_14.c_mktsegment AS c2, subq_1.c6 AS c3, ref_14.c_acctbal AS c4, subq_0.c1 AS c5, subq_0.c1 AS c6
                    FROM
                        main.customer AS ref_14
                    WHERE (0)
                    AND ((1)
                        OR (subq_0.c0 IS NOT NULL))))
            OR (1))))
AND (subq_1.c0 IS NOT NULL) THEN
subq_1.c1
ELSE
    subq_1.c1
END AS c3, subq_1.c3 AS c4, subq_0.c1 AS c5, subq_0.c0 AS c6, subq_1.c3 AS c7, subq_0.c2 AS c8, subq_1.c4 AS c9
FROM (
    SELECT
        ref_1.p_container AS c0,
        ref_1.p_mfgr AS c1,
        ref_1.p_name AS c2
    FROM
        main.lineitem AS ref_0
        INNER JOIN main.part AS ref_1
        INNER JOIN main.orders AS ref_2 ON (ref_2.o_comment IS NULL) ON (ref_0.l_linestatus = ref_2.o_orderstatus)
    WHERE
        ref_0.l_linenumber IS NOT NULL
    LIMIT 186) AS subq_0
    LEFT JOIN (
        SELECT
            ref_3.ps_suppkey AS c0,
            (
                SELECT
                    r_regionkey
                FROM
                    main.region
                LIMIT 1 offset 6) AS c1,
            ref_3.ps_comment AS c2,
            ref_3.ps_partkey AS c3,
            ref_3.ps_availqty AS c4,
            (
                SELECT
                    n_comment
                FROM
                    main.nation
                LIMIT 1 offset 2) AS c5,
            ref_3.ps_comment AS c6
        FROM
            main.partsupp AS ref_3
        WHERE
            ref_3.ps_partkey IS NOT NULL
        LIMIT 146) AS subq_1 ON (subq_1.c4 IS NULL)
WHERE
    0
