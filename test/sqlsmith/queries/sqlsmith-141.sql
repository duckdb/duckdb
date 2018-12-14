SELECT
    subq_0.c7 AS c0,
    ref_8.o_orderdate AS c1
FROM (
    SELECT
        ref_0.r_name AS c0,
        ref_1.s_nationkey AS c1,
        ref_1.s_suppkey AS c2,
        28 AS c3,
        ref_2.s_address AS c4,
        CASE WHEN ((0)
                OR (((0)
                        OR ((ref_1.s_name IS NULL)
                            AND (((1)
                                    OR (ref_0.r_regionkey IS NULL))
                                AND (1))))
                    AND ((ref_0.r_regionkey IS NOT NULL)
                        OR (ref_2.s_nationkey IS NULL))))
            AND (((((0)
                            OR ((1)
                                OR ((((1)
                                            OR (EXISTS (
                                                    SELECT
                                                        ref_3.s_nationkey AS c0,
                                                        ref_0.r_comment AS c1,
                                                        ref_1.s_acctbal AS c2,
                                                        ref_3.s_nationkey AS c3,
                                                        ref_1.s_comment AS c4,
                                                        72 AS c5,
                                                        ref_3.s_nationkey AS c6
                                                    FROM
                                                        main.supplier AS ref_3
                                                    WHERE
                                                        EXISTS (
                                                            SELECT
                                                                ref_4.n_comment AS c0, ref_2.s_nationkey AS c1, ref_4.n_regionkey AS c2, ref_1.s_suppkey AS c3
                                                            FROM
                                                                main.nation AS ref_4
                                                            WHERE
                                                                0)
                                                        LIMIT 86)))
                                            AND (0))
                                        AND (((ref_2.s_comment IS NOT NULL)
                                                AND (ref_0.r_name IS NULL))
                                            OR (1)))))
                            AND ((0)
                                AND (ref_1.s_phone IS NOT NULL)))
                        AND (EXISTS (
                                SELECT
                                    ref_0.r_name AS c0,
                                    ref_1.s_phone AS c1,
                                    ref_2.s_suppkey AS c2,
                                    ref_0.r_name AS c3,
                                    ref_2.s_acctbal AS c4,
                                    ref_2.s_suppkey AS c5,
                                    ref_5.p_partkey AS c6
                                FROM
                                    main.part AS ref_5
                                WHERE
                                    1
                                LIMIT 101)))
                    OR (ref_2.s_phone IS NOT NULL)) THEN
                ref_1.s_acctbal
            ELSE
                ref_1.s_acctbal
            END AS c5,
            ref_1.s_address AS c6,
            CASE WHEN (ref_0.r_comment IS NULL)
                AND (((0)
                        OR ((ref_1.s_acctbal IS NOT NULL)
                            OR ((ref_1.s_address IS NOT NULL)
                                AND ((0)
                                    AND (EXISTS (
                                            SELECT
                                                ref_1.s_suppkey AS c0,
                                                ref_2.s_nationkey AS c1,
                                                ref_0.r_regionkey AS c2,
                                                ref_1.s_acctbal AS c3,
                                                ref_6.r_name AS c4,
                                                ref_2.s_phone AS c5,
                                                ref_2.s_acctbal AS c6,
                                                ref_6.r_comment AS c7
                                            FROM
                                                main.region AS ref_6
                                            WHERE (34 IS NOT NULL)
                                            OR (ref_0.r_comment IS NULL)
                                        LIMIT 72))))))
                OR (1)) THEN
            ref_2.s_name
        ELSE
            ref_2.s_name
        END AS c7,
        45 AS c8
    FROM
        main.region AS ref_0
    LEFT JOIN main.supplier AS ref_1
    INNER JOIN main.supplier AS ref_2 ON (ref_1.s_phone IS NULL) ON (ref_0.r_comment = ref_1.s_name)
WHERE (1)
AND (((((1)
                OR (((0)
                        AND (0))
                    AND (1)))
            OR ((ref_1.s_comment IS NOT NULL)
                AND (ref_0.r_comment IS NULL)))
        AND (EXISTS (
                SELECT
                    ref_0.r_comment AS c0, ref_2.s_phone AS c1, ref_7.p_mfgr AS c2, ref_2.s_phone AS c3, ref_7.p_retailprice AS c4, ref_7.p_name AS c5, ref_7.p_size AS c6, ref_7.p_mfgr AS c7, ref_0.r_name AS c8, ref_0.r_regionkey AS c9, ref_0.r_comment AS c10, ref_0.r_name AS c11, ref_7.p_comment AS c12, ref_1.s_comment AS c13, ref_2.s_acctbal AS c14, ref_1.s_address AS c15, ref_0.r_comment AS c16
                FROM
                    main.part AS ref_7
                WHERE
                    ref_0.r_comment IS NULL)))
        OR (((ref_0.r_comment IS NULL)
                AND ((ref_1.s_name IS NULL)
                    OR (1)))
            OR (1)))
LIMIT 125) AS subq_0
    INNER JOIN main.orders AS ref_8 ON (subq_0.c8 = ref_8.o_orderkey)
WHERE
    subq_0.c5 IS NULL
LIMIT 112
