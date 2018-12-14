SELECT
    CASE WHEN ((0)
            AND (((((0)
                            AND (ref_0.r_comment IS NULL))
                        OR ((EXISTS (
                                    SELECT
                                        ref_1.s_address AS c0,
                                        ref_0.r_comment AS c1,
                                        ref_1.s_name AS c2,
                                        ref_1.s_acctbal AS c3,
                                        ref_1.s_name AS c4,
                                        ref_1.s_comment AS c5,
                                        ref_1.s_comment AS c6,
                                        ref_0.r_name AS c7,
                                        ref_0.r_comment AS c8,
                                        ref_0.r_name AS c9
                                    FROM
                                        main.supplier AS ref_1
                                    WHERE ((1)
                                        AND ((1)
                                            AND (((1)
                                                    AND (EXISTS (
                                                            SELECT
                                                                ref_0.r_comment AS c0, ref_0.r_name AS c1, ref_1.s_comment AS c2, ref_2.r_name AS c3, ref_2.r_name AS c4, ref_1.s_phone AS c5, ref_2.r_name AS c6, ref_0.r_regionkey AS c7, ref_0.r_comment AS c8, ref_2.r_name AS c9, ref_1.s_suppkey AS c10, ref_1.s_address AS c11
                                                            FROM
                                                                main.region AS ref_2
                                                            WHERE (0)
                                                            OR ((1)
                                                                OR ((ref_0.r_regionkey IS NOT NULL)
                                                                    OR (1)))
                                                        LIMIT 66)))
                                            OR (0))))
                                AND ((EXISTS (
                                            SELECT
                                                ref_0.r_comment AS c0,
                                                ref_0.r_regionkey AS c1,
                                                ref_1.s_suppkey AS c2,
                                                ref_3.s_name AS c3,
                                                ref_0.r_regionkey AS c4,
                                                ref_1.s_nationkey AS c5,
                                                ref_0.r_comment AS c6,
                                                ref_0.r_comment AS c7,
                                                ref_1.s_comment AS c8,
                                                ref_1.s_suppkey AS c9,
                                                ref_0.r_regionkey AS c10
                                            FROM
                                                main.supplier AS ref_3
                                            WHERE
                                                ref_3.s_acctbal IS NOT NULL))
                                        OR (ref_1.s_nationkey IS NULL))
                                LIMIT 78))
                        OR ((0)
                            OR ((0)
                                AND ((1)
                                    OR ((((ref_0.r_name IS NULL)
                                                AND (1))
                                            OR (0))
                                        AND (ref_0.r_comment IS NULL)))))))
                OR (ref_0.r_name IS NOT NULL))
            AND (ref_0.r_name IS NULL)))
    OR (ref_0.r_comment IS NOT NULL) THEN
    CASE WHEN 1 THEN
        29
    ELSE
        29
    END
ELSE
    CASE WHEN 1 THEN
        29
    ELSE
        29
    END
END AS c0,
CASE WHEN ((ref_0.r_regionkey IS NULL)
        OR ((1)
            AND (EXISTS (
                    SELECT
                        ref_4.r_name AS c0,
                        ref_4.r_regionkey AS c1
                    FROM
                        main.region AS ref_4
                    WHERE
                        0
                    LIMIT 55))))
    OR ((((0)
                OR (EXISTS (
                        SELECT
                            ref_0.r_regionkey AS c0,
                            10 AS c1,
                            ref_0.r_name AS c2,
                            ref_0.r_name AS c3,
                            ref_0.r_name AS c4,
                            ref_5.l_linestatus AS c5,
                            ref_0.r_name AS c6
                        FROM
                            main.lineitem AS ref_5
                        WHERE ((0)
                            AND ((1)
                                OR ((ref_5.l_partkey IS NOT NULL)
                                    OR (1))))
                        OR (ref_0.r_comment IS NOT NULL))))
            OR ((((1)
                        AND (0))
                    OR ((0)
                        OR (ref_0.r_comment IS NULL)))
                OR ((0)
                    OR (1))))
        OR ((((0)
                    AND ((ref_0.r_comment IS NOT NULL)
                        AND (EXISTS (
                                SELECT
                                    ref_6.r_regionkey AS c0, ref_0.r_comment AS c1, ref_0.r_regionkey AS c2
                                FROM
                                    main.region AS ref_6
                                WHERE
                                    ref_6.r_comment IS NOT NULL
                                LIMIT 131))))
                AND ((1)
                    OR (EXISTS (
                            SELECT
                                ref_0.r_name AS c0,
                                56 AS c1,
                                ref_0.r_regionkey AS c2,
                                77 AS c3,
                                ref_7.c_custkey AS c4,
                                ref_0.r_comment AS c5,
                                ref_0.r_comment AS c6,
                                ref_0.r_name AS c7,
                                ref_7.c_name AS c8
                            FROM
                                main.customer AS ref_7
                            WHERE (1)
                            AND (ref_7.c_address IS NULL)
                        LIMIT 70))))
        AND ((ref_0.r_regionkey IS NOT NULL)
            AND (EXISTS (
                    SELECT
                        ref_8.s_acctbal AS c0,
                        ref_0.r_comment AS c1,
                        ref_8.s_name AS c2,
                        ref_0.r_name AS c3,
                        ref_8.s_name AS c4
                    FROM
                        main.supplier AS ref_8
                    WHERE (ref_0.r_regionkey IS NULL)
                    AND (0)
                LIMIT 158))))) THEN
CASE WHEN 1 THEN
    ref_0.r_comment
ELSE
    ref_0.r_comment
END
ELSE
    CASE WHEN 1 THEN
        ref_0.r_comment
    ELSE
        ref_0.r_comment
    END
END AS c1,
ref_0.r_comment AS c2,
ref_0.r_name AS c3,
ref_0.r_comment AS c4,
ref_0.r_name AS c5,
ref_0.r_name AS c6,
ref_0.r_comment AS c7,
CASE WHEN ((((EXISTS (
                        SELECT
                            ref_0.r_comment AS c0,
                            ref_0.r_name AS c1,
                            ref_9.l_linestatus AS c2,
                            ref_0.r_comment AS c3,
                            ref_9.l_quantity AS c4,
                            ref_9.l_partkey AS c5,
                            ref_0.r_name AS c6,
                            ref_0.r_name AS c7,
                            ref_9.l_quantity AS c8,
                            ref_9.l_partkey AS c9,
                            ref_0.r_name AS c10,
                            ref_9.l_discount AS c11
                        FROM
                            main.lineitem AS ref_9
                        WHERE
                            EXISTS (
                                SELECT
                                    ref_10.p_mfgr AS c0, ref_9.l_discount AS c1
                                FROM
                                    main.part AS ref_10
                                WHERE
                                    EXISTS (
                                        SELECT
                                            ref_11.s_address AS c0, ref_10.p_comment AS c1, ref_9.l_suppkey AS c2, ref_9.l_commitdate AS c3, ref_9.l_returnflag AS c4, ref_0.r_comment AS c5, ref_9.l_returnflag AS c6
                                        FROM
                                            main.supplier AS ref_11
                                        WHERE
                                            ref_11.s_suppkey IS NOT NULL
                                        LIMIT 38))
                            LIMIT 108))
                    AND (((ref_0.r_comment IS NOT NULL)
                            OR (ref_0.r_name IS NOT NULL))
                        AND (((EXISTS (
                                        SELECT
                                            ref_12.c_nationkey AS c0,
                                            ref_0.r_name AS c1,
                                            ref_12.c_acctbal AS c2,
                                            ref_12.c_mktsegment AS c3,
                                            44 AS c4,
                                            ref_12.c_mktsegment AS c5,
                                            ref_12.c_nationkey AS c6,
                                            ref_12.c_address AS c7,
                                            ref_0.r_name AS c8,
                                            ref_12.c_acctbal AS c9,
                                            ref_12.c_address AS c10,
                                            ref_12.c_acctbal AS c11,
                                            ref_12.c_phone AS c12,
                                            ref_12.c_comment AS c13
                                        FROM
                                            main.customer AS ref_12
                                        WHERE
                                            0
                                        LIMIT 93))
                                AND ((0)
                                    OR (1)))
                            AND (0))))
                AND (((1)
                        OR (EXISTS (
                                SELECT
                                    ref_0.r_name AS c0,
                                    ref_0.r_name AS c1,
                                    ref_13.p_type AS c2,
                                    ref_13.p_name AS c3,
                                    ref_0.r_comment AS c4,
                                    ref_13.p_retailprice AS c5,
                                    ref_0.r_name AS c6,
                                    ref_13.p_size AS c7,
                                    ref_13.p_size AS c8,
                                    ref_13.p_size AS c9,
                                    ref_0.r_comment AS c10
                                FROM
                                    main.part AS ref_13
                                WHERE (0)
                                OR (1)
                            LIMIT 127)))
                OR ((ref_0.r_comment IS NOT NULL)
                    AND ((0)
                        OR ((ref_0.r_comment IS NOT NULL)
                            OR (((1)
                                    AND ((ref_0.r_regionkey IS NULL)
                                        AND (((1)
                                                OR (((1)
                                                        AND (1))
                                                    OR ((EXISTS (
                                                                SELECT
                                                                    ref_0.r_regionkey AS c0,
                                                                    ref_14.s_address AS c1,
                                                                    ref_14.s_suppkey AS c2,
                                                                    ref_0.r_regionkey AS c3
                                                                FROM
                                                                    main.supplier AS ref_14
                                                                WHERE
                                                                    1
                                                                LIMIT 35))
                                                        AND ((EXISTS (
                                                                    SELECT
                                                                        ref_0.r_comment AS c0
                                                                    FROM
                                                                        main.partsupp AS ref_15
                                                                    WHERE (ref_0.r_name IS NULL)
                                                                    AND ((0)
                                                                        OR (ref_0.r_name IS NOT NULL))))
                                                            AND (((((0)
                                                                            AND (ref_0.r_name IS NULL))
                                                                        AND ((ref_0.r_comment IS NOT NULL)
                                                                            AND (1)))
                                                                    AND (0))
                                                                OR (EXISTS (
                                                                        SELECT
                                                                            ref_0.r_comment AS c0, ref_0.r_comment AS c1, ref_16.ps_comment AS c2, ref_0.r_comment AS c3, ref_0.r_comment AS c4
                                                                        FROM
                                                                            main.partsupp AS ref_16
                                                                        WHERE
                                                                            1
                                                                        LIMIT 84)))))))
                                            AND (1))))
                                OR (1)))))))
        OR (1))
    AND (0) THEN
    ref_0.r_comment
ELSE
    ref_0.r_comment
END AS c8,
CASE WHEN (EXISTS (
            SELECT
                ref_17.c_mktsegment AS c0,
                45 AS c1
            FROM
                main.customer AS ref_17
            WHERE
                ref_17.c_address IS NOT NULL))
        OR ((1)
            OR (ref_0.r_name IS NULL)) THEN
        ref_0.r_name
    ELSE
        ref_0.r_name
    END AS c9
FROM
    main.region AS ref_0
WHERE (ref_0.r_name IS NULL)
OR (EXISTS (
        SELECT
            ref_18.r_comment AS c0, ref_0.r_name AS c1, ref_18.r_name AS c2, ref_18.r_comment AS c3
        FROM
            main.region AS ref_18
        WHERE (0)
        AND (0)))
LIMIT 76
