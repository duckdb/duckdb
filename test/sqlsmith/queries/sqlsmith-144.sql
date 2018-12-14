SELECT
    subq_0.c1 AS c0
FROM (
    SELECT
        ref_0.s_comment AS c0,
        CASE WHEN ref_0.s_name IS NOT NULL THEN
            CASE WHEN 1 THEN
                ref_0.s_suppkey
            ELSE
                ref_0.s_suppkey
            END
        ELSE
            CASE WHEN 1 THEN
                ref_0.s_suppkey
            ELSE
                ref_0.s_suppkey
            END
        END AS c1
    FROM
        main.supplier AS ref_0
    WHERE
        ref_0.s_address IS NOT NULL
    LIMIT 120) AS subq_0
WHERE (((((EXISTS (
                        SELECT
                            subq_0.c1 AS c0, ref_1.o_orderpriority AS c1, ref_1.o_orderstatus AS c2, ref_1.o_totalprice AS c3, ref_1.o_orderkey AS c4, subq_0.c0 AS c5, ref_1.o_comment AS c6, ref_1.o_custkey AS c7, subq_0.c0 AS c8
                        FROM
                            main.orders AS ref_1
                        WHERE
                            0
                        LIMIT 68))
                AND ((((((subq_0.c1 IS NULL)
                                    OR ((subq_0.c0 IS NOT NULL)
                                        OR (1)))
                                OR ((((subq_0.c1 IS NULL)
                                            OR ((subq_0.c1 IS NULL)
                                                AND (EXISTS (
                                                        SELECT
                                                            subq_0.c0 AS c0,
                                                            ref_2.r_regionkey AS c1,
                                                            subq_0.c0 AS c2,
                                                            ref_2.r_comment AS c3,
                                                            subq_0.c0 AS c4,
                                                            subq_0.c1 AS c5,
                                                            ref_2.r_comment AS c6
                                                        FROM
                                                            main.region AS ref_2
                                                        WHERE
                                                            1
                                                        LIMIT 116))))
                                        OR (1))
                                    OR ((1)
                                        OR (subq_0.c1 IS NULL))))
                            AND (((subq_0.c1 IS NULL)
                                    AND (0))
                                OR ((((0)
                                            OR (subq_0.c0 IS NOT NULL))
                                        OR (1))
                                    AND (1))))
                        AND ((((subq_0.c1 IS NOT NULL)
                                    AND ((1)
                                        AND (((((subq_0.c1 IS NOT NULL)
                                                        OR (EXISTS (
                                                                SELECT
                                                                    subq_0.c1 AS c0,
                                                                    subq_0.c0 AS c1,
                                                                    subq_0.c1 AS c2,
                                                                    subq_0.c1 AS c3,
                                                                    ref_3.p_brand AS c4,
                                                                    subq_0.c1 AS c5,
                                                                    ref_3.p_size AS c6,
                                                                    subq_0.c0 AS c7,
                                                                    subq_0.c0 AS c8,
                                                                    ref_3.p_comment AS c9,
                                                                    subq_0.c0 AS c10,
                                                                    subq_0.c1 AS c11,
                                                                    ref_3.p_retailprice AS c12,
                                                                    subq_0.c1 AS c13,
                                                                    ref_3.p_retailprice AS c14,
                                                                    subq_0.c0 AS c15,
                                                                    subq_0.c0 AS c16,
                                                                    ref_3.p_name AS c17
                                                                FROM
                                                                    main.part AS ref_3
                                                                WHERE
                                                                    0
                                                                LIMIT 88)))
                                                    AND (subq_0.c1 IS NULL))
                                                AND (subq_0.c0 IS NULL))
                                            AND (0))))
                                AND (EXISTS (
                                        SELECT
                                            subq_0.c0 AS c0,
                                            subq_0.c1 AS c1,
                                            ref_4.l_suppkey AS c2,
                                            99 AS c3,
                                            ref_4.l_shipmode AS c4,
                                            ref_4.l_returnflag AS c5
                                        FROM
                                            main.lineitem AS ref_4
                                        WHERE
                                            1
                                        LIMIT 44)))
                            OR (((0)
                                    AND (subq_0.c1 IS NOT NULL))
                                OR (EXISTS (
                                        SELECT
                                            ref_5.p_comment AS c0,
                                            subq_0.c1 AS c1,
                                            ref_5.p_partkey AS c2,
                                            subq_0.c0 AS c3,
                                            subq_0.c0 AS c4,
                                            subq_0.c0 AS c5,
                                            ref_5.p_size AS c6
                                        FROM
                                            main.part AS ref_5
                                        WHERE
                                            1
                                        LIMIT 144)))))
                    OR (((EXISTS (
                                    SELECT
                                        subq_0.c1 AS c0,
                                        ref_6.p_partkey AS c1,
                                        ref_6.p_name AS c2,
                                        subq_0.c1 AS c3,
                                        ref_6.p_size AS c4
                                    FROM
                                        main.part AS ref_6
                                    WHERE
                                        ref_6.p_partkey IS NULL))
                                AND ((1)
                                    OR ((subq_0.c0 IS NULL)
                                        OR (subq_0.c1 IS NULL))))
                            AND (EXISTS (
                                    SELECT
                                        subq_0.c1 AS c0, ref_7.n_name AS c1
                                    FROM
                                        main.nation AS ref_7
                                    WHERE
                                        EXISTS (
                                            SELECT
                                                32 AS c0, 29 AS c1, ref_7.n_comment AS c2, ref_7.n_comment AS c3, ref_7.n_name AS c4, ref_8.ps_availqty AS c5, ref_7.n_name AS c6, ref_7.n_comment AS c7, subq_0.c1 AS c8, ref_8.ps_suppkey AS c9, ref_7.n_name AS c10, ref_7.n_comment AS c11, subq_0.c1 AS c12
                                            FROM
                                                main.partsupp AS ref_8
                                            WHERE (0)
                                            AND (ref_8.ps_partkey IS NULL)))))))
                    OR ((((((((0)
                                                OR (1))
                                            OR ((1)
                                                OR ((1)
                                                    OR ((0)
                                                        AND (EXISTS (
                                                                SELECT
                                                                    subq_0.c0 AS c0, subq_0.c1 AS c1, ref_9.l_extendedprice AS c2, subq_0.c0 AS c3, subq_0.c0 AS c4, ref_9.l_returnflag AS c5
                                                                FROM
                                                                    main.lineitem AS ref_9
                                                                WHERE
                                                                    ref_9.l_shipdate IS NOT NULL
                                                                LIMIT 169))))))
                                        AND (0))
                                    OR (EXISTS (
                                            SELECT
                                                34 AS c0,
                                                ref_10.o_custkey AS c1,
                                                subq_0.c1 AS c2,
                                                subq_0.c0 AS c3,
                                                subq_0.c1 AS c4,
                                                subq_0.c0 AS c5,
                                                ref_10.o_orderstatus AS c6
                                            FROM
                                                main.orders AS ref_10
                                            WHERE
                                                1)))
                                    OR ((((0)
                                                OR ((0)
                                                    OR (0)))
                                            AND (EXISTS (
                                                    SELECT
                                                        subq_0.c0 AS c0, 31 AS c1, ref_11.c_mktsegment AS c2, ref_11.c_phone AS c3, ref_11.c_phone AS c4, ref_11.c_address AS c5, subq_0.c0 AS c6, subq_0.c0 AS c7, 6 AS c8, subq_0.c0 AS c9, subq_0.c0 AS c10, ref_11.c_nationkey AS c11, subq_0.c1 AS c12
                                                    FROM
                                                        main.customer AS ref_11
                                                    WHERE (1)
                                                    OR (EXISTS (
                                                            SELECT
                                                                ref_12.s_comment AS c0, subq_0.c1 AS c1, 31 AS c2, ref_11.c_nationkey AS c3
                                                            FROM
                                                                main.supplier AS ref_12
                                                            WHERE
                                                                ref_12.s_suppkey IS NULL
                                                            LIMIT 130))
                                                LIMIT 90)))
                                    AND (subq_0.c0 IS NOT NULL)))
                            OR ((EXISTS (
                                        SELECT
                                            ref_13.o_clerk AS c0,
                                            ref_13.o_comment AS c1,
                                            subq_0.c0 AS c2,
                                            73 AS c3,
                                            ref_13.o_custkey AS c4,
                                            subq_0.c0 AS c5,
                                            ref_13.o_custkey AS c6,
                                            subq_0.c1 AS c7,
                                            subq_0.c0 AS c8,
                                            ref_13.o_orderpriority AS c9,
                                            45 AS c10,
                                            ref_13.o_orderstatus AS c11,
                                            ref_13.o_comment AS c12,
                                            ref_13.o_comment AS c13,
                                            subq_0.c0 AS c14,
                                            ref_13.o_shippriority AS c15,
                                            ref_13.o_orderkey AS c16
                                        FROM
                                            main.orders AS ref_13
                                        WHERE
                                            ref_13.o_shippriority IS NULL
                                        LIMIT 96))
                                AND ((subq_0.c0 IS NOT NULL)
                                    AND (EXISTS (
                                            SELECT
                                                subq_0.c1 AS c0,
                                                subq_0.c1 AS c1,
                                                subq_0.c1 AS c2
                                            FROM
                                                main.orders AS ref_14
                                            WHERE (((EXISTS (
                                                            SELECT
                                                                subq_0.c1 AS c0, ref_15.c_comment AS c1, ref_14.o_orderdate AS c2
                                                            FROM
                                                                main.customer AS ref_15
                                                            WHERE
                                                                EXISTS (
                                                                    SELECT
                                                                        48 AS c0
                                                                    FROM
                                                                        main.region AS ref_16
                                                                    WHERE
                                                                        1
                                                                    LIMIT 133)
                                                            LIMIT 56))
                                                    OR ((1)
                                                        AND (1)))
                                                AND (subq_0.c0 IS NULL))
                                            OR ((EXISTS (
                                                        SELECT
                                                            ref_14.o_totalprice AS c0,
                                                            ref_17.p_name AS c1,
                                                            ref_14.o_orderstatus AS c2,
                                                            subq_0.c0 AS c3,
                                                            subq_0.c1 AS c4,
                                                            ref_14.o_orderkey AS c5,
                                                            subq_0.c1 AS c6,
                                                            ref_17.p_size AS c7
                                                        FROM
                                                            main.part AS ref_17
                                                        WHERE (1)
                                                        OR ((1)
                                                            OR (EXISTS (
                                                                    SELECT
                                                                        ref_17.p_mfgr AS c0, ref_14.o_custkey AS c1, subq_0.c1 AS c2, ref_14.o_comment AS c3, ref_14.o_totalprice AS c4, ref_18.l_linenumber AS c5, ref_14.o_custkey AS c6, ref_18.l_shipdate AS c7, subq_0.c1 AS c8
                                                                    FROM
                                                                        main.lineitem AS ref_18
                                                                    WHERE (subq_0.c0 IS NULL)
                                                                    OR ((0)
                                                                        OR (1))
                                                                LIMIT 100)))))
                                            AND (0)))))))
                    AND ((EXISTS (
                                SELECT
                                    ref_19.p_brand AS c0,
                                    ref_19.p_brand AS c1,
                                    subq_0.c0 AS c2,
                                    ref_19.p_size AS c3
                                FROM
                                    main.part AS ref_19
                                WHERE
                                    ref_19.p_mfgr IS NULL))
                            OR ((EXISTS (
                                        SELECT
                                            DISTINCT subq_0.c0 AS c0, subq_0.c0 AS c1, ref_20.r_name AS c2
                                        FROM
                                            main.region AS ref_20
                                        WHERE (EXISTS (
                                                SELECT
                                                    ref_20.r_regionkey AS c0
                                                FROM
                                                    main.supplier AS ref_21
                                                WHERE
                                                    EXISTS (
                                                        SELECT
                                                            ref_22.l_quantity AS c0
                                                        FROM
                                                            main.lineitem AS ref_22
                                                        WHERE
                                                            ref_21.s_suppkey IS NULL)
                                                    LIMIT 151))
                                            AND (((((0)
                                                            OR ((ref_20.r_name IS NULL)
                                                                AND ((0)
                                                                    AND (1))))
                                                        OR (1))
                                                    OR (0))
                                                OR (0))))
                                    AND (0)))))
                    AND (subq_0.c0 IS NOT NULL))
                AND (0))
            OR (
                CASE WHEN ((subq_0.c1 IS NULL)
                        AND ((18 IS NULL)
                            OR (1)))
                    AND (0) THEN
                    subq_0.c0
                ELSE
                    subq_0.c0
                END IS NOT NULL)
        LIMIT 101
