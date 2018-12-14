SELECT
    CAST(coalesce(subq_1.c1, 82) AS INTEGER) AS c0,
    subq_1.c0 AS c1,
    subq_1.c0 AS c2,
    59 AS c3,
    CASE WHEN (subq_1.c0 IS NOT NULL)
        OR (subq_1.c2 IS NOT NULL) THEN
        subq_2.c2
    ELSE
        subq_2.c2
    END AS c4,
    subq_2.c0 AS c5,
    subq_2.c1 AS c6,
    subq_1.c2 AS c7,
    86 AS c8,
    subq_1.c0 AS c9,
    CASE WHEN subq_1.c2 IS NOT NULL THEN
        subq_1.c2
    ELSE
        subq_1.c2
    END AS c10,
    subq_1.c3 AS c11,
    CASE WHEN 1 THEN
        (
            SELECT
                n_nationkey
            FROM
                main.nation
            LIMIT 1 offset 6)
    ELSE
        (
            SELECT
                n_nationkey
            FROM
                main.nation
            LIMIT 1 offset 6)
    END AS c12,
    subq_2.c1 AS c13
FROM (
    SELECT
        subq_0.c0 AS c0,
        subq_0.c0 AS c1,
        CASE WHEN (subq_0.c0 IS NOT NULL)
            AND ((subq_0.c0 IS NOT NULL)
                AND ((EXISTS (
                            SELECT
                                subq_0.c0 AS c0
                            FROM
                                main.orders AS ref_2
                            WHERE
                                EXISTS (
                                    SELECT
                                        subq_0.c0 AS c0, ref_2.o_orderstatus AS c1, ref_3.o_orderkey AS c2
                                    FROM
                                        main.orders AS ref_3
                                    WHERE
                                        0)))
                            AND ((EXISTS (
                                        SELECT
                                            ref_4.l_linestatus AS c0
                                        FROM
                                            main.lineitem AS ref_4
                                        WHERE
                                            EXISTS (
                                                SELECT
                                                    ref_5.p_container AS c0, ref_4.l_extendedprice AS c1, ref_4.l_extendedprice AS c2, subq_0.c0 AS c3, subq_0.c0 AS c4, ref_4.l_suppkey AS c5, subq_0.c0 AS c6, subq_0.c0 AS c7
                                                FROM
                                                    main.part AS ref_5
                                                WHERE (57 IS NOT NULL)
                                                AND (0))))
                                    AND (((EXISTS (
                                                    SELECT
                                                        ref_6.p_container AS c0, subq_0.c0 AS c1, subq_0.c0 AS c2, subq_0.c0 AS c3, ref_6.p_type AS c4, subq_0.c0 AS c5, subq_0.c0 AS c6, ref_6.p_retailprice AS c7, subq_0.c0 AS c8, subq_0.c0 AS c9
                                                    FROM
                                                        main.part AS ref_6
                                                    WHERE
                                                        0
                                                    LIMIT 144))
                                            OR (EXISTS (
                                                    SELECT
                                                        (
                                                            SELECT
                                                                o_totalprice
                                                            FROM
                                                                main.orders
                                                            LIMIT 1 offset 3) AS c0,
                                                        (
                                                            SELECT
                                                                r_regionkey
                                                            FROM
                                                                main.region
                                                            LIMIT 1 offset 2) AS c1
                                                    FROM
                                                        main.customer AS ref_7
                                                    WHERE (
                                                        SELECT
                                                            p_brand
                                                        FROM
                                                            main.part
                                                        LIMIT 1 offset 1)
                                                    IS NULL
                                                LIMIT 51)))
                                    AND (subq_0.c0 IS NULL))))) THEN
                    subq_0.c0
                ELSE
                    subq_0.c0
                END AS c2,
                subq_0.c0 AS c3
            FROM (
                SELECT
                    ref_0.o_shippriority AS c0
                FROM
                    main.orders AS ref_0
                WHERE (1)
                AND (((1)
                        OR ((1)
                            OR ((((0)
                                        OR ((1)
                                            OR (ref_0.o_custkey IS NOT NULL)))
                                    AND (0))
                                OR (ref_0.o_clerk IS NOT NULL))))
                    AND ((EXISTS (
                                SELECT
                                    ref_0.o_orderkey AS c0, ref_1.ps_comment AS c1, ref_1.ps_partkey AS c2, ref_1.ps_suppkey AS c3
                                FROM
                                    main.partsupp AS ref_1
                                WHERE (0)
                                OR (((1)
                                        AND (ref_1.ps_comment IS NULL))
                                    OR (1))))
                        OR ((94 IS NOT NULL)
                            OR (0))))
            LIMIT 33) AS subq_0
    WHERE (
        SELECT
            c_custkey
        FROM
            main.customer
        LIMIT 1 offset 89)
    IS NOT NULL
LIMIT 49) AS subq_1
    INNER JOIN (
        SELECT
            ref_9.o_shippriority AS c0,
            CASE WHEN (ref_9.o_custkey IS NOT NULL)
                    AND (ref_9.o_custkey IS NOT NULL) THEN
                    ref_8.p_mfgr
                ELSE
                    ref_8.p_mfgr
                END AS c1,
                ref_8.p_retailprice AS c2,
                ref_8.p_mfgr AS c3
            FROM
                main.part AS ref_8
        RIGHT JOIN main.orders AS ref_9
        INNER JOIN main.customer AS ref_10 ON (ref_9.o_shippriority IS NOT NULL) ON (ref_8.p_mfgr = ref_9.o_orderstatus)
    WHERE ((((ref_10.c_name IS NOT NULL)
                OR ((EXISTS (
                            SELECT
                                ref_10.c_custkey AS c0, ref_8.p_type AS c1, ref_11.s_name AS c2, ref_8.p_partkey AS c3, ref_11.s_name AS c4, ref_8.p_mfgr AS c5, ref_8.p_comment AS c6, ref_8.p_name AS c7, (
                                    SELECT
                                        r_regionkey
                                    FROM
                                        main.region
                                    LIMIT 1 offset 1) AS c8,
                                4 AS c9,
                                ref_9.o_custkey AS c10,
                                ref_11.s_phone AS c11
                            FROM
                                main.supplier AS ref_11
                            WHERE
                                0))
                        AND (ref_8.p_size IS NULL)))
                OR (EXISTS (
                        SELECT
                            ref_8.p_type AS c0, ref_9.o_orderdate AS c1
                        FROM
                            main.partsupp AS ref_12
                        WHERE ((((1)
                                    OR (ref_10.c_phone IS NULL))
                                AND (EXISTS (
                                        SELECT
                                            ref_9.o_custkey AS c0, ref_8.p_name AS c1, ref_8.p_brand AS c2, ref_8.p_type AS c3
                                        FROM
                                            main.orders AS ref_13
                                        WHERE
                                            1
                                        LIMIT 94)))
                            AND ((0)
                                OR (ref_8.p_size IS NULL)))
                        OR (ref_9.o_orderdate IS NOT NULL)
                    LIMIT 144)))
        AND (ref_10.c_phone IS NOT NULL))
    AND ((ref_8.p_retailprice IS NULL)
        AND (((ref_8.p_comment IS NOT NULL)
                OR ((EXISTS (
                            SELECT
                                ref_9.o_orderstatus AS c0,
                                (
                                    SELECT
                                        p_size
                                    FROM
                                        main.part
                                    LIMIT 1 offset 3) AS c1,
                                ref_8.p_mfgr AS c2
                            FROM
                                main.partsupp AS ref_14
                            WHERE
                                ref_9.o_orderkey IS NOT NULL
                            LIMIT 105))
                    AND (ref_10.c_phone IS NULL)))
            AND (((ref_10.c_custkey IS NOT NULL)
                    AND ((((1)
                                AND ((0)
                                    OR (1)))
                            AND (EXISTS (
                                    SELECT
                                        ref_10.c_name AS c0,
                                        ref_8.p_name AS c1,
                                        ref_9.o_clerk AS c2,
                                        (
                                            SELECT
                                                p_brand
                                            FROM
                                                main.part
                                            LIMIT 1 offset 3) AS c3,
                                        ref_8.p_container AS c4,
                                        ref_8.p_brand AS c5,
                                        (
                                            SELECT
                                                o_orderkey
                                            FROM
                                                main.orders
                                            LIMIT 1 offset 1) AS c6,
                                        ref_15.o_orderstatus AS c7,
                                        (
                                            SELECT
                                                s_phone
                                            FROM
                                                main.supplier
                                            LIMIT 1 offset 3) AS c8,
                                        ref_8.p_retailprice AS c9,
                                        26 AS c10,
                                        ref_10.c_acctbal AS c11,
                                        ref_9.o_shippriority AS c12,
                                        ref_9.o_orderstatus AS c13,
                                        ref_15.o_orderstatus AS c14,
                                        ref_10.c_name AS c15,
                                        ref_9.o_orderkey AS c16,
                                        ref_10.c_mktsegment AS c17
                                    FROM
                                        main.orders AS ref_15
                                    WHERE (
                                        SELECT
                                            c_nationkey
                                        FROM
                                            main.customer
                                        LIMIT 1 offset 36)
                                    IS NOT NULL)))
                        OR (((1)
                                AND (ref_8.p_container IS NULL))
                            AND (1))))
                OR ((ref_8.p_brand IS NULL)
                    AND (ref_8.p_partkey IS NOT NULL)))))
LIMIT 130) AS subq_2 ON (subq_1.c0 = subq_2.c0)
WHERE
    0
