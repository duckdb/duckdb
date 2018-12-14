SELECT
    subq_0.c2 AS c0,
    subq_0.c4 AS c1,
    subq_0.c5 AS c2,
    subq_0.c0 AS c3,
    subq_0.c8 AS c4,
    CAST(coalesce(subq_0.c7, subq_0.c1) AS VARCHAR) AS c5,
    subq_0.c9 AS c6,
    subq_0.c8 AS c7
FROM (
    SELECT
        ref_1.o_shippriority AS c0,
        ref_0.r_comment AS c1,
        35 AS c2,
        ref_1.o_shippriority AS c3,
        ref_1.o_orderkey AS c4,
        ref_1.o_custkey AS c5,
        ref_1.o_totalprice AS c6,
        ref_1.o_clerk AS c7,
        ref_2.o_totalprice AS c8,
        ref_0.r_regionkey AS c9
    FROM
        main.region AS ref_0
        INNER JOIN main.orders AS ref_1
        INNER JOIN main.orders AS ref_2 ON (ref_2.o_comment IS NOT NULL) ON (ref_0.r_name = ref_1.o_orderstatus)
    WHERE
        ref_0.r_regionkey IS NULL) AS subq_0
WHERE (((subq_0.c2 IS NOT NULL)
        AND ((subq_0.c6 IS NULL)
            AND (0)))
    OR (0))
AND ((
        CASE WHEN subq_0.c2 IS NOT NULL THEN
            subq_0.c8
        ELSE
            subq_0.c8
        END IS NOT NULL)
    AND ((((55 IS NOT NULL)
                AND (((0)
                        AND ((0)
                            OR ((subq_0.c3 IS NOT NULL)
                                OR (EXISTS (
                                        SELECT
                                            ref_3.p_container AS c0, subq_0.c1 AS c1, subq_0.c8 AS c2, subq_0.c4 AS c3, subq_0.c5 AS c4, ref_3.p_container AS c5, subq_0.c9 AS c6, ref_3.p_brand AS c7, ref_3.p_partkey AS c8, subq_0.c9 AS c9, ref_3.p_type AS c10, subq_0.c9 AS c11
                                        FROM
                                            main.part AS ref_3
                                        WHERE (1)
                                        AND (((0)
                                                AND ((0)
                                                    AND (0)))
                                            OR (1))
                                    LIMIT 90)))))
                AND (1)))
        AND (((subq_0.c8 IS NOT NULL)
                AND (((((((0)
                                        AND ((0)
                                            OR ((subq_0.c9 IS NOT NULL)
                                                OR ((((1)
                                                            OR ((0)
                                                                OR (0)))
                                                        OR ((EXISTS (
                                                                    SELECT
                                                                        subq_0.c0 AS c0, subq_0.c8 AS c1, ref_4.ps_comment AS c2, ref_4.ps_supplycost AS c3, subq_0.c2 AS c4, ref_4.ps_partkey AS c5, subq_0.c9 AS c6
                                                                    FROM
                                                                        main.partsupp AS ref_4
                                                                    WHERE
                                                                        subq_0.c2 IS NOT NULL
                                                                    LIMIT 119))
                                                            AND (EXISTS (
                                                                    SELECT
                                                                        ref_5.c_custkey AS c0
                                                                    FROM
                                                                        main.customer AS ref_5
                                                                    WHERE (0)
                                                                    OR ((((((((0)
                                                                                                OR (1))
                                                                                            OR (1))
                                                                                        AND ((0)
                                                                                            AND (subq_0.c9 IS NULL)))
                                                                                    AND ((EXISTS (
                                                                                                SELECT
                                                                                                    subq_0.c1 AS c0, ref_6.p_container AS c1, ref_6.p_mfgr AS c2, subq_0.c1 AS c3, ref_6.p_type AS c4, 44 AS c5, ref_6.p_name AS c6, ref_6.p_partkey AS c7, subq_0.c6 AS c8, ref_5.c_custkey AS c9
                                                                                                FROM
                                                                                                    main.part AS ref_6
                                                                                                WHERE
                                                                                                    1
                                                                                                LIMIT 113))
                                                                                        AND (1)))
                                                                                AND (0))
                                                                            AND (0))
                                                                        AND (1))
                                                                LIMIT 141))))
                                                AND (subq_0.c6 IS NULL)))))
                                OR (subq_0.c1 IS NOT NULL))
                            OR ((1)
                                AND (0)))
                        OR (0))
                    OR (EXISTS (
                            SELECT
                                subq_0.c8 AS c0,
                                subq_0.c0 AS c1
                            FROM
                                main.region AS ref_7
                            WHERE
                                1
                            LIMIT 93)))
                AND (EXISTS (
                        SELECT
                            ref_8.o_shippriority AS c0,
                            ref_8.o_totalprice AS c1
                        FROM
                            main.orders AS ref_8
                        WHERE ((0)
                            OR (EXISTS (
                                    SELECT
                                        ref_9.ps_partkey AS c0, ref_8.o_orderdate AS c1, ref_8.o_shippriority AS c2, ref_8.o_shippriority AS c3
                                    FROM
                                        main.partsupp AS ref_9
                                    WHERE (subq_0.c0 IS NULL)
                                    AND ((0)
                                        OR (0)))))
                        OR (ref_8.o_totalprice IS NOT NULL)
                    LIMIT 60))))
    AND (((1)
            AND (subq_0.c3 IS NULL))
        AND (0))))
AND ((0)
    OR (0))))
