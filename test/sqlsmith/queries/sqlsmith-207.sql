SELECT
    subq_0.c0 AS c0,
    subq_0.c6 AS c1,
    CASE WHEN subq_0.c4 IS NULL THEN
        subq_0.c2
    ELSE
        subq_0.c2
    END AS c2,
    subq_0.c8 AS c3,
    subq_0.c6 AS c4
FROM (
    SELECT
        ref_4.p_type AS c0,
        ref_1.s_comment AS c1,
        ref_3.o_orderdate AS c2,
        ref_1.s_suppkey AS c3,
        ref_0.r_comment AS c4,
        ref_1.s_name AS c5,
        ref_4.p_brand AS c6,
        ref_3.o_totalprice AS c7,
        CASE WHEN (ref_2.c_acctbal IS NOT NULL)
            AND (EXISTS (
                    SELECT
                        ref_5.n_regionkey AS c0,
                        ref_4.p_mfgr AS c1,
                        ref_3.o_shippriority AS c2,
                        ref_1.s_nationkey AS c3,
                        ref_2.c_custkey AS c4,
                        ref_0.r_regionkey AS c5
                    FROM
                        main.nation AS ref_5
                    WHERE
                        0
                    LIMIT 127)) THEN
            ref_4.p_size
        ELSE
            ref_4.p_size
        END AS c8
    FROM
        main.region AS ref_0
        INNER JOIN main.supplier AS ref_1
        RIGHT JOIN main.customer AS ref_2 ON (ref_1.s_name IS NOT NULL)
        INNER JOIN main.orders AS ref_3
        INNER JOIN main.part AS ref_4 ON (ref_4.p_name IS NOT NULL) ON (ref_1.s_suppkey = ref_3.o_orderkey) ON (ref_0.r_regionkey = ref_3.o_orderkey)
    WHERE
        ref_3.o_orderdate IS NULL
    LIMIT 112) AS subq_0
WHERE ((subq_0.c8 IS NULL)
    OR ((((subq_0.c8 IS NOT NULL)
                AND (((((subq_0.c8 IS NULL)
                                OR (((subq_0.c0 IS NOT NULL)
                                        AND (0))
                                    AND ((36 IS NOT NULL)
                                        OR ((1)
                                            OR (1)))))
                            AND (subq_0.c8 IS NULL))
                        AND ((1)
                            OR ((0)
                                OR (0))))
                    AND (((((subq_0.c8 IS NULL)
                                    AND (0))
                                AND ((
                                        SELECT
                                            c_name
                                        FROM
                                            main.customer
                                        LIMIT 1 offset 6)
                                    IS NULL))
                            AND (((0)
                                    AND ((1)
                                        AND (1)))
                                AND (subq_0.c0 IS NULL)))
                        AND (1))))
            AND (((((((
                                        SELECT
                                            n_name
                                        FROM
                                            main.nation
                                        LIMIT 1 offset 4)
                                    IS NOT NULL)
                                OR ((1)
                                    AND (1)))
                            OR (((
                                        SELECT
                                            p_retailprice
                                        FROM
                                            main.part
                                        LIMIT 1 offset 2)
                                    IS NULL)
                                AND (subq_0.c7 IS NULL)))
                        AND ((subq_0.c2 IS NOT NULL)
                            OR (subq_0.c4 IS NOT NULL)))
                    OR (((1)
                            OR ((1)
                                AND (subq_0.c6 IS NULL)))
                        OR (EXISTS (
                                SELECT
                                    subq_0.c0 AS c0,
                                    ref_6.ps_partkey AS c1,
                                    ref_6.ps_suppkey AS c2
                                FROM
                                    main.partsupp AS ref_6
                                WHERE ((ref_6.ps_suppkey IS NULL)
                                    OR ((0)
                                        AND ((0)
                                            AND ((1)
                                                AND (ref_6.ps_comment IS NULL)))))
                                OR ((0)
                                    OR ((1)
                                        OR ((0)
                                            AND (0))))))))
                OR (((subq_0.c4 IS NULL)
                        OR (subq_0.c2 IS NULL))
                    AND (((1)
                            OR ((0)
                                AND ((((1)
                                            OR (0))
                                        OR (subq_0.c0 IS NULL))
                                    AND ((0)
                                        OR ((((((1)
                                                            OR ((EXISTS (
                                                                        SELECT
                                                                            ref_7.r_regionkey AS c0, ref_7.r_name AS c1, subq_0.c7 AS c2, ref_7.r_regionkey AS c3, subq_0.c0 AS c4, subq_0.c7 AS c5, subq_0.c7 AS c6, subq_0.c7 AS c7, ref_7.r_name AS c8, subq_0.c2 AS c9
                                                                        FROM
                                                                            main.region AS ref_7
                                                                        WHERE
                                                                            subq_0.c3 IS NULL
                                                                        LIMIT 18))
                                                                OR (((1)
                                                                        OR (0))
                                                                    AND ((((30 IS NOT NULL)
                                                                                OR (1))
                                                                            OR (EXISTS (
                                                                                    SELECT
                                                                                        subq_0.c5 AS c0,
                                                                                        subq_0.c3 AS c1,
                                                                                        ref_8.s_name AS c2,
                                                                                        subq_0.c2 AS c3,
                                                                                        subq_0.c8 AS c4
                                                                                    FROM
                                                                                        main.supplier AS ref_8
                                                                                    WHERE
                                                                                        ref_8.s_acctbal IS NULL
                                                                                    LIMIT 66)))
                                                                        AND (1)))))
                                                        AND (1))
                                                    OR (0))
                                                OR ((1)
                                                    OR (0)))
                                            AND (1))))))
                        AND (subq_0.c8 IS NULL)))))
        OR (((1)
                AND (1))
            OR ((((EXISTS (
                                SELECT
                                    ref_9.c_custkey AS c0,
                                    ref_9.c_custkey AS c1,
                                    ref_9.c_nationkey AS c2,
                                    subq_0.c7 AS c3,
                                    subq_0.c7 AS c4,
                                    subq_0.c1 AS c5
                                FROM
                                    main.customer AS ref_9
                                WHERE
                                    0
                                LIMIT 122))
                        OR (1))
                    AND (1))
                OR (0)))))
OR ((
        SELECT
            p_comment
        FROM
            main.part
        LIMIT 1 offset 6)
    IS NULL)
LIMIT 113
