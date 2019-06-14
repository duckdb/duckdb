SELECT
    subq_0.c5 AS c0,
    CASE WHEN (((1)
            AND (EXISTS (
                    SELECT
                        ref_11.o_orderpriority AS c0,
                        subq_0.c4 AS c1,
                        ref_11.o_shippriority AS c2
                    FROM
                        main.orders AS ref_11
                    WHERE
                        0)))
            AND (0))
        OR ((subq_0.c5 IS NULL)
            OR ((1)
                OR (subq_0.c2 IS NULL))) THEN
        subq_0.c1
    ELSE
        subq_0.c1
    END AS c1
FROM (
    SELECT
        ref_2.n_regionkey AS c0,
        ref_8.ps_supplycost AS c1,
        ref_8.ps_supplycost AS c2,
        ref_1.ps_suppkey AS c3,
        CAST(coalesce(ref_1.ps_comment, ref_2.n_name) AS VARCHAR) AS c4,
        ref_1.ps_availqty AS c5
    FROM
        main.partsupp AS ref_0
        INNER JOIN main.partsupp AS ref_1
        INNER JOIN main.nation AS ref_2 ON ((ref_2.n_regionkey IS NOT NULL)
                AND ((ref_1.ps_availqty IS NOT NULL)
                    AND ((EXISTS (
                                SELECT
                                    ref_3.s_suppkey AS c0,
                                    ref_1.ps_partkey AS c1,
                                    ref_2.n_nationkey AS c2,
                                    ref_2.n_comment AS c3,
                                    ref_1.ps_availqty AS c4,
                                    60 AS c5,
                                    ref_3.s_comment AS c6,
                                    ref_1.ps_availqty AS c7,
                                    ref_3.s_comment AS c8,
                                    95 AS c9,
                                    ref_1.ps_comment AS c10,
                                    ref_3.s_phone AS c11,
                                    ref_1.ps_suppkey AS c12,
                                    ref_3.s_comment AS c13,
                                    ref_2.n_nationkey AS c14,
                                    ref_1.ps_partkey AS c15,
                                    ref_2.n_regionkey AS c16
                                FROM
                                    main.supplier AS ref_3
                            WHERE ((0)
                                OR (1))
                            OR (((ref_1.ps_supplycost IS NOT NULL)
                                    OR ((1)
                                        OR (ref_1.ps_comment IS NULL)))
                                AND (EXISTS (
                                        SELECT
                                            23 AS c0, ref_4.c_address AS c1, ref_1.ps_suppkey AS c2, ref_4.c_phone AS c3
                                        FROM
                                            main.customer AS ref_4
                                        WHERE
                                            EXISTS (
                                                SELECT
                                                    ref_4.c_address AS c0, ref_1.ps_availqty AS c1, ref_3.s_acctbal AS c2, ref_1.ps_availqty AS c3, ref_3.s_acctbal AS c4, ref_5.n_regionkey AS c5
                                                FROM
                                                    main.nation AS ref_5
                                                WHERE ((0)
                                                    OR (1))
                                                AND (ref_5.n_name IS NOT NULL))
                                        LIMIT 132)))
                        LIMIT 123))
                OR (1))))
    LEFT JOIN main.orders AS ref_6
    INNER JOIN main.lineitem AS ref_7 ON (ref_6.o_totalprice = ref_7.l_extendedprice) ON ((ref_6.o_orderstatus IS NULL)
            OR (1))
        INNER JOIN main.partsupp AS ref_8 ON (ref_2.n_nationkey = ref_8.ps_partkey) ON ((((1)
                        OR (0))
                    AND (((1)
                            AND (ref_0.ps_availqty IS NULL))
                        OR (1)))
                OR (1))
    WHERE (ref_1.ps_availqty IS NULL)
    AND (((ref_0.ps_comment IS NOT NULL)
            AND (ref_0.ps_comment IS NULL))
        AND (EXISTS (
                SELECT
                    ref_7.l_quantity AS c0, ref_6.o_orderstatus AS c1, ref_6.o_comment AS c2, ref_8.ps_partkey AS c3
                FROM
                    main.customer AS ref_9
                WHERE (((((0)
                                AND ((((ref_0.ps_comment IS NULL)
                                            OR ((((ref_0.ps_availqty IS NOT NULL)
                                                        OR (1))
                                                    AND (0))
                                                AND (0)))
                                        OR ((((((ref_0.ps_partkey IS NOT NULL)
                                                            AND (EXISTS (
                                                                    SELECT
                                                                        ref_6.o_orderstatus AS c0, ref_2.n_regionkey AS c1, ref_6.o_orderkey AS c2, ref_8.ps_suppkey AS c3, ref_9.c_nationkey AS c4
                                                                    FROM
                                                                        main.customer AS ref_10
                                                                    WHERE (1)
                                                                    AND (1)
                                                                LIMIT 163)))
                                                    AND (0))
                                                AND (0))
                                            AND (0))
                                        OR (1)))
                                AND (ref_2.n_comment IS NULL)))
                        AND ((0)
                            OR (ref_2.n_nationkey IS NULL)))
                    OR (1))
                OR (ref_6.o_shippriority IS NULL))
            AND ((0)
                AND (1)))))
LIMIT 88) AS subq_0
WHERE (1)
AND (subq_0.c4 IS NOT NULL)
LIMIT 145
