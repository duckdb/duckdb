SELECT
    (
        SELECT
            o_totalprice
        FROM
            main.orders
        LIMIT 1 offset 1) AS c0
FROM (
    SELECT
        subq_0.c1 AS c0,
        subq_0.c1 AS c1,
        subq_0.c3 AS c2
    FROM (
        SELECT
            ref_1.s_nationkey AS c0,
            (
                SELECT
                    r_regionkey
                FROM
                    main.region
                LIMIT 1 offset 2) AS c1,
            ref_1.s_name AS c2,
            ref_2.o_orderpriority AS c3,
            ref_1.s_phone AS c4,
            ref_0.c_address AS c5,
            ref_1.s_acctbal AS c6,
            ref_1.s_name AS c7,
            41 AS c8,
            ref_0.c_mktsegment AS c9,
            ref_0.c_name AS c10,
            ref_0.c_name AS c11,
            ref_2.o_custkey AS c12,
            ref_1.s_address AS c13,
            ref_2.o_orderkey AS c14,
            ref_2.o_orderpriority AS c15
        FROM
            main.customer AS ref_0
            INNER JOIN main.supplier AS ref_1
            LEFT JOIN main.orders AS ref_2 ON (ref_1.s_acctbal IS NULL) ON (ref_0.c_mktsegment = ref_1.s_name)
        WHERE (1)
        OR ((0)
            OR (((1)
                    OR (0))
                OR ((((1)
                            OR (ref_0.c_custkey IS NULL))
                        AND (ref_1.s_phone IS NOT NULL))
                    AND (ref_1.s_name IS NULL))))
    LIMIT 131) AS subq_0
WHERE ((subq_0.c7 IS NOT NULL)
    OR (EXISTS (
            SELECT
                ref_3.ps_suppkey AS c0, subq_0.c15 AS c1, subq_0.c2 AS c2, subq_0.c7 AS c3, (
                    SELECT
                        n_regionkey
                    FROM
                        main.nation
                    LIMIT 1 offset 4) AS c4,
                ref_3.ps_supplycost AS c5,
                subq_0.c13 AS c6,
                ref_3.ps_suppkey AS c7,
                subq_0.c8 AS c8,
                ref_3.ps_supplycost AS c9,
                subq_0.c6 AS c10,
                subq_0.c5 AS c11,
                ref_3.ps_comment AS c12,
                ref_3.ps_supplycost AS c13,
                subq_0.c7 AS c14,
                subq_0.c13 AS c15,
                ref_3.ps_partkey AS c16,
                subq_0.c6 AS c17
            FROM
                main.partsupp AS ref_3
            WHERE
                ref_3.ps_supplycost IS NULL
            LIMIT 181)))
OR ((1)
    AND (((subq_0.c2 IS NOT NULL)
            AND (1))
        AND ((((subq_0.c12 IS NOT NULL)
                    OR (((subq_0.c11 IS NOT NULL)
                            OR (((1)
                                    AND (1))
                                AND (EXISTS (
                                        SELECT
                                            ref_4.o_custkey AS c0,
                                            ref_4.o_orderstatus AS c1,
                                            subq_0.c6 AS c2
                                        FROM
                                            main.orders AS ref_4
                                        WHERE (1)
                                        OR (EXISTS (
                                                SELECT
                                                    ref_5.c_name AS c0, subq_0.c0 AS c1
                                                FROM
                                                    main.customer AS ref_5
                                                WHERE
                                                    ref_5.c_mktsegment IS NULL
                                                LIMIT 160))
                                    LIMIT 101))))
                    OR (1)))
            OR (1))
        OR (((subq_0.c0 IS NULL)
                AND ((((
                                SELECT
                                    s_phone
                                FROM
                                    main.supplier
                                LIMIT 1 offset 6)
                            IS NOT NULL)
                        OR (1))
                    AND (0)))
            OR (0)))))
LIMIT 107) AS subq_1
WHERE (((subq_1.c2 IS NULL)
        OR (0))
    AND (subq_1.c0 IS NULL))
AND ((1)
    AND ((subq_1.c1 IS NULL)
        AND (((EXISTS (
                        SELECT
                            subq_1.c2 AS c0, ref_6.s_suppkey AS c1, subq_1.c1 AS c2
                        FROM
                            main.supplier AS ref_6
                        WHERE ((ref_6.s_address IS NULL)
                            AND (1))
                        OR ((0)
                            AND ((0)
                                AND ((0)
                                    AND (1))))))
                OR (0))
            AND (subq_1.c0 IS NOT NULL))))
