SELECT
    ref_1.l_commitdate AS c0,
    CAST(nullif (subq_0.c0, CASE WHEN 1 THEN
                ref_1.l_shipinstruct
            ELSE
                ref_1.l_shipinstruct
            END) AS VARCHAR) AS c1,
    ref_1.l_returnflag AS c2,
    ref_1.l_orderkey AS c3
FROM (
    SELECT
        ref_0.n_comment AS c0
    FROM
        main.nation AS ref_0
    WHERE
        ref_0.n_comment IS NULL
    LIMIT 83) AS subq_0
    LEFT JOIN main.lineitem AS ref_1 ON ((EXISTS (
                SELECT
                    ref_1.l_orderkey AS c0,
                    ref_1.l_quantity AS c1,
                    ref_1.l_quantity AS c2,
                    ref_2.r_name AS c3,
                    ref_1.l_shipinstruct AS c4,
                    ref_1.l_shipinstruct AS c5
                FROM
                    main.region AS ref_2
                WHERE
                    1
                LIMIT 153))
        AND (
            CASE WHEN (((((((((EXISTS (
                                                    SELECT
                                                        ref_1.l_suppkey AS c0,
                                                        subq_0.c0 AS c1,
                                                        subq_0.c0 AS c2,
                                                        ref_1.l_comment AS c3,
                                                        subq_0.c0 AS c4,
                                                        ref_3.s_acctbal AS c5,
                                                        ref_1.l_discount AS c6,
                                                        ref_1.l_comment AS c7,
                                                        ref_1.l_receiptdate AS c8,
                                                        subq_0.c0 AS c9,
                                                        ref_1.l_partkey AS c10,
                                                        ref_1.l_extendedprice AS c11,
                                                        subq_0.c0 AS c12,
                                                        ref_3.s_comment AS c13,
                                                        ref_3.s_suppkey AS c14,
                                                        subq_0.c0 AS c15,
                                                        ref_3.s_phone AS c16,
                                                        17 AS c17,
                                                        ref_1.l_extendedprice AS c18
                                                    FROM
                                                        main.supplier AS ref_3
                                                    WHERE
                                                        0
                                                    LIMIT 81))
                                            OR (1))
                                        OR (subq_0.c0 IS NULL))
                                    OR (((1)
                                            OR (ref_1.l_receiptdate IS NOT NULL))
                                        AND ((0)
                                            OR (subq_0.c0 IS NULL))))
                                AND (0))
                            OR (1))
                        OR ((0)
                            OR ((ref_1.l_suppkey IS NULL)
                                OR ((0)
                                    OR (1)))))
                    OR ((1)
                        AND ((1)
                            AND (0))))
                AND ((((((((subq_0.c0 IS NULL)
                                            OR (ref_1.l_discount IS NULL))
                                        OR (1))
                                    AND (ref_1.l_shipinstruct IS NOT NULL))
                                OR (0))
                            OR (ref_1.l_receiptdate IS NOT NULL))
                        OR (subq_0.c0 IS NULL))
                    AND (0)))
                AND (1) THEN
                ref_1.l_linestatus
            ELSE
                ref_1.l_linestatus
            END IS NULL))
WHERE (subq_0.c0 IS NULL)
    OR (subq_0.c0 IS NULL)
LIMIT 100
