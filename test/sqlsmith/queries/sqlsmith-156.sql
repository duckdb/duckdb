SELECT
    subq_0.c15 AS c0,
    CAST(coalesce(subq_0.c24, subq_0.c2) AS INTEGER) AS c1,
    subq_0.c18 AS c2
FROM (
    SELECT
        CAST(coalesce(ref_0.c_nationkey, CASE WHEN 1 THEN
                    ref_0.c_nationkey
                ELSE
                    ref_0.c_nationkey
                END) AS INTEGER) AS c0,
        ref_0.c_custkey AS c1,
        ref_0.c_nationkey AS c2,
        ref_0.c_phone AS c3,
        ref_0.c_mktsegment AS c4,
        ref_0.c_custkey AS c5,
        ref_0.c_acctbal AS c6,
        ref_0.c_custkey AS c7,
        ref_0.c_name AS c8,
        ref_0.c_mktsegment AS c9,
        ref_0.c_comment AS c10,
        ref_0.c_acctbal AS c11,
        12 AS c12,
        ref_0.c_custkey AS c13,
        ref_0.c_custkey AS c14,
        ref_0.c_custkey AS c15,
        89 AS c16,
        ref_0.c_mktsegment AS c17,
        ref_0.c_comment AS c18,
        ref_0.c_address AS c19,
        ref_0.c_mktsegment AS c20,
        ref_0.c_address AS c21,
        ref_0.c_comment AS c22,
        ref_0.c_nationkey AS c23,
        ref_0.c_custkey AS c24
    FROM
        main.customer AS ref_0
    WHERE ((((((((ref_0.c_address IS NOT NULL)
                                OR (0))
                            OR ((0)
                                OR ((ref_0.c_phone IS NULL)
                                    AND (((0)
                                            OR (ref_0.c_mktsegment IS NOT NULL))
                                        OR (((1)
                                                OR ((1)
                                                    OR (((0)
                                                            AND (ref_0.c_acctbal IS NOT NULL))
                                                        OR (ref_0.c_acctbal IS NULL))))
                                            AND (EXISTS (
                                                    SELECT
                                                        26 AS c0, 73 AS c1, ref_1.r_name AS c2, ref_0.c_name AS c3, ref_0.c_acctbal AS c4
                                                    FROM
                                                        main.region AS ref_1
                                                    WHERE
                                                        1
                                                    LIMIT 96)))))))
                        OR (ref_0.c_mktsegment IS NOT NULL))
                    OR ((1)
                        AND (EXISTS (
                                SELECT
                                    ref_0.c_nationkey AS c0,
                                    ref_2.ps_suppkey AS c1,
                                    ref_2.ps_suppkey AS c2,
                                    ref_0.c_custkey AS c3,
                                    ref_2.ps_partkey AS c4,
                                    ref_0.c_nationkey AS c5,
                                    ref_2.ps_suppkey AS c6,
                                    86 AS c7,
                                    ref_2.ps_availqty AS c8
                                FROM
                                    main.partsupp AS ref_2
                                WHERE (0)
                                OR ((0)
                                    AND (1))
                            LIMIT 159))))
            OR (ref_0.c_mktsegment IS NOT NULL))
        OR ((0)
            AND (ref_0.c_name IS NOT NULL)))
    OR (((1)
            AND (EXISTS (
                    SELECT
                        ref_0.c_name AS c0
                    FROM
                        main.nation AS ref_3
                    WHERE
                        EXISTS (
                            SELECT
                                ref_3.n_regionkey AS c0, ref_0.c_nationkey AS c1
                            FROM
                                main.part AS ref_4
                            WHERE
                                ref_0.c_nationkey IS NULL
                            LIMIT 81)
                    LIMIT 77)))
        AND ((0)
            AND (1))))
AND (1)) AS subq_0
WHERE
    subq_0.c5 IS NULL
LIMIT 105
