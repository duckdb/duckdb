SELECT
    CAST(nullif (CAST(coalesce(subq_0.c5, subq_0.c5) AS INTEGER), subq_0.c6) AS INTEGER) AS c0,
    subq_0.c0 AS c1,
    subq_0.c14 AS c2
FROM (
    SELECT
        ref_0.s_suppkey AS c0,
        ref_0.s_address AS c1,
        ref_0.s_acctbal AS c2,
        ref_0.s_comment AS c3,
        ref_0.s_acctbal AS c4,
        60 AS c5,
        ref_0.s_suppkey AS c6,
        ref_0.s_acctbal AS c7,
        ref_0.s_name AS c8,
        ref_0.s_name AS c9,
        ref_0.s_address AS c10,
        61 AS c11,
        ref_0.s_phone AS c12,
        ref_0.s_name AS c13,
        ref_0.s_suppkey AS c14,
        ref_0.s_suppkey AS c15
    FROM
        main.supplier AS ref_0
    WHERE (0)
    OR ((ref_0.s_address IS NOT NULL)
        AND ((EXISTS (
                    SELECT
                        21 AS c0, ref_0.s_acctbal AS c1, ref_1.n_name AS c2, ref_1.n_name AS c3, ref_0.s_suppkey AS c4, ref_0.s_phone AS c5
                    FROM
                        main.nation AS ref_1
                    WHERE
                        1
                    LIMIT 141))
            OR (1)))
LIMIT 80) AS subq_0
WHERE (1)
AND (((((1)
                OR (((0)
                        AND (((0)
                                AND (1))
                            OR (0)))
                    OR (0)))
            AND (subq_0.c0 IS NOT NULL))
        AND ((
                SELECT
                    c_custkey
                FROM
                    main.customer
                LIMIT 1 offset 5) IS NULL))
    OR (subq_0.c4 IS NOT NULL))
LIMIT 109
