SELECT
    subq_0.c6 AS c0,
    subq_0.c1 AS c1,
    subq_0.c2 AS c2
FROM (
    SELECT
        ref_0.c_phone AS c0,
        ref_0.c_custkey AS c1,
        ref_0.c_mktsegment AS c2,
        ref_0.c_nationkey AS c3,
        ref_0.c_nationkey AS c4,
        ref_0.c_acctbal AS c5,
        ref_0.c_nationkey AS c6,
        19 AS c7,
        CAST(coalesce(ref_0.c_comment, ref_0.c_address) AS VARCHAR) AS c8
    FROM
        main.customer AS ref_0
    WHERE
        ref_0.c_mktsegment IS NULL) AS subq_0
WHERE (subq_0.c8 IS NOT NULL)
    AND ((subq_0.c7 IS NOT NULL)
        AND ((subq_0.c7 IS NULL)
            OR ((1)
                OR ((subq_0.c6 IS NOT NULL)
                    OR (subq_0.c3 IS NULL)))))
LIMIT 105
