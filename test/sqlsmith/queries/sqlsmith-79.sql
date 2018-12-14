SELECT
    subq_0.c0 AS c0,
    subq_0.c0 AS c1,
    subq_0.c0 AS c2,
    subq_0.c0 AS c3,
    CAST(nullif (subq_0.c0, subq_0.c0) AS VARCHAR) AS c4,
    CAST(coalesce(subq_0.c0, subq_0.c0) AS VARCHAR) AS c5,
    subq_0.c0 AS c6
FROM (
    SELECT
        ref_1.c_comment AS c0
    FROM
        main.lineitem AS ref_0
    LEFT JOIN main.customer AS ref_1
    INNER JOIN main.customer AS ref_2 ON (((ref_1.c_acctbal IS NOT NULL)
                AND (1))
            OR ((1)
                AND (0))) ON (ref_0.l_shipinstruct = ref_1.c_name)
        INNER JOIN main.partsupp AS ref_3 ON (ref_2.c_phone IS NOT NULL)
    WHERE
        ref_0.l_commitdate IS NOT NULL
    LIMIT 111) AS subq_0
WHERE
    subq_0.c0 IS NOT NULL
LIMIT 50
