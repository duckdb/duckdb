SELECT
    subq_0.c3 AS c0
FROM (
    SELECT
        DISTINCT ref_0.c_name AS c0,
        ref_0.c_nationkey AS c1,
        ref_0.c_acctbal AS c2,
        ref_0.c_address AS c3,
        ref_0.c_nationkey AS c4,
        ref_0.c_address AS c5,
        ref_0.c_custkey AS c6
    FROM
        main.customer AS ref_0
    WHERE
        1
    LIMIT 34) AS subq_0
WHERE
    3 IS NULL
LIMIT 27
