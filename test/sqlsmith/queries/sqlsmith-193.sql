SELECT
    subq_0.c17 AS c0,
    subq_0.c3 AS c1,
    ref_0.ps_supplycost AS c2,
    subq_0.c9 AS c3
FROM
    main.partsupp AS ref_0
    INNER JOIN (
        SELECT
            ref_1.c_name AS c0,
            ref_1.c_custkey AS c1,
            ref_1.c_address AS c2,
            ref_1.c_acctbal AS c3,
            ref_1.c_acctbal AS c4,
            ref_1.c_comment AS c5,
            ref_1.c_name AS c6,
            ref_1.c_acctbal AS c7,
            ref_1.c_comment AS c8,
            ref_1.c_nationkey AS c9,
            ref_1.c_nationkey AS c10,
            ref_1.c_custkey AS c11,
            ref_1.c_acctbal AS c12,
            ref_1.c_nationkey AS c13,
            ref_1.c_mktsegment AS c14,
            ref_1.c_comment AS c15,
            ref_1.c_comment AS c16,
            CAST(coalesce(ref_1.c_phone, ref_1.c_name) AS VARCHAR) AS c17
        FROM
            main.customer AS ref_1
        WHERE
            ref_1.c_phone IS NOT NULL
        LIMIT 143) AS subq_0 ON (ref_0.ps_suppkey = subq_0.c1)
WHERE
    1
LIMIT 167
