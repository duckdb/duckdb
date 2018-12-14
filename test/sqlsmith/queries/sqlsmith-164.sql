SELECT
    ref_0.c_mktsegment AS c0,
    ref_0.c_acctbal AS c1,
    ref_0.c_nationkey AS c2,
    ref_0.c_comment AS c3,
    ref_0.c_custkey AS c4,
    ref_0.c_nationkey AS c5,
    ref_0.c_nationkey AS c6,
    ref_0.c_phone AS c7,
    ref_0.c_phone AS c8,
    ref_0.c_nationkey AS c9,
    ref_0.c_custkey AS c10,
    ref_0.c_name AS c11,
    ref_0.c_address AS c12,
    ref_0.c_name AS c13,
    ref_0.c_custkey AS c14,
    CASE WHEN CASE WHEN 1 THEN
            ref_0.c_phone
        ELSE
            ref_0.c_phone
        END IS NOT NULL THEN
        ref_0.c_acctbal
    ELSE
        ref_0.c_acctbal
    END AS c15
FROM
    main.customer AS ref_0
WHERE ((ref_0.c_name IS NULL)
    AND (1))
AND (ref_0.c_comment IS NULL)
