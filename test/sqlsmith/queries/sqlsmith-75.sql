SELECT
    subq_0.c25 AS c0,
    subq_0.c21 AS c1,
    subq_0.c11 AS c2,
    subq_0.c17 AS c3,
    subq_0.c17 AS c4,
    3 AS c5,
    subq_0.c3 AS c6,
    subq_0.c34 AS c7,
    subq_0.c4 AS c8,
    CAST(coalesce(subq_0.c30, subq_0.c4) AS INTEGER) AS c9
FROM (
    SELECT
        ref_0.s_suppkey AS c0,
        ref_0.s_suppkey AS c1,
        ref_0.s_address AS c2,
        ref_0.s_suppkey AS c3,
        100 AS c4,
        ref_0.s_name AS c5,
        ref_0.s_comment AS c6,
        ref_0.s_suppkey AS c7,
        (
            SELECT
                n_name
            FROM
                main.nation
            LIMIT 1 offset 1) AS c8,
        ref_0.s_suppkey AS c9,
        ref_0.s_address AS c10,
        ref_0.s_nationkey AS c11,
        ref_0.s_acctbal AS c12,
        ref_0.s_address AS c13,
        ref_0.s_comment AS c14,
        ref_0.s_nationkey AS c15,
        ref_0.s_name AS c16,
        ref_0.s_comment AS c17,
        ref_0.s_phone AS c18,
        ref_0.s_address AS c19,
        ref_0.s_nationkey AS c20,
        ref_0.s_acctbal AS c21,
        ref_0.s_comment AS c22,
        CAST(coalesce(ref_0.s_address, CASE WHEN 1 THEN
                    ref_0.s_phone
                ELSE
                    ref_0.s_phone
                END) AS VARCHAR) AS c23,
        ref_0.s_suppkey AS c24,
        ref_0.s_address AS c25,
        ref_0.s_nationkey AS c26,
        ref_0.s_address AS c27,
        CAST(nullif (ref_0.s_name, ref_0.s_name) AS VARCHAR) AS c28,
        (
            SELECT
                s_suppkey
            FROM
                main.supplier
            LIMIT 1 offset 3) AS c29,
        ref_0.s_nationkey AS c30,
        ref_0.s_address AS c31,
        ref_0.s_suppkey AS c32,
        ref_0.s_comment AS c33,
        ref_0.s_address AS c34
    FROM
        main.supplier AS ref_0
    WHERE (ref_0.s_comment IS NULL)
    OR (0)) AS subq_0
WHERE (1)
AND (43 IS NOT NULL)
LIMIT 75
