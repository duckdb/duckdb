WITH jennifer_0 AS (
    SELECT
        ref_0.c_address AS c0,
        CASE WHEN 1 THEN
            CAST(coalesce(ref_0.c_address, ref_0.c_address) AS VARCHAR)
        ELSE
            CAST(coalesce(ref_0.c_address, ref_0.c_address) AS VARCHAR)
        END AS c1,
        ref_0.c_custkey AS c2,
        ref_0.c_phone AS c3,
        ref_0.c_custkey AS c4,
        ref_0.c_nationkey AS c5
    FROM
        main.customer AS ref_0
    WHERE
        0
    LIMIT 66
)
SELECT
    CAST(coalesce(subq_0.c9, subq_0.c14) AS VARCHAR) AS c0,
    subq_0.c0 AS c1,
    78 AS c2
FROM (
    SELECT
        ref_1.r_regionkey AS c0,
        ref_1.r_name AS c1,
        ref_3.n_regionkey AS c2,
        ref_3.n_name AS c3,
        ref_3.n_nationkey AS c4,
        ref_2.s_suppkey AS c5,
        ref_1.r_name AS c6,
        ref_2.s_suppkey AS c7,
        ref_2.s_comment AS c8,
        ref_1.r_comment AS c9,
        ref_2.s_phone AS c10,
        ref_2.s_phone AS c11,
        ref_2.s_name AS c12,
        ref_2.s_suppkey AS c13,
        ref_3.n_name AS c14
    FROM
        main.region AS ref_1
        INNER JOIN main.supplier AS ref_2
        INNER JOIN main.nation AS ref_3 ON (17 IS NOT NULL) ON (ref_1.r_name = ref_3.n_name)
    WHERE
        CASE WHEN 1 THEN
            ref_3.n_regionkey
        ELSE
            ref_3.n_regionkey
        END IS NOT NULL
    LIMIT 62) AS subq_0
WHERE
    1
LIMIT 98
