SELECT
    CASE WHEN (ref_0.r_regionkey IS NULL)
        AND (1) THEN
        CASE WHEN 1 THEN
            ref_0.r_name
        ELSE
            ref_0.r_name
        END
    ELSE
        CASE WHEN 1 THEN
            ref_0.r_name
        ELSE
            ref_0.r_name
        END
    END AS c0,
    ref_0.r_comment AS c1,
    ref_0.r_regionkey AS c2,
    ref_0.r_name AS c3,
    ref_0.r_regionkey AS c4,
    ref_0.r_name AS c5,
    (
        SELECT
            n_nationkey
        FROM
            main.nation
        LIMIT 1 offset 1) AS c6,
    ref_0.r_regionkey AS c7,
    ref_0.r_comment AS c8,
    ref_0.r_name AS c9,
    ref_0.r_regionkey AS c10,
    ref_0.r_name AS c11,
    CAST(nullif (ref_0.r_regionkey, CAST(coalesce(ref_0.r_regionkey, ref_0.r_regionkey) AS INTEGER)) AS INTEGER) AS c12,
    ref_0.r_name AS c13,
    CASE WHEN CASE WHEN ref_0.r_regionkey IS NOT NULL THEN
            ref_0.r_comment
        ELSE
            ref_0.r_comment
        END IS NOT NULL THEN
        ref_0.r_regionkey
    ELSE
        ref_0.r_regionkey
    END AS c14,
    ref_0.r_name AS c15,
    ref_0.r_comment AS c16,
    ref_0.r_comment AS c17
FROM
    main.region AS ref_0
WHERE
    ref_0.r_comment IS NULL
LIMIT 154
