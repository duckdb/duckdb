SELECT
    ref_0.r_comment AS c0,
    CASE WHEN ref_0.r_name IS NULL THEN
        ref_0.r_comment
    ELSE
        ref_0.r_comment
    END AS c1,
    ref_0.r_regionkey AS c2,
    ref_0.r_comment AS c3,
    CAST(nullif (ref_0.r_comment, ref_0.r_name) AS VARCHAR) AS c4,
    ref_0.r_name AS c5,
    ref_0.r_name AS c6,
    ref_0.r_regionkey AS c7,
    CAST(coalesce(ref_0.r_regionkey, CASE WHEN 0 THEN
                ref_0.r_regionkey
            ELSE
                ref_0.r_regionkey
            END) AS INTEGER) AS c8
FROM
    main.region AS ref_0
WHERE (ref_0.r_regionkey IS NOT NULL)
AND (ref_0.r_name IS NOT NULL)
LIMIT 61
