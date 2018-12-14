SELECT
    ref_0.r_comment AS c0,
    70 AS c1,
    ref_0.r_regionkey AS c2,
    CAST(coalesce(CAST(coalesce(ref_0.r_name, ref_0.r_name) AS VARCHAR), CASE WHEN 0 THEN
                ref_0.r_comment
            ELSE
                ref_0.r_comment
            END) AS VARCHAR) AS c3,
    ref_0.r_name AS c4,
    ref_0.r_comment AS c5,
    ref_0.r_name AS c6,
    ref_0.r_name AS c7,
    ref_0.r_regionkey AS c8,
    ref_0.r_regionkey AS c9,
    ref_0.r_name AS c10,
    ref_0.r_name AS c11,
    ref_0.r_comment AS c12
FROM
    main.region AS ref_0
WHERE (((1)
        AND (ref_0.r_regionkey IS NULL))
    OR (ref_0.r_comment IS NOT NULL))
OR (ref_0.r_name IS NULL)
LIMIT 133
