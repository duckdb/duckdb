SELECT
    ref_0.r_regionkey AS c0,
    ref_0.r_name AS c1,
    CAST(nullif (ref_0.r_comment, ref_0.r_name) AS VARCHAR) AS c2
FROM
    main.region AS ref_0
WHERE
    ref_0.r_name IS NULL
LIMIT 53
