SELECT
    ref_0.r_regionkey AS c0,
    ref_0.r_comment AS c1,
    ref_0.r_comment AS c2,
    ref_0.r_regionkey AS c3,
    ref_0.r_comment AS c4,
    ref_0.r_comment AS c5,
    CASE WHEN CASE WHEN 0 THEN
            ref_0.r_name
        ELSE
            ref_0.r_name
        END IS NULL THEN
        ref_0.r_regionkey
    ELSE
        ref_0.r_regionkey
    END AS c6,
    ref_0.r_comment AS c7,
    ref_0.r_comment AS c8,
    ref_0.r_name AS c9
FROM
    main.region AS ref_0
WHERE
    ref_0.r_name IS NOT NULL
