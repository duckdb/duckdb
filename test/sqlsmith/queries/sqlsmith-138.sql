SELECT
    ref_0.r_regionkey AS c0,
    ref_0.r_comment AS c1,
    CASE WHEN (0)
        AND (
            CASE WHEN (0)
                AND (1) THEN
                ref_0.r_regionkey
            ELSE
                ref_0.r_regionkey
            END IS NULL) THEN
        ref_0.r_comment
    ELSE
        ref_0.r_comment
    END AS c2,
    ref_0.r_comment AS c3,
    ref_0.r_regionkey AS c4
FROM
    main.region AS ref_0
WHERE
    ref_0.r_name IS NULL
LIMIT 57
