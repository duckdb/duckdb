SELECT
    ref_0.s_address AS c0,
    CASE WHEN 1 THEN
        CASE WHEN ref_0.s_address IS NULL THEN
            ref_0.s_nationkey
        ELSE
            ref_0.s_nationkey
        END
    ELSE
        CASE WHEN ref_0.s_address IS NULL THEN
            ref_0.s_nationkey
        ELSE
            ref_0.s_nationkey
        END
    END AS c1,
    CASE WHEN CASE WHEN 7 IS NOT NULL THEN
            ref_0.s_address
        ELSE
            ref_0.s_address
        END IS NOT NULL THEN
        ref_0.s_nationkey
    ELSE
        ref_0.s_nationkey
    END AS c2,
    36 AS c3,
    CAST(nullif (ref_0.s_suppkey, ref_0.s_nationkey) AS INTEGER) AS c4,
    ref_0.s_phone AS c5
FROM
    main.supplier AS ref_0
WHERE (ref_0.s_phone IS NOT NULL)
OR (ref_0.s_comment IS NOT NULL)
LIMIT 103
