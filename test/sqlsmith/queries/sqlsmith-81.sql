SELECT
    ref_0.r_name AS c0,
    CASE WHEN CASE WHEN EXISTS (
                SELECT
                    ref_1.r_name AS c0,
                    ref_1.r_comment AS c1,
                    ref_1.r_regionkey AS c2,
                    47 AS c3,
                    ref_0.r_comment AS c4,
                    93 AS c5
                FROM
                    main.region AS ref_1
                WHERE
                    ref_0.r_comment IS NOT NULL
                LIMIT 153) THEN
            CASE WHEN 1 THEN
                ref_0.r_comment
            ELSE
                ref_0.r_comment
            END
        ELSE
            CASE WHEN 1 THEN
                ref_0.r_comment
            ELSE
                ref_0.r_comment
            END
        END IS NOT NULL THEN
        ref_0.r_name
    ELSE
        ref_0.r_name
    END AS c1,
    ref_0.r_comment AS c2,
    CASE WHEN ref_0.r_comment IS NOT NULL THEN
        ref_0.r_name
    ELSE
        ref_0.r_name
    END AS c3,
    ref_0.r_comment AS c4,
    ref_0.r_name AS c5,
    44 AS c6,
    CASE WHEN (1)
        OR (1) THEN
        ref_0.r_regionkey
    ELSE
        ref_0.r_regionkey
    END AS c7,
    ref_0.r_comment AS c8
FROM
    main.region AS ref_0
WHERE
    ref_0.r_name IS NULL
