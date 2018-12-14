SELECT
    94 AS c0,
    8 AS c1,
    CASE WHEN (subq_0.c0 IS NOT NULL)
        AND (((0)
                AND (1))
            AND (subq_0.c1 IS NULL)) THEN
        CASE WHEN 0 THEN
            subq_0.c1
        ELSE
            subq_0.c1
        END
    ELSE
        CASE WHEN 0 THEN
            subq_0.c1
        ELSE
            subq_0.c1
        END
    END AS c2,
    subq_0.c0 AS c3,
    subq_0.c1 AS c4,
    subq_0.c0 AS c5,
    subq_0.c1 AS c6,
    subq_0.c0 AS c7,
    subq_0.c1 AS c8,
    subq_0.c1 AS c9,
    subq_0.c1 AS c10,
    subq_0.c0 AS c11,
    subq_0.c1 AS c12
FROM (
    SELECT
        37 AS c0,
        ref_0.r_name AS c1
    FROM
        main.region AS ref_0
    WHERE
        ref_0.r_name IS NULL) AS subq_0
WHERE
    1
LIMIT 126
