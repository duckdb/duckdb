SELECT
    (
        SELECT
            r_comment
        FROM
            main.region
        LIMIT 1 offset 4) AS c0,
    subq_0.c0 AS c1,
    subq_0.c1 AS c2,
    CASE WHEN subq_0.c1 IS NULL THEN
        CASE WHEN 1 THEN
            subq_0.c0
        ELSE
            subq_0.c0
        END
    ELSE
        CASE WHEN 1 THEN
            subq_0.c0
        ELSE
            subq_0.c0
        END
    END AS c3,
    (
        SELECT
            s_name
        FROM
            main.supplier
        LIMIT 1 offset 2) AS c4,
    subq_0.c0 AS c5,
    subq_0.c1 AS c6,
    subq_0.c0 AS c7
FROM (
    SELECT
        ref_0.s_name AS c0,
        ref_0.s_comment AS c1
    FROM
        main.supplier AS ref_0
    WHERE
        ref_0.s_phone IS NOT NULL
    LIMIT 120) AS subq_0
WHERE
    1
LIMIT 100
