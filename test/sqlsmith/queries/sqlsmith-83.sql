SELECT
    subq_0.c3 AS c0,
    CAST(coalesce(subq_0.c1, CAST(coalesce(
                    CASE WHEN 1 THEN
                        subq_0.c3
                    ELSE
                        subq_0.c3
                    END, subq_0.c0) AS VARCHAR)) AS VARCHAR) AS c1,
    subq_0.c0 AS c2,
    subq_0.c1 AS c3,
    subq_0.c0 AS c4,
    subq_0.c3 AS c5
FROM (
    SELECT
        ref_0.s_comment AS c0,
        ref_0.s_address AS c1,
        ref_0.s_phone AS c2,
        ref_0.s_comment AS c3
    FROM
        main.supplier AS ref_0
    WHERE
        1
    LIMIT 90) AS subq_0
WHERE
    CASE WHEN subq_0.c1 IS NULL THEN
        subq_0.c2
    ELSE
        subq_0.c2
    END IS NULL
LIMIT 109
