SELECT
    subq_0.c2 AS c0,
    subq_0.c2 AS c1,
    subq_0.c1 AS c2,
    subq_0.c3 AS c3,
    subq_0.c0 AS c4,
    subq_0.c1 AS c5,
    subq_0.c0 AS c6,
    subq_0.c3 AS c7,
    CASE WHEN subq_0.c2 IS NOT NULL THEN
        75
    ELSE
        75
    END AS c8,
    subq_0.c3 AS c9,
    subq_0.c2 AS c10,
    CAST(coalesce(subq_0.c3, CASE WHEN 1 THEN
                subq_0.c3
            ELSE
                subq_0.c3
            END) AS VARCHAR) AS c11,
    subq_0.c2 AS c12,
    subq_0.c3 AS c13,
    subq_0.c1 AS c14,
    subq_0.c3 AS c15,
    subq_0.c3 AS c16,
    subq_0.c2 AS c17,
    subq_0.c3 AS c18,
    subq_0.c0 AS c19,
    CASE WHEN ((subq_0.c2 IS NOT NULL)
        AND (subq_0.c2 IS NOT NULL))
        AND ((subq_0.c1 IS NULL)
            OR ((EXISTS (
                        SELECT
                            subq_0.c3 AS c0,
                            ref_5.n_comment AS c1,
                            subq_0.c3 AS c2
                        FROM
                            main.nation AS ref_5
                        WHERE
                            ref_5.n_comment IS NULL
                        LIMIT 104))
                AND (((subq_0.c3 IS NOT NULL)
                        AND (subq_0.c2 IS NOT NULL))
                    OR (subq_0.c2 IS NOT NULL)))) THEN
        subq_0.c0
    ELSE
        subq_0.c0
    END AS c20,
    subq_0.c1 AS c21,
    subq_0.c1 AS c22,
    subq_0.c1 AS c23,
    subq_0.c2 AS c24
FROM (
    SELECT
        ref_0.r_comment AS c0,
        ref_0.r_name AS c1,
        ref_0.r_name AS c2,
        ref_0.r_comment AS c3
    FROM
        main.region AS ref_0
    WHERE
        ref_0.r_name IS NOT NULL
    LIMIT 80) AS subq_0
WHERE
    1
LIMIT 50
