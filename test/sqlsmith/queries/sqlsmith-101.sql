SELECT
    CASE WHEN EXISTS (
            SELECT
                ref_1.r_regionkey AS c0
            FROM
                main.region AS ref_1
            WHERE ((subq_0.c0 IS NOT NULL)
                AND (0))
            OR (ref_1.r_regionkey IS NULL)) THEN
        subq_0.c1
    ELSE
        subq_0.c1
    END AS c0, subq_0.c2 AS c1, subq_0.c3 AS c2, subq_0.c1 AS c3, CAST(nullif (subq_0.c2, subq_0.c0) AS INTEGER) AS c4, 89 AS c5, subq_0.c1 AS c6, subq_0.c0 AS c7, subq_0.c2 AS c8, subq_0.c3 AS c9, CAST(nullif (subq_0.c3, CAST(nullif (subq_0.c1, subq_0.c3) AS VARCHAR)) AS VARCHAR) AS c10, subq_0.c1 AS c11, subq_0.c0 AS c12, (
        SELECT
            ps_suppkey
        FROM
            main.partsupp
        LIMIT 1 offset 1) AS c13,
    subq_0.c1 AS c14,
    subq_0.c2 AS c15,
    subq_0.c3 AS c16,
    subq_0.c2 AS c17,
    subq_0.c1 AS c18
FROM (
    SELECT
        CASE WHEN ref_0.r_regionkey IS NOT NULL THEN
            ref_0.r_regionkey
        ELSE
            ref_0.r_regionkey
        END AS c0,
        ref_0.r_name AS c1,
        71 AS c2,
        (
            SELECT
                n_name
            FROM
                main.nation
            LIMIT 1 offset 4) AS c3
    FROM
        main.region AS ref_0
    WHERE
        ref_0.r_comment IS NOT NULL) AS subq_0
WHERE
    CASE WHEN CASE WHEN 0 THEN
            subq_0.c2
        ELSE
            subq_0.c2
        END IS NULL THEN
        subq_0.c3
    ELSE
        subq_0.c3
    END IS NOT NULL
