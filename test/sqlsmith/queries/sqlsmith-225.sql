SELECT
    CASE WHEN subq_1.c2 IS NULL THEN
        CASE WHEN 1 THEN
            subq_1.c5
        ELSE
            subq_1.c5
        END
    ELSE
        CASE WHEN 1 THEN
            subq_1.c5
        ELSE
            subq_1.c5
        END
    END AS c0,
    subq_1.c2 AS c1,
    subq_1.c0 AS c2,
    subq_1.c2 AS c3,
    subq_1.c1 AS c4,
    subq_1.c9 AS c5
FROM (
    SELECT
        subq_0.c6 AS c0,
        subq_0.c3 AS c1,
        14 AS c2,
        subq_0.c4 AS c3,
        subq_0.c6 AS c4,
        subq_0.c8 AS c5,
        subq_0.c4 AS c6,
        subq_0.c5 AS c7,
        subq_0.c6 AS c8,
        (
            SELECT
                l_commitdate
            FROM
                main.lineitem
            LIMIT 1 offset 1) AS c9
    FROM (
        SELECT
            ref_0.n_regionkey AS c0,
            82 AS c1,
            ref_0.n_name AS c2,
            ref_0.n_name AS c3,
            ref_0.n_comment AS c4,
            ref_0.n_comment AS c5,
            ref_0.n_regionkey AS c6,
            ref_0.n_comment AS c7,
            ref_0.n_regionkey AS c8,
            ref_0.n_regionkey AS c9,
            ref_0.n_comment AS c10
        FROM
            main.nation AS ref_0
        WHERE
            ref_0.n_comment IS NULL
        LIMIT 141) AS subq_0
WHERE
    subq_0.c5 IS NULL) AS subq_1
WHERE
    subq_1.c6 IS NULL
