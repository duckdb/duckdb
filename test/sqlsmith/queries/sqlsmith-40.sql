SELECT
    subq_0.c4 AS c0,
    subq_0.c2 AS c1,
    subq_0.c2 AS c2,
    subq_0.c6 AS c3,
    subq_0.c4 AS c4,
    subq_0.c3 AS c5,
    subq_0.c2 AS c6,
    subq_0.c2 AS c7,
    subq_0.c6 AS c8,
    CASE WHEN 1 THEN
        subq_0.c2
    ELSE
        subq_0.c2
    END AS c9
FROM (
    SELECT
        ref_0.r_regionkey AS c0,
        ref_0.r_regionkey AS c1,
        ref_0.r_name AS c2,
        (
            SELECT
                r_regionkey
            FROM
                main.region
            LIMIT 1 offset 1) AS c3,
        ref_0.r_regionkey AS c4,
        ref_0.r_regionkey AS c5,
        ref_0.r_comment AS c6
    FROM
        main.region AS ref_0
    WHERE
        1) AS subq_0
WHERE
    1
LIMIT 99
