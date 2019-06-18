SELECT
    subq_1.c0 AS c0,
    subq_1.c0 AS c1,
    subq_1.c0 AS c2,
    43 AS c3,
    subq_1.c0 AS c4,
    (
        SELECT
            l_linestatus
        FROM
            main.lineitem
        LIMIT 1 offset 100) AS c5,
    subq_1.c0 AS c6,
    subq_1.c0 AS c7
FROM ( SELECT DISTINCT
        subq_0.c0 AS c0
    FROM (
        SELECT
            ref_0.c_phone AS c0
        FROM
            main.customer AS ref_0
        WHERE
            1) AS subq_0
    WHERE
        subq_0.c0 IS NOT NULL) AS subq_1
WHERE
    1
