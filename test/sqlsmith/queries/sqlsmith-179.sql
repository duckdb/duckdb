SELECT
    subq_1.c0 AS c0,
    CASE WHEN 1 THEN
        subq_1.c0
    ELSE
        subq_1.c0
    END AS c1,
    subq_1.c0 AS c2,
    subq_1.c0 AS c3,
    CAST(coalesce(subq_1.c0, CASE WHEN 0 THEN
                subq_1.c0
            ELSE
                subq_1.c0
            END) AS VARCHAR) AS c4
FROM (
    SELECT
        subq_0.c5 AS c0
    FROM (
        SELECT
            ref_1.r_name AS c0,
            ref_1.r_regionkey AS c1,
            ref_0.c_custkey AS c2,
            ref_0.c_phone AS c3,
            ref_1.r_name AS c4,
            ref_1.r_comment AS c5,
            ref_0.c_name AS c6
        FROM
            main.customer AS ref_0
            INNER JOIN main.region AS ref_1 ON (ref_0.c_address IS NOT NULL)
        WHERE
            ref_1.r_regionkey IS NOT NULL) AS subq_0
    WHERE
        subq_0.c5 IS NOT NULL
    LIMIT 56) AS subq_1
WHERE
    1
