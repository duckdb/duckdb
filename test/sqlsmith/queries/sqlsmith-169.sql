SELECT
    subq_0.c1 AS c0,
    ref_0.r_name AS c1,
    ref_0.r_name AS c2,
    ref_0.r_comment AS c3,
    subq_0.c7 AS c4,
    subq_0.c4 AS c5
FROM
    main.region AS ref_0
    INNER JOIN (
        SELECT
            ref_1.r_name AS c0,
            ref_1.r_comment AS c1,
            ref_1.r_comment AS c2,
            ref_1.r_comment AS c3,
            ref_1.r_comment AS c4,
            ref_1.r_regionkey AS c5,
            ref_1.r_regionkey AS c6,
            (
                SELECT
                    s_phone
                FROM
                    main.supplier
                LIMIT 1 offset 1) AS c7,
            78 AS c8,
            90 AS c9
        FROM
            main.region AS ref_1
        WHERE
            ref_1.r_comment IS NOT NULL) AS subq_0 ON (ref_0.r_comment = subq_0.c0)
WHERE (32 IS NULL)
OR (ref_0.r_comment IS NULL)
LIMIT 53
