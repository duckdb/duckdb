SELECT
    CASE WHEN 1 THEN
        CAST(coalesce(subq_1.c0, subq_1.c0) AS VARCHAR)
    ELSE
        CAST(coalesce(subq_1.c0, subq_1.c0) AS VARCHAR)
    END AS c0
FROM (
    SELECT
        subq_0.c1 AS c0,
        subq_0.c6 AS c1,
        subq_0.c7 AS c2
    FROM (
        SELECT
            ref_0.p_name AS c0,
            (
                SELECT
                    p_type
                FROM
                    main.part
                LIMIT 1 offset 6) AS c1,
            ref_1.r_comment AS c2,
            ref_0.p_partkey AS c3,
            ref_2.n_comment AS c4,
            72 AS c5,
            ref_1.r_name AS c6,
            47 AS c7,
            ref_1.r_regionkey AS c8
        FROM
            main.part AS ref_0
        LEFT JOIN main.region AS ref_1
        RIGHT JOIN main.nation AS ref_2 ON ((1)
                AND (1)) ON (ref_0.p_comment = ref_1.r_name)
    WHERE
        ref_1.r_comment IS NOT NULL
    LIMIT 46) AS subq_0
WHERE
    subq_0.c0 IS NOT NULL
LIMIT 67) AS subq_1
WHERE (subq_1.c2 IS NOT NULL)
AND (subq_1.c1 IS NULL)
