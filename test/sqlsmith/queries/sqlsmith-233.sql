SELECT
    ref_0.r_name AS c0,
    subq_0.c2 AS c1,
    CASE WHEN 1 THEN
        subq_0.c7
    ELSE
        subq_0.c7
    END AS c2,
    CAST(nullif (subq_0.c2, subq_0.c2) AS INTEGER) AS c3,
    CASE WHEN CASE WHEN 1 THEN
            subq_0.c0
        ELSE
            subq_0.c0
        END IS NULL THEN
        ref_0.r_regionkey
    ELSE
        ref_0.r_regionkey
    END AS c4,
    subq_0.c7 AS c5,
    subq_0.c2 AS c6,
    subq_0.c3 AS c7,
    ref_0.r_regionkey AS c8
FROM
    main.region AS ref_0
    LEFT JOIN (
        SELECT
            ref_2.ps_suppkey AS c0,
            ref_1.p_brand AS c1,
            ref_2.ps_suppkey AS c2,
            ref_2.ps_comment AS c3,
            ref_1.p_type AS c4,
            ref_1.p_partkey AS c5,
            ref_2.ps_comment AS c6,
            CASE WHEN ref_2.ps_partkey IS NOT NULL THEN
                ref_1.p_name
            ELSE
                ref_1.p_name
            END AS c7
        FROM
            main.part AS ref_1
            LEFT JOIN main.partsupp AS ref_2 ON (ref_2.ps_partkey IS NOT NULL)
        WHERE (1)
        AND (ref_1.p_comment IS NOT NULL)
    LIMIT 51) AS subq_0 ON (ref_0.r_comment = subq_0.c1)
WHERE
    1
LIMIT 70
