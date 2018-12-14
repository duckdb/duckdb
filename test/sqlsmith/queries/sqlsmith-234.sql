SELECT
    78 AS c0,
    subq_0.c0 AS c1,
    subq_0.c0 AS c2,
    CASE WHEN subq_0.c0 IS NOT NULL THEN
        subq_0.c0
    ELSE
        subq_0.c0
    END AS c3,
    subq_0.c0 AS c4,
    subq_0.c0 AS c5,
    subq_0.c0 AS c6,
    (
        SELECT
            p_type
        FROM
            main.part
        LIMIT 1 offset 3) AS c7,
    subq_0.c0 AS c8
FROM (
    SELECT
        20 AS c0
    FROM
        main.supplier AS ref_0
    LEFT JOIN main.lineitem AS ref_1
    INNER JOIN main.customer AS ref_2 ON ((ref_1.l_shipmode IS NOT NULL)
            AND ((1)
                AND (ref_2.c_name IS NULL))) ON (ref_0.s_comment = ref_1.l_returnflag)
WHERE
    ref_2.c_comment IS NOT NULL
LIMIT 109) AS subq_0
WHERE (subq_0.c0 IS NULL)
AND (0)
