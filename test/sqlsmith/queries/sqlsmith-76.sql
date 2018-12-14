SELECT
    subq_0.c0 AS c0,
    ref_0.s_name AS c1,
    ref_0.s_name AS c2,
    subq_0.c2 AS c3
FROM
    main.supplier AS ref_0
    INNER JOIN (
        SELECT
            ref_1.p_container AS c0,
            ref_1.p_mfgr AS c1,
            ref_1.p_type AS c2,
            ref_1.p_mfgr AS c3
        FROM
            main.part AS ref_1
        WHERE (ref_1.p_type IS NULL)
        AND ((1)
            OR (ref_1.p_name IS NOT NULL))) AS subq_0
    LEFT JOIN (
        SELECT
            ref_2.s_name AS c0, 70 AS c1, ref_2.s_address AS c2, ref_2.s_name AS c3, ref_2.s_suppkey AS c4, ref_2.s_address AS c5, ref_2.s_suppkey AS c6, ref_2.s_suppkey AS c7
        FROM
            main.supplier AS ref_2
        WHERE
            1
        LIMIT 154) AS subq_1 ON (subq_1.c4 IS NOT NULL) ON (ref_0.s_name = subq_0.c0)
WHERE
    EXISTS (
        SELECT
            48 AS c0, CASE WHEN subq_1.c0 IS NOT NULL THEN
                ref_3.l_shipdate
            ELSE
                ref_3.l_shipdate
            END AS c1, subq_1.c3 AS c2
        FROM
            main.lineitem AS ref_3
        WHERE (0)
        AND (subq_0.c2 IS NULL)
    LIMIT 134)
LIMIT 73
