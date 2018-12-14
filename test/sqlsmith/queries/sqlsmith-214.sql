SELECT
    subq_0.c0 AS c0,
    CASE WHEN (subq_0.c1 IS NOT NULL)
        AND (subq_0.c3 IS NULL) THEN
        subq_0.c0
    ELSE
        subq_0.c0
    END AS c1,
    CASE WHEN EXISTS (
            SELECT
                subq_0.c1 AS c0,
                subq_0.c4 AS c1
            FROM
                main.part AS ref_5
            WHERE
                0
            LIMIT 76) THEN
        subq_0.c0
    ELSE
        subq_0.c0
    END AS c2,
    subq_0.c0 AS c3
FROM (
    SELECT
        ref_2.r_regionkey AS c0,
        ref_1.r_name AS c1,
        ref_3.l_quantity AS c2,
        ref_3.l_returnflag AS c3,
        ref_0.s_suppkey AS c4,
        CAST(nullif ((
                    SELECT
                        l_partkey FROM main.lineitem
                    LIMIT 1 offset 5), ref_3.l_linenumber) AS INTEGER) AS c5
    FROM
        main.supplier AS ref_0
        INNER JOIN main.region AS ref_1 ON (ref_0.s_address = ref_1.r_name)
        INNER JOIN main.region AS ref_2
        INNER JOIN main.lineitem AS ref_3
        INNER JOIN main.part AS ref_4 ON ((1)
                AND (1)) ON (ref_4.p_type IS NOT NULL) ON (ref_1.r_name = ref_4.p_name)
    WHERE
        89 IS NOT NULL) AS subq_0
WHERE
    1
