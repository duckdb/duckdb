SELECT
    subq_0.c2 AS c0,
    CAST(nullif (subq_0.c4, CASE WHEN subq_0.c4 IS NOT NULL THEN
                CASE WHEN 0 THEN
                    subq_0.c1
                ELSE
                    subq_0.c1
                END
            ELSE
                CASE WHEN 0 THEN
                    subq_0.c1
                ELSE
                    subq_0.c1
                END
            END) AS VARCHAR) AS c1
FROM (
    SELECT
        ref_0.p_mfgr AS c0,
        (
            SELECT
                p_brand
            FROM
                main.part
            LIMIT 1 offset 3) AS c1,
        ref_1.o_comment AS c2,
        ref_0.p_partkey AS c3,
        ref_1.o_clerk AS c4
    FROM
        main.part AS ref_0
        INNER JOIN main.orders AS ref_1 ON (ref_1.o_orderstatus IS NULL)
    WHERE (
        SELECT
            r_name
        FROM
            main.region
        LIMIT 1 offset 52)
    IS NULL) AS subq_0
WHERE (subq_0.c2 IS NOT NULL)
OR (subq_0.c0 IS NULL)
LIMIT 117
