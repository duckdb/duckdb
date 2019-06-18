INSERT INTO main.orders
    VALUES (40, 2, CASE WHEN EXISTS (
            SELECT
                ref_0.ps_partkey AS c0,
                ref_0.ps_partkey AS c1,
                ref_0.ps_comment AS c2
            FROM
                main.partsupp AS ref_0
            WHERE (0)
            AND ((((ref_0.ps_comment IS NOT NULL)
                        OR (0))
                    AND (1))
                OR (((0)
                        OR (1))
                    OR (0)))
        LIMIT 131) THEN
            CAST(NULL AS VARCHAR)
        ELSE
            CAST(NULL AS VARCHAR)
        END,
        DEFAULT,
        DEFAULT,
        CAST(NULL AS VARCHAR),
        CAST(NULL AS VARCHAR),
        CASE WHEN ((70 IS NOT NULL)
            OR (26 IS NULL))
            OR (1) THEN
            81
        ELSE
            81
        END,
        CAST(NULL AS VARCHAR))
