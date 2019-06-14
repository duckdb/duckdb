INSERT INTO main.lineitem
    VALUES (1, 51, CASE WHEN 0 THEN
            86
        ELSE
            86
        END, CASE WHEN (64 IS NULL)
            AND ((
                    SELECT
                        p_type
                    FROM
                        main.part
                    LIMIT 1 offset 1) IS NULL) THEN
            89
        ELSE
            89
        END,
        CAST(coalesce(29, 70) AS INTEGER),
        CAST(NULL AS DECIMAL),
        CAST(nullif (CAST(NULL AS DECIMAL), CAST(NULL AS DECIMAL)) AS DECIMAL),
        DEFAULT,
        DEFAULT,
        DEFAULT,
        CASE WHEN (
            SELECT
                l_shipmode
            FROM
                main.lineitem
            LIMIT 1 offset 4) IS NULL THEN
            CAST(NULL AS DATE)
        ELSE
            CAST(NULL AS DATE)
        END,
        CAST(NULL AS DATE),
        CAST(coalesce(CAST(NULL AS DATE), CASE WHEN 50 IS NOT NULL THEN
                    CAST(NULL AS DATE)
                ELSE
                    CAST(NULL AS DATE)
                END) AS DATE),
        CAST(NULL AS VARCHAR),
        CAST(NULL AS VARCHAR),
        DEFAULT)
