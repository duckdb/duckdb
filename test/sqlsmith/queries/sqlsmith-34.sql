INSERT INTO main.partsupp
    VALUES (32, CASE WHEN ((
                SELECT
                    c_name
                FROM
                    main.customer
                LIMIT 1 offset 5) IS NOT NULL)
            OR (71 IS NOT NULL) THEN
            83
        ELSE
            83
        END,
        52,
        CAST(NULL AS DECIMAL),
        DEFAULT)
