INSERT INTO main.orders
    VALUES (27, 98, CAST(NULL AS VARCHAR), CAST(NULL AS DECIMAL), CAST(NULL AS DATE), CASE WHEN (EXISTS (
                SELECT
                    ref_0.r_regionkey AS c0,
                    ref_0.r_comment AS c1,
                    ref_0.r_regionkey AS c2,
                    ref_0.r_name AS c3,
                    ref_0.r_comment AS c4,
                    ref_0.r_comment AS c5
                FROM
                    main.region AS ref_0
                WHERE
                    ref_0.r_name IS NULL))
            AND (62 IS NOT NULL) THEN
            CAST(NULL AS VARCHAR)
        ELSE
            CAST(NULL AS VARCHAR)
        END, CAST(NULL AS VARCHAR), 97, CAST(NULL AS VARCHAR))
