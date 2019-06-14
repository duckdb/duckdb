INSERT INTO main.lineitem
    VALUES (95, 82, 42, 100, 53, CAST(NULL AS DECIMAL), CAST(NULL AS DECIMAL), DEFAULT, DEFAULT, DEFAULT, CASE WHEN (((0)
                AND ((EXISTS (
                            SELECT
                                ref_0.l_shipdate AS c0,
                                (
                                    SELECT
                                        s_suppkey
                                    FROM
                                        main.supplier
                                    LIMIT 1 offset 6) AS c1,
                                ref_0.l_quantity AS c2,
                                ref_0.l_suppkey AS c3,
                                ref_0.l_comment AS c4
                            FROM
                                main.lineitem AS ref_0
                            WHERE
                                ref_0.l_comment IS NOT NULL))
                        OR (78 IS NOT NULL)))
                OR (26 IS NULL))
            OR ((98 IS NOT NULL)
                AND (75 IS NOT NULL)) THEN
            CASE WHEN ((98 IS NULL)
                OR (62 IS NOT NULL))
                OR (16 IS NOT NULL) THEN
                CAST(coalesce(CAST(NULL AS DATE), CAST(NULL AS DATE)) AS DATE)
            ELSE
                CAST(coalesce(CAST(NULL AS DATE), CAST(NULL AS DATE)) AS DATE)
            END
        ELSE
            CASE WHEN ((98 IS NULL)
                OR (62 IS NOT NULL))
                OR (16 IS NOT NULL) THEN
                CAST(coalesce(CAST(NULL AS DATE), CAST(NULL AS DATE)) AS DATE)
            ELSE
                CAST(coalesce(CAST(NULL AS DATE), CAST(NULL AS DATE)) AS DATE)
            END
        END, CASE WHEN 5 IS NOT NULL THEN
            CAST(NULL AS DATE)
        ELSE
            CAST(NULL AS DATE)
        END, CAST(NULL AS DATE), CAST(NULL AS VARCHAR), DEFAULT, CAST(nullif (CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)) AS VARCHAR))
