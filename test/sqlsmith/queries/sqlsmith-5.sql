INSERT INTO main.part
    VALUES (3, CAST(NULL AS VARCHAR), CASE WHEN (((1)
                OR ((EXISTS (
                            SELECT
                                (
                                    SELECT
                                        ps_partkey
                                    FROM
                                        main.partsupp
                                    LIMIT 1 offset 2) AS c0
                            FROM
                                main.lineitem AS ref_0
                            WHERE (1)
                            AND ((ref_0.l_discount IS NOT NULL)
                                AND (ref_0.l_orderkey IS NULL))))
                    AND (((1)
                            AND (36 IS NULL))
                        AND (1))))
            AND ((48 IS NOT NULL)
                AND (1)))
            AND (98 IS NULL) THEN
            CAST(NULL AS VARCHAR)
        ELSE
            CAST(NULL AS VARCHAR)
        END, CAST(NULL AS VARCHAR), CAST(nullif (CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)) AS VARCHAR), 64, CAST(coalesce(CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)) AS VARCHAR), CAST(nullif (CAST(NULL AS DECIMAL), CAST(NULL AS DECIMAL)) AS DECIMAL), DEFAULT)
