INSERT INTO main.lineitem
        VALUES (65, 77, 34, 16, 91, DEFAULT, CAST(coalesce(CAST(NULL AS DECIMAL), CAST(NULL AS DECIMAL)) AS DECIMAL), CASE WHEN EXISTS (
                    SELECT
                        ref_2.p_type AS c0,
                        ref_0.s_suppkey AS c1,
                        92 AS c2,
                        ref_1.n_nationkey AS c3
                    FROM
                        main.supplier AS ref_0
                    RIGHT JOIN main.nation AS ref_1
                    RIGHT JOIN main.part AS ref_2 ON (ref_1.n_comment = ref_2.p_name)
                    INNER JOIN main.nation AS ref_3
                    RIGHT JOIN main.partsupp AS ref_4 ON (ref_3.n_name IS NOT NULL) ON (ref_2.p_container = ref_3.n_name) ON (ref_0.s_comment = ref_2.p_name)
                WHERE
                    0
                LIMIT 104) THEN
            CAST(NULL AS DECIMAL)
        ELSE
            CAST(NULL AS DECIMAL)
        END,
        DEFAULT,
        CAST(NULL AS VARCHAR),
        CASE WHEN 100 IS NULL THEN
            CAST(NULL AS DATE)
        ELSE
            CAST(NULL AS DATE)
        END,
        CAST(NULL AS DATE),
        DEFAULT,
        DEFAULT,
        CASE WHEN 0 THEN
            CAST(NULL AS VARCHAR)
        ELSE
            CAST(NULL AS VARCHAR)
        END,
        CAST(NULL AS VARCHAR))
