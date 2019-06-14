INSERT INTO main.orders
    VALUES (87, CAST(coalesce(84, CASE WHEN 58 IS NOT NULL THEN
                    82
                ELSE
                    82
                END) AS INTEGER), CASE WHEN ((91 IS NULL)
            OR ((((1)
                        AND (31 IS NOT NULL))
                    OR (EXISTS (
                            SELECT
                                subq_0.c0 AS c0
                            FROM
                                main.lineitem AS ref_0,
                                LATERAL (
                                    SELECT
                                        ref_0.l_linestatus AS c0,
                                        ref_0.l_discount AS c1,
                                        ref_1.n_name AS c2,
                                        ref_0.l_shipmode AS c3
                                    FROM
                                        main.nation AS ref_1
                                    WHERE
                                        0
                                    LIMIT 135) AS subq_0
                            WHERE ((1)
                                AND (subq_0.c0 IS NOT NULL))
                            OR (ref_0.l_commitdate IS NULL))))
                AND (((((7 IS NOT NULL)
                                OR (((19 IS NOT NULL)
                                        OR (EXISTS (
                                                SELECT
                                                    ref_2.c_name AS c0, ref_2.c_address AS c1
                                                FROM
                                                    main.customer AS ref_2
                                                WHERE (0)
                                                OR (EXISTS (
                                                        SELECT
                                                            ref_3.s_phone AS c0, ref_3.s_comment AS c1, ref_2.c_address AS c2
                                                        FROM
                                                            main.supplier AS ref_3
                                                        WHERE
                                                            ref_2.c_nationkey IS NOT NULL))
                                                LIMIT 160)))
                                    AND (0)))
                            OR (0))
                        AND (1))
                    AND (1))))
            AND ((22 IS NULL)
                OR (84 IS NOT NULL)) THEN
            CAST(NULL AS VARCHAR)
        ELSE
            CAST(NULL AS VARCHAR)
        END,
        CAST(NULL AS DECIMAL),
        CASE WHEN 1 IS NOT NULL THEN
            CAST(NULL AS DATE)
        ELSE
            CAST(NULL AS DATE)
        END,
        DEFAULT,
        DEFAULT,
        60,
        DEFAULT)
