INSERT INTO main.supplier
    VALUES (CAST(coalesce(
                CASE WHEN (20 IS NULL)
                    AND (((EXISTS (
                            SELECT
                                ref_0.s_phone AS c0, ref_0.s_phone AS c1, ref_0.s_name AS c2, ref_0.s_phone AS c3, ref_0.s_nationkey AS c4, ref_0.s_nationkey AS c5, ref_0.s_acctbal AS c6, ref_0.s_comment AS c7 FROM main.supplier AS ref_0
                            WHERE ((
                                SELECT
                                    r_name FROM main.region
                                LIMIT 1 offset 5) IS NOT NULL)
                        AND (0)))
                    AND (77 IS NOT NULL))
                    AND ((26 IS NOT NULL)
                    AND (((((0)
                    AND (29 IS NOT NULL))
                    AND (41 IS NOT NULL))
                    AND (1))
                    AND (41 IS NOT NULL)))) THEN
                    63
                ELSE
                    63
                END, 68) AS INTEGER),
        DEFAULT,
        CAST(NULL AS VARCHAR),
        100,
        DEFAULT,
        DEFAULT,
        DEFAULT)
