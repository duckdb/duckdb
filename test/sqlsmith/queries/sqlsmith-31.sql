INSERT INTO main.region
    VALUES (31, CASE WHEN ((((6 IS NULL)
                    OR (19 IS NOT NULL))
                OR ((47 IS NULL)
                    AND (((66 IS NOT NULL)
                            OR (EXISTS (
                                    SELECT
                                        ref_0.r_name AS c0
                                    FROM
                                        main.region AS ref_0
                                    WHERE
                                        1)))
                            AND (EXISTS (
                                    SELECT
                                        ref_1.p_mfgr AS c0, ref_1.p_mfgr AS c1, ref_1.p_type AS c2, (
                                            SELECT
                                                p_size
                                            FROM
                                                main.part
                                            LIMIT 1 offset 1) AS c3
                                    FROM
                                        main.part AS ref_1
                                    WHERE
                                        1
                                    LIMIT 117)))))
                AND (19 IS NOT NULL))
            AND (((EXISTS (
                            SELECT
                                ref_2.r_comment AS c0,
                                ref_2.r_comment AS c1,
                                subq_0.c1 AS c2,
                                ref_2.r_name AS c3,
                                subq_0.c3 AS c4,
                                ref_2.r_regionkey AS c5,
                                ref_2.r_name AS c6
                            FROM
                                main.region AS ref_2,
                                LATERAL (
                                    SELECT
                                        ref_3.s_phone AS c0,
                                        ref_3.s_comment AS c1,
                                        ref_2.r_comment AS c2,
                                        ref_3.s_suppkey AS c3
                                    FROM
                                        main.supplier AS ref_3
                                    WHERE ((1)
                                        AND (ref_3.s_nationkey IS NULL))
                                    OR (ref_2.r_regionkey IS NULL)
                                LIMIT 94) AS subq_0
                        WHERE
                            subq_0.c3 IS NULL))
                    OR (((1)
                            OR (((1)
                                    OR ((93 IS NULL)
                                        AND (1)))
                                OR (((31 IS NOT NULL)
                                        AND (0))
                                    OR (EXISTS (
                                            SELECT
                                                ref_4.c_comment AS c0, ref_4.c_nationkey AS c1, ref_4.c_acctbal AS c2, ref_4.c_name AS c3, ref_4.c_phone AS c4, ref_4.c_phone AS c5, ref_4.c_name AS c6
                                            FROM
                                                main.customer AS ref_4
                                            WHERE
                                                0
                                            LIMIT 93)))))
                        AND ((((
                                        SELECT
                                            ps_suppkey
                                        FROM
                                            main.partsupp
                                        LIMIT 1 offset 2) IS NULL)
                                AND (40 IS NOT NULL))
                            OR (97 IS NULL))))
                OR (61 IS NOT NULL)) THEN
            CAST(NULL AS VARCHAR)
        ELSE
            CAST(NULL AS VARCHAR)
        END,
        DEFAULT)
