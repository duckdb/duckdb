SELECT
    ref_0.s_name AS c0
FROM
    main.supplier AS ref_0
    INNER JOIN (
        SELECT
            DISTINCT ref_1.p_size AS c0,
            ref_1.p_brand AS c1,
            ref_1.p_mfgr AS c2,
            ref_1.p_comment AS c3
        FROM
            main.part AS ref_1
        WHERE ((ref_1.p_partkey IS NOT NULL)
            OR (EXISTS (
                    SELECT
                        ref_1.p_type AS c0, (
                            SELECT
                                s_phone
                            FROM
                                main.supplier
                            LIMIT 1 offset 1) AS c1,
                        ref_1.p_name AS c2,
                        ref_1.p_partkey AS c3,
                        ref_1.p_container AS c4,
                        (
                            SELECT
                                p_brand
                            FROM
                                main.part
                            LIMIT 1 offset 5) AS c5
                    FROM
                        main.customer AS ref_2
                    WHERE (ref_1.p_mfgr IS NULL)
                    AND (0)
                LIMIT 25)))
    AND (ref_1.p_comment IS NOT NULL)
LIMIT 156) AS subq_0 ON (ref_0.s_comment = subq_0.c1)
WHERE ((1)
    OR (ref_0.s_address IS NOT NULL))
OR (subq_0.c0 IS NOT NULL)
LIMIT 54
