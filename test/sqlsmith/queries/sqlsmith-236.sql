SELECT
    CASE WHEN 0 THEN
        subq_1.c12
    ELSE
        subq_1.c12
    END AS c0,
    subq_1.c14 AS c1,
    CASE WHEN subq_1.c14 IS NULL THEN
        CASE WHEN 1 THEN
            subq_1.c16
        ELSE
            subq_1.c16
        END
    ELSE
        CASE WHEN 1 THEN
            subq_1.c16
        ELSE
            subq_1.c16
        END
    END AS c2,
    subq_1.c18 AS c3
FROM (
    SELECT
        subq_0.c0 AS c0,
        subq_0.c1 AS c1,
        subq_0.c1 AS c2,
        subq_0.c0 AS c3,
        subq_0.c1 AS c4,
        subq_0.c1 AS c5,
        80 AS c6,
        subq_0.c0 AS c7,
        subq_0.c1 AS c8,
        subq_0.c1 AS c9,
        96 AS c10,
        subq_0.c0 AS c11,
        subq_0.c0 AS c12,
        subq_0.c0 AS c13,
        subq_0.c0 AS c14,
        subq_0.c0 AS c15,
        CAST(coalesce(subq_0.c0, subq_0.c0) AS INTEGER) AS c16,
        subq_0.c1 AS c17,
        subq_0.c1 AS c18,
        subq_0.c0 AS c19,
        subq_0.c1 AS c20,
        subq_0.c1 AS c21
    FROM (
        SELECT
            85 AS c0,
            ref_0.r_comment AS c1
        FROM
            main.region AS ref_0
        WHERE (0)
        OR ((((((((1)
                                    AND ((ref_0.r_regionkey IS NOT NULL)
                                        AND ((1)
                                            OR (1))))
                                AND ((1)
                                    OR (((ref_0.r_regionkey IS NULL)
                                            OR (ref_0.r_name IS NULL))
                                        OR ((EXISTS (
                                                    SELECT
                                                        ref_1.s_address AS c0, ref_0.r_comment AS c1, ref_0.r_name AS c2, 12 AS c3, ref_1.s_acctbal AS c4, ref_0.r_regionkey AS c5, 48 AS c6, 42 AS c7
                                                    FROM
                                                        main.supplier AS ref_1
                                                    WHERE
                                                        1))
                                                AND (1)))))
                                OR ((1)
                                    OR ((ref_0.r_comment IS NULL)
                                        AND (ref_0.r_comment IS NOT NULL))))
                            OR (0))
                        AND (0))
                    OR (92 IS NOT NULL))
                OR (EXISTS (
                        SELECT
                            ref_2.r_regionkey AS c0, ref_0.r_regionkey AS c1, ref_2.r_name AS c2
                        FROM
                            main.region AS ref_2
                        WHERE
                            ref_0.r_comment IS NULL
                        LIMIT 48)))
        LIMIT 111) AS subq_0
WHERE
    subq_0.c1 IS NULL) AS subq_1
WHERE (subq_1.c14 IS NULL)
OR ((0)
    OR (EXISTS (
            SELECT
                ref_3.ps_availqty AS c0, ref_3.ps_suppkey AS c1
            FROM
                main.partsupp AS ref_3
            WHERE
                ref_3.ps_comment IS NULL)))
LIMIT 53
