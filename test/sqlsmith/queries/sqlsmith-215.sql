SELECT
    CAST(nullif (subq_0.c0, subq_0.c7) AS DECIMAL) AS c0,
    CASE WHEN subq_0.c1 IS NOT NULL THEN
        CASE WHEN ((subq_0.c1 IS NULL)
                AND ((((((subq_0.c4 IS NULL)
                                    OR (0))
                                OR (EXISTS (
                                        SELECT
                                            ref_3.r_regionkey AS c0,
                                            subq_0.c3 AS c1,
                                            subq_0.c3 AS c2,
                                            43 AS c3,
                                            subq_0.c0 AS c4,
                                            subq_0.c5 AS c5,
                                            subq_0.c7 AS c6,
                                            13 AS c7,
                                            subq_0.c6 AS c8,
                                            subq_0.c5 AS c9,
                                            subq_0.c6 AS c10,
                                            subq_0.c5 AS c11,
                                            ref_3.r_comment AS c12,
                                            subq_0.c1 AS c13,
                                            ref_3.r_regionkey AS c14,
                                            subq_0.c1 AS c15,
                                            ref_3.r_name AS c16,
                                            ref_3.r_name AS c17,
                                            ref_3.r_comment AS c18,
                                            ref_3.r_regionkey AS c19,
                                            ref_3.r_comment AS c20,
                                            ref_3.r_regionkey AS c21,
                                            subq_0.c8 AS c22,
                                            ref_3.r_name AS c23,
                                            subq_0.c1 AS c24,
                                            ref_3.r_name AS c25,
                                            subq_0.c8 AS c26,
                                            subq_0.c5 AS c27
                                        FROM
                                            main.region AS ref_3
                                        WHERE
                                            subq_0.c6 IS NULL
                                        LIMIT 23)))
                            AND (1))
                        AND (1))
                    AND (1)))
            OR (subq_0.c7 IS NULL) THEN
            CASE WHEN 0 THEN
                CAST(nullif (subq_0.c5, subq_0.c5) AS VARCHAR)
            ELSE
                CAST(nullif (subq_0.c5, subq_0.c5) AS VARCHAR)
            END
        ELSE
            CASE WHEN 0 THEN
                CAST(nullif (subq_0.c5, subq_0.c5) AS VARCHAR)
            ELSE
                CAST(nullif (subq_0.c5, subq_0.c5) AS VARCHAR)
            END
        END
    ELSE
        CASE WHEN ((subq_0.c1 IS NULL)
                AND ((((((subq_0.c4 IS NULL)
                                    OR (0))
                                OR (EXISTS (
                                        SELECT
                                            ref_3.r_regionkey AS c0,
                                            subq_0.c3 AS c1,
                                            subq_0.c3 AS c2,
                                            43 AS c3,
                                            subq_0.c0 AS c4,
                                            subq_0.c5 AS c5,
                                            subq_0.c7 AS c6,
                                            13 AS c7,
                                            subq_0.c6 AS c8,
                                            subq_0.c5 AS c9,
                                            subq_0.c6 AS c10,
                                            subq_0.c5 AS c11,
                                            ref_3.r_comment AS c12,
                                            subq_0.c1 AS c13,
                                            ref_3.r_regionkey AS c14,
                                            subq_0.c1 AS c15,
                                            ref_3.r_name AS c16,
                                            ref_3.r_name AS c17,
                                            ref_3.r_comment AS c18,
                                            ref_3.r_regionkey AS c19,
                                            ref_3.r_comment AS c20,
                                            ref_3.r_regionkey AS c21,
                                            subq_0.c8 AS c22,
                                            ref_3.r_name AS c23,
                                            subq_0.c1 AS c24,
                                            ref_3.r_name AS c25,
                                            subq_0.c8 AS c26,
                                            subq_0.c5 AS c27
                                        FROM
                                            main.region AS ref_3
                                        WHERE
                                            subq_0.c6 IS NULL
                                        LIMIT 23)))
                            AND (1))
                        AND (1))
                    AND (1)))
            OR (subq_0.c7 IS NULL) THEN
            CASE WHEN 0 THEN
                CAST(nullif (subq_0.c5, subq_0.c5) AS VARCHAR)
            ELSE
                CAST(nullif (subq_0.c5, subq_0.c5) AS VARCHAR)
            END
        ELSE
            CASE WHEN 0 THEN
                CAST(nullif (subq_0.c5, subq_0.c5) AS VARCHAR)
            ELSE
                CAST(nullif (subq_0.c5, subq_0.c5) AS VARCHAR)
            END
        END
    END AS c1,
    subq_0.c5 AS c2,
    subq_0.c6 AS c3,
    subq_0.c7 AS c4,
    subq_0.c6 AS c5,
    subq_0.c1 AS c6
FROM (
    SELECT
        ref_0.o_totalprice AS c0,
        ref_0.o_totalprice AS c1,
        82 AS c2,
        ref_0.o_custkey AS c3,
        ref_0.o_orderstatus AS c4,
        ref_0.o_orderstatus AS c5,
        ref_0.o_orderkey AS c6,
        ref_0.o_totalprice AS c7,
        ref_0.o_orderkey AS c8
    FROM
        main.orders AS ref_0
    WHERE
        CASE WHEN ((EXISTS (
                        SELECT
                            ref_0.o_orderkey AS c0
                        FROM
                            main.region AS ref_1
                        WHERE
                            40 IS NULL
                        LIMIT 180))
                AND (0))
            AND (EXISTS (
                    SELECT
                        ref_2.r_regionkey AS c0,
                        ref_2.r_comment AS c1,
                        ref_2.r_comment AS c2,
                        ref_0.o_orderkey AS c3
                    FROM
                        main.region AS ref_2
                    WHERE (1)
                    OR (1))) THEN
            ref_0.o_comment
        ELSE
            ref_0.o_comment
        END IS NULL) AS subq_0
WHERE (subq_0.c7 IS NULL)
OR (1)
LIMIT 143
