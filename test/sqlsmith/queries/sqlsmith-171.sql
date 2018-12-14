SELECT
    subq_1.c0 AS c0,
    subq_1.c0 AS c1,
    subq_1.c0 AS c2,
    subq_1.c0 AS c3,
    subq_1.c0 AS c4,
    subq_1.c0 AS c5,
    subq_1.c0 AS c6,
    subq_1.c0 AS c7,
    71 AS c8,
    subq_1.c0 AS c9,
    subq_1.c0 AS c10,
    subq_1.c0 AS c11
FROM (
    SELECT
        subq_0.c1 AS c0
    FROM
        main.part AS ref_0
        INNER JOIN (
            SELECT
                ref_1.c_comment AS c0,
                (
                    SELECT
                        n_name
                    FROM
                        main.nation
                    LIMIT 1 offset 4) AS c1,
                ref_1.c_acctbal AS c2
            FROM
                main.customer AS ref_1
            WHERE
                1) AS subq_0 ON (ref_0.p_container = subq_0.c0)
        WHERE (ref_0.p_brand IS NOT NULL)
        OR (1)
    LIMIT 63) AS subq_1
WHERE
    CASE WHEN subq_1.c0 IS NOT NULL THEN
        CASE WHEN ((0)
                AND ((subq_1.c0 IS NOT NULL)
                    OR ((((((0)
                                        OR (subq_1.c0 IS NOT NULL))
                                    AND (1))
                                OR (EXISTS (
                                        SELECT
                                            (
                                                SELECT
                                                    s_address
                                                FROM
                                                    main.supplier
                                                LIMIT 1 offset 2) AS c0,
                                            subq_1.c0 AS c1,
                                            ref_2.ps_availqty AS c2,
                                            subq_1.c0 AS c3,
                                            29 AS c4,
                                            ref_2.ps_comment AS c5,
                                            subq_1.c0 AS c6,
                                            (
                                                SELECT
                                                    c_nationkey
                                                FROM
                                                    main.customer
                                                LIMIT 1 offset 5) AS c7,
                                            subq_1.c0 AS c8,
                                            subq_1.c0 AS c9,
                                            subq_1.c0 AS c10,
                                            subq_1.c0 AS c11,
                                            (
                                                SELECT
                                                    s_address
                                                FROM
                                                    main.supplier
                                                LIMIT 1 offset 13) AS c12,
                                            ref_2.ps_supplycost AS c13,
                                            subq_1.c0 AS c14,
                                            ref_2.ps_supplycost AS c15,
                                            ref_2.ps_comment AS c16,
                                            subq_1.c0 AS c17,
                                            subq_1.c0 AS c18,
                                            subq_1.c0 AS c19,
                                            ref_2.ps_availqty AS c20,
                                            ref_2.ps_supplycost AS c21,
                                            subq_1.c0 AS c22,
                                            ref_2.ps_suppkey AS c23,
                                            subq_1.c0 AS c24,
                                            ref_2.ps_availqty AS c25,
                                            subq_1.c0 AS c26,
                                            ref_2.ps_suppkey AS c27,
                                            ref_2.ps_suppkey AS c28,
                                            subq_1.c0 AS c29,
                                            ref_2.ps_comment AS c30
                                        FROM
                                            main.partsupp AS ref_2
                                        WHERE
                                            1
                                        LIMIT 29)))
                            AND (((0)
                                    OR ((1)
                                        AND ((EXISTS (
                                                    SELECT
                                                        ref_3.r_name AS c0,
                                                        ref_3.r_name AS c1,
                                                        subq_1.c0 AS c2,
                                                        subq_1.c0 AS c3,
                                                        subq_1.c0 AS c4
                                                    FROM
                                                        main.region AS ref_3
                                                    WHERE
                                                        ref_3.r_comment IS NULL
                                                    LIMIT 150))
                                            OR (subq_1.c0 IS NULL))))
                                AND (subq_1.c0 IS NOT NULL)))
                        AND ((subq_1.c0 IS NULL)
                            OR (0)))))
            OR ((((subq_1.c0 IS NULL)
                        OR ((
                                SELECT
                                    p_brand
                                FROM
                                    main.part
                                LIMIT 1 offset 5)
                            IS NOT NULL))
                    OR (subq_1.c0 IS NULL))
                AND (((EXISTS (
                                SELECT
                                    ref_4.ps_suppkey AS c0,
                                    subq_1.c0 AS c1
                                FROM
                                    main.partsupp AS ref_4
                                WHERE ((ref_4.ps_comment IS NULL)
                                    AND ((1)
                                        OR ((1)
                                            OR (1))))
                                OR (1)
                            LIMIT 47))
                    OR (((0)
                            OR (0))
                        OR ((1)
                            AND ((1)
                                OR (subq_1.c0 IS NOT NULL)))))
                AND (12 IS NULL))) THEN
        subq_1.c0
    ELSE
        subq_1.c0
    END
ELSE
    CASE WHEN ((0)
            AND ((subq_1.c0 IS NOT NULL)
                OR ((((((0)
                                    OR (subq_1.c0 IS NOT NULL))
                                AND (1))
                            OR (EXISTS (
                                    SELECT
                                        (
                                            SELECT
                                                s_address
                                            FROM
                                                main.supplier
                                            LIMIT 1 offset 2) AS c0,
                                        subq_1.c0 AS c1,
                                        ref_2.ps_availqty AS c2,
                                        subq_1.c0 AS c3,
                                        29 AS c4,
                                        ref_2.ps_comment AS c5,
                                        subq_1.c0 AS c6,
                                        (
                                            SELECT
                                                c_nationkey
                                            FROM
                                                main.customer
                                            LIMIT 1 offset 5) AS c7,
                                        subq_1.c0 AS c8,
                                        subq_1.c0 AS c9,
                                        subq_1.c0 AS c10,
                                        subq_1.c0 AS c11,
                                        (
                                            SELECT
                                                s_address
                                            FROM
                                                main.supplier
                                            LIMIT 1 offset 13) AS c12,
                                        ref_2.ps_supplycost AS c13,
                                        subq_1.c0 AS c14,
                                        ref_2.ps_supplycost AS c15,
                                        ref_2.ps_comment AS c16,
                                        subq_1.c0 AS c17,
                                        subq_1.c0 AS c18,
                                        subq_1.c0 AS c19,
                                        ref_2.ps_availqty AS c20,
                                        ref_2.ps_supplycost AS c21,
                                        subq_1.c0 AS c22,
                                        ref_2.ps_suppkey AS c23,
                                        subq_1.c0 AS c24,
                                        ref_2.ps_availqty AS c25,
                                        subq_1.c0 AS c26,
                                        ref_2.ps_suppkey AS c27,
                                        ref_2.ps_suppkey AS c28,
                                        subq_1.c0 AS c29,
                                        ref_2.ps_comment AS c30
                                    FROM
                                        main.partsupp AS ref_2
                                    WHERE
                                        1
                                    LIMIT 29)))
                        AND (((0)
                                OR ((1)
                                    AND ((EXISTS (
                                                SELECT
                                                    ref_3.r_name AS c0,
                                                    ref_3.r_name AS c1,
                                                    subq_1.c0 AS c2,
                                                    subq_1.c0 AS c3,
                                                    subq_1.c0 AS c4
                                                FROM
                                                    main.region AS ref_3
                                                WHERE
                                                    ref_3.r_comment IS NULL
                                                LIMIT 150))
                                        OR (subq_1.c0 IS NULL))))
                            AND (subq_1.c0 IS NOT NULL)))
                    AND ((subq_1.c0 IS NULL)
                        OR (0)))))
        OR ((((subq_1.c0 IS NULL)
                    OR ((
                            SELECT
                                p_brand
                            FROM
                                main.part
                            LIMIT 1 offset 5)
                        IS NOT NULL))
                OR (subq_1.c0 IS NULL))
            AND (((EXISTS (
                            SELECT
                                ref_4.ps_suppkey AS c0,
                                subq_1.c0 AS c1
                            FROM
                                main.partsupp AS ref_4
                            WHERE ((ref_4.ps_comment IS NULL)
                                AND ((1)
                                    OR ((1)
                                        OR (1))))
                            OR (1)
                        LIMIT 47))
                OR (((0)
                        OR (0))
                    OR ((1)
                        AND ((1)
                            OR (subq_1.c0 IS NOT NULL)))))
            AND (12 IS NULL))) THEN
    subq_1.c0
ELSE
    subq_1.c0
END
END IS NULL
