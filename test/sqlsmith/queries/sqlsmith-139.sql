SELECT
    98 AS c0,
    (
        SELECT
            ps_supplycost
        FROM
            main.partsupp
        LIMIT 1 offset 1) AS c1,
    subq_0.c3 AS c2,
    CAST(nullif (subq_0.c0, subq_0.c1) AS VARCHAR) AS c3,
    CASE WHEN (subq_0.c1 IS NULL)
        AND ((subq_0.c4 IS NULL)
            OR ((1)
                OR (((0)
                        OR (1))
                    AND (((((subq_0.c4 IS NULL)
                                    AND ((((1)
                                                OR ((0)
                                                    AND (1)))
                                            AND (1))
                                        AND (1)))
                                AND (0))
                            OR ((((((0)
                                                AND (subq_0.c2 IS NOT NULL))
                                            OR (EXISTS (
                                                    SELECT
                                                        subq_0.c4 AS c0,
                                                        subq_0.c4 AS c1,
                                                        ref_1.s_comment AS c2,
                                                        subq_0.c0 AS c3,
                                                        55 AS c4
                                                    FROM
                                                        main.supplier AS ref_1
                                                    WHERE
                                                        1
                                                    LIMIT 114)))
                                        AND (EXISTS (
                                                SELECT
                                                    ref_2.s_address AS c0,
                                                    ref_2.s_suppkey AS c1,
                                                    ref_2.s_acctbal AS c2,
                                                    ref_2.s_nationkey AS c3
                                                FROM
                                                    main.supplier AS ref_2
                                                WHERE
                                                    subq_0.c3 IS NULL
                                                LIMIT 110)))
                                    OR (subq_0.c0 IS NOT NULL))
                                OR (0)))
                        OR (((((0)
                                        OR (EXISTS (
                                                SELECT
                                                    ref_3.n_name AS c0,
                                                    ref_3.n_name AS c1,
                                                    subq_0.c4 AS c2,
                                                    (
                                                        SELECT
                                                            l_returnflag
                                                        FROM
                                                            main.lineitem
                                                        LIMIT 1 offset 6) AS c3,
                                                    ref_3.n_nationkey AS c4,
                                                    ref_3.n_comment AS c5,
                                                    subq_0.c3 AS c6,
                                                    ref_3.n_comment AS c7
                                                FROM
                                                    main.nation AS ref_3
                                                WHERE
                                                    subq_0.c0 IS NOT NULL
                                                LIMIT 124)))
                                    AND (((0)
                                            AND (0))
                                        OR (0)))
                                OR (subq_0.c4 IS NULL))
                            OR (subq_0.c0 IS NULL)))))) THEN
        subq_0.c4
    ELSE
        subq_0.c4
    END AS c4,
    subq_0.c1 AS c5,
    subq_0.c0 AS c6,
    CAST(coalesce(subq_0.c3, subq_0.c1) AS VARCHAR) AS c7,
        subq_0.c0 AS c8,
        (
            SELECT
                p_container
            FROM
                main.part
            LIMIT 1 offset 1) AS c9,
        CAST(coalesce(subq_0.c2, subq_0.c2) AS INTEGER) AS c10,
    subq_0.c2 AS c11,
    subq_0.c3 AS c12,
    subq_0.c4 AS c13,
    subq_0.c0 AS c14,
    subq_0.c3 AS c15,
    CASE WHEN (subq_0.c2 IS NULL)
        AND (1) THEN
        subq_0.c3
    ELSE
        subq_0.c3
    END AS c16,
    subq_0.c1 AS c17,
    subq_0.c1 AS c18,
    subq_0.c2 AS c19,
    subq_0.c2 AS c20,
    subq_0.c3 AS c21,
    subq_0.c2 AS c22,
    CASE WHEN subq_0.c3 IS NULL THEN
        CASE WHEN 1 THEN
            subq_0.c2
        ELSE
            subq_0.c2
        END
    ELSE
        CASE WHEN 1 THEN
            subq_0.c2
        ELSE
            subq_0.c2
        END
    END AS c23,
    subq_0.c4 AS c24,
    subq_0.c1 AS c25,
    subq_0.c4 AS c26,
    subq_0.c1 AS c27,
    26 AS c28,
    subq_0.c2 AS c29,
    subq_0.c1 AS c30,
    (
        SELECT
            o_custkey
        FROM
            main.orders
        LIMIT 1 offset 6) AS c31,
    subq_0.c2 AS c32,
    subq_0.c2 AS c33,
    subq_0.c3 AS c34,
    subq_0.c0 AS c35,
    subq_0.c2 AS c36
FROM (
    SELECT
        ref_0.r_comment AS c0,
        (
            SELECT
                l_comment
            FROM
                main.lineitem
            LIMIT 1 offset 3) AS c1,
        (
            SELECT
                l_quantity
            FROM
                main.lineitem
            LIMIT 1 offset 1) AS c2,
        ref_0.r_comment AS c3,
        ref_0.r_comment AS c4
    FROM
        main.region AS ref_0
    WHERE
        ref_0.r_comment IS NULL) AS subq_0
WHERE (subq_0.c0 IS NULL)
OR (subq_0.c4 IS NOT NULL)
LIMIT 106
