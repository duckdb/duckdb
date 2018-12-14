SELECT
    subq_0.c6 AS c0,
    subq_0.c7 AS c1,
    CASE WHEN ((1)
            AND (EXISTS (
                    SELECT
                        (
                            SELECT
                                r_comment
                            FROM
                                main.region
                            LIMIT 1 offset 2) AS c0,
                        ref_1.o_totalprice AS c1,
                        ref_1.o_comment AS c2,
                        ref_1.o_clerk AS c3,
                        ref_1.o_comment AS c4,
                        (
                            SELECT
                                s_nationkey
                            FROM
                                main.supplier
                            LIMIT 1 offset 85) AS c5,
                        ref_1.o_orderdate AS c6,
                        subq_0.c3 AS c7
                    FROM
                        main.orders AS ref_1
                    WHERE (ref_1.o_comment IS NULL)
                    AND ((subq_0.c4 IS NOT NULL)
                        AND (1))
                LIMIT 24)))
    OR (((subq_0.c8 IS NOT NULL)
            OR (subq_0.c3 IS NOT NULL))
        AND ((0)
            OR (EXISTS (
                    SELECT
                        subq_0.c5 AS c0,
                        55 AS c1,
                        ref_2.n_regionkey AS c2,
                        subq_0.c3 AS c3,
                        ref_2.n_comment AS c4,
                        ref_2.n_nationkey AS c5,
                        ref_2.n_name AS c6,
                        subq_0.c8 AS c7,
                        subq_0.c9 AS c8,
                        subq_0.c8 AS c9,
                        subq_0.c1 AS c10,
                        subq_0.c0 AS c11,
                        ref_2.n_regionkey AS c12,
                        ref_2.n_comment AS c13,
                        subq_0.c2 AS c14,
                        subq_0.c3 AS c15,
                        subq_0.c9 AS c16,
                        subq_0.c9 AS c17
                    FROM
                        main.nation AS ref_2
                    WHERE
                        subq_0.c7 IS NOT NULL
                    LIMIT 106)))) THEN
    subq_0.c1
ELSE
    subq_0.c1
END AS c2,
subq_0.c4 AS c3,
subq_0.c0 AS c4,
subq_0.c0 AS c5,
subq_0.c6 AS c6,
subq_0.c8 AS c7,
(
    SELECT
        n_regionkey
    FROM
        main.nation
    LIMIT 1 offset 1) AS c8,
    subq_0.c3 AS c9,
    subq_0.c2 AS c10,
    14 AS c11,
    subq_0.c6 AS c12,
    subq_0.c2 AS c13,
    subq_0.c1 AS c14,
    subq_0.c0 AS c15,
    subq_0.c6 AS c16,
    subq_0.c2 AS c17,
    subq_0.c2 AS c18,
    subq_0.c4 AS c19,
    subq_0.c2 AS c20,
    subq_0.c1 AS c21,
    CASE WHEN CASE WHEN 1 THEN
            subq_0.c4
        ELSE
            subq_0.c4
        END IS NOT NULL THEN
        subq_0.c1
    ELSE
        subq_0.c1
    END AS c22
FROM (
    SELECT
        ref_0.o_comment AS c0,
        ref_0.o_orderdate AS c1,
        25 AS c2,
        ref_0.o_custkey AS c3,
        CASE WHEN ref_0.o_comment IS NULL THEN
            ref_0.o_custkey
        ELSE
            ref_0.o_custkey
        END AS c4,
        ref_0.o_orderkey AS c5,
        ref_0.o_shippriority AS c6,
        58 AS c7,
        ref_0.o_clerk AS c8,
        (
            SELECT
                s_address
            FROM
                main.supplier
            LIMIT 1 offset 5) AS c9
    FROM
        main.orders AS ref_0
    WHERE
        1
    LIMIT 144) AS subq_0
WHERE
    subq_0.c8 IS NULL
