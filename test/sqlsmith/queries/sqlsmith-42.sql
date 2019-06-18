SELECT
    CAST(coalesce(subq_0.c1, subq_0.c1) AS VARCHAR) AS c0,
    subq_0.c1 AS c1,
    subq_0.c0 AS c2,
    76 AS c3,
    subq_0.c0 AS c4,
    subq_0.c1 AS c5,
    CASE WHEN 1 THEN
        subq_0.c0
    ELSE
        subq_0.c0
    END AS c6,
    CASE WHEN (((1)
            OR ((subq_0.c3 IS NOT NULL)
                AND (1)))
        AND (((subq_0.c3 IS NULL)
                AND (0))
            OR (EXISTS (
                    SELECT
                        (
                            SELECT
                                p_comment
                            FROM
                                main.part
                            LIMIT 1 offset 1) AS c0
                    FROM
                        main.partsupp AS ref_1
                    WHERE
                        subq_0.c0 IS NOT NULL))))
        AND ((subq_0.c0 IS NOT NULL)
            AND (subq_0.c3 IS NOT NULL)) THEN
        subq_0.c3
    ELSE
        subq_0.c3
    END AS c7,
    subq_0.c1 AS c8
FROM (
    SELECT
        ref_0.o_orderkey AS c0,
        ref_0.o_comment AS c1,
        ref_0.o_custkey AS c2,
        ref_0.o_orderdate AS c3
    FROM
        main.orders AS ref_0
    WHERE
        ref_0.o_orderpriority IS NULL
    LIMIT 153) AS subq_0
WHERE (CAST(nullif (subq_0.c0, subq_0.c2) AS INTEGER) IS NOT NULL)
    OR ((subq_0.c1 IS NOT NULL)
        AND (((((0)
                        AND (subq_0.c1 IS NOT NULL))
                    AND (EXISTS (
                            SELECT
                                subq_0.c1 AS c0,
                                ref_2.o_orderkey AS c1,
                                subq_0.c1 AS c2,
                                subq_0.c3 AS c3
                            FROM
                                main.orders AS ref_2
                            WHERE
                                0
                            LIMIT 48)))
                AND (subq_0.c1 IS NOT NULL))
            OR (((EXISTS (
                            SELECT
                                ref_3.n_name AS c0
                            FROM
                                main.nation AS ref_3
                            WHERE
                                subq_0.c3 IS NOT NULL
                            LIMIT 93))
                    AND (((subq_0.c2 IS NULL)
                            OR (subq_0.c3 IS NOT NULL))
                        OR ((EXISTS (
                                    SELECT
                                        ref_4.o_orderdate AS c0,
                                        ref_4.o_clerk AS c1,
                                        ref_4.o_custkey AS c2,
                                        subq_0.c2 AS c3,
                                        ref_4.o_orderkey AS c4,
                                        subq_0.c0 AS c5,
                                        ref_4.o_clerk AS c6
                                    FROM
                                        main.orders AS ref_4
                                    WHERE
                                        0
                                    LIMIT 121))
                            OR ((0)
                                AND ((subq_0.c0 IS NOT NULL)
                                    OR (1))))))
                OR (1))))
