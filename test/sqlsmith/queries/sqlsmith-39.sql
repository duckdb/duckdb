SELECT
    subq_0.c4 AS c0,
    CASE WHEN subq_0.c6 IS NOT NULL THEN
        CASE WHEN (
            SELECT
                ps_supplycost
            FROM
                main.partsupp
            LIMIT 1 offset 3) IS NULL THEN
            subq_0.c8
        ELSE
            subq_0.c8
        END
    ELSE
        CASE WHEN (
            SELECT
                ps_supplycost
            FROM
                main.partsupp
            LIMIT 1 offset 3) IS NULL THEN
            subq_0.c8
        ELSE
            subq_0.c8
        END
    END AS c1,
    subq_0.c1 AS c2
FROM (
    SELECT
        ref_0.o_orderdate AS c0,
        ref_0.o_clerk AS c1,
        ref_0.o_comment AS c2,
        ref_0.o_orderdate AS c3,
        ref_0.o_orderkey AS c4,
        ref_0.o_comment AS c5,
        ref_0.o_custkey AS c6,
        ref_0.o_orderdate AS c7,
        ref_0.o_custkey AS c8,
        ref_0.o_totalprice AS c9,
        ref_0.o_orderdate AS c10,
        ref_0.o_custkey AS c11,
        ref_0.o_orderstatus AS c12,
        ref_0.o_orderstatus AS c13,
        ref_0.o_clerk AS c14,
        (
            SELECT
                s_comment
            FROM
                main.supplier
            LIMIT 1 offset 3) AS c15,
        ref_0.o_orderkey AS c16,
        ref_0.o_custkey AS c17,
        ref_0.o_comment AS c18
    FROM
        main.orders AS ref_0
    WHERE (1)
    AND (ref_0.o_orderstatus IS NULL)
LIMIT 119) AS subq_0
WHERE (EXISTS (
        SELECT
            subq_0.c10 AS c0, subq_0.c12 AS c1
        FROM
            main.partsupp AS ref_1
        WHERE
            1
        LIMIT 32))
AND ((((0)
            AND (subq_0.c16 IS NULL))
        OR ((subq_0.c10 IS NULL)
            OR ((EXISTS (
                        SELECT
                            subq_0.c10 AS c0,
                            subq_0.c14 AS c1
                        FROM
                            main.orders AS ref_2
                        WHERE
                            subq_0.c4 IS NULL
                        LIMIT 105))
                AND (subq_0.c8 IS NOT NULL))))
    OR ((subq_0.c14 IS NOT NULL)
        AND (subq_0.c1 IS NULL)))
