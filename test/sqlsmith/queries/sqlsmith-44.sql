SELECT
    subq_1.c2 AS c0,
    subq_1.c2 AS c1
FROM (
    SELECT
        subq_0.c4 AS c0,
        subq_0.c2 AS c1,
        CASE WHEN ((0)
            AND (0))
            AND (0) THEN
            subq_0.c0
        ELSE
            subq_0.c0
        END AS c2,
        subq_0.c4 AS c3,
        subq_0.c1 AS c4,
        subq_0.c5 AS c5,
        subq_0.c0 AS c6
    FROM (
        SELECT
            (
                SELECT
                    r_name
                FROM
                    main.region
                LIMIT 1 offset 5) AS c0,
            ref_0.o_orderkey AS c1,
            ref_0.o_orderkey AS c2,
            ref_0.o_orderstatus AS c3,
            ref_0.o_orderpriority AS c4,
            ref_0.o_orderkey AS c5
        FROM
            main.orders AS ref_0
        WHERE
            1
        LIMIT 96) AS subq_0
WHERE
    1
LIMIT 109) AS subq_1
WHERE (
    SELECT
        o_orderstatus
    FROM
        main.orders
    LIMIT 1 offset 53) IS NOT NULL
LIMIT 168
