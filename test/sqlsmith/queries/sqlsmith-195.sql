SELECT
    ref_0.o_shippriority AS c0,
    ref_0.o_orderpriority AS c1,
    CASE WHEN ref_0.o_orderkey IS NOT NULL THEN
        ref_0.o_orderkey
    ELSE
        ref_0.o_orderkey
    END AS c2,
    CAST(coalesce(
            CASE WHEN ((3 IS NULL)
                    AND (0))
                OR (0) THEN
                ref_0.o_comment
            ELSE
                ref_0.o_comment
            END, CAST(nullif (ref_0.o_orderstatus, ref_0.o_orderpriority) AS VARCHAR)) AS VARCHAR) AS c3,
    ref_0.o_orderpriority AS c4,
    66 AS c5
FROM
    main.orders AS ref_0
WHERE
    ref_0.o_orderpriority IS NULL
