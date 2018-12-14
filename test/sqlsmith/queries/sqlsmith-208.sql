SELECT
    ref_0.o_orderpriority AS c0,
    ref_0.o_orderstatus AS c1,
    ref_0.o_shippriority AS c2,
    35 AS c3,
    ref_0.o_orderpriority AS c4,
    ref_0.o_clerk AS c5,
    ref_0.o_orderstatus AS c6,
    ref_0.o_shippriority AS c7,
    ref_0.o_orderdate AS c8,
    ref_0.o_orderstatus AS c9,
    ref_0.o_comment AS c10,
    ref_0.o_orderpriority AS c11,
    ref_0.o_orderstatus AS c12,
    ref_0.o_orderkey AS c13,
    CAST(coalesce(ref_0.o_orderkey, CASE WHEN 1 THEN
                ref_0.o_shippriority
            ELSE
                ref_0.o_shippriority
            END) AS INTEGER) AS c14
FROM
    main.orders AS ref_0
WHERE
    ref_0.o_comment IS NOT NULL
LIMIT 100
