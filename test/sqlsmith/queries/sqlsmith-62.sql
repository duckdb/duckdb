SELECT
    ref_1.o_totalprice AS c0,
    CASE WHEN ref_0.n_comment IS NULL THEN
        ref_1.o_comment
    ELSE
        ref_1.o_comment
    END AS c1,
    ref_0.n_nationkey AS c2,
    ref_1.o_orderdate AS c3,
    ref_1.o_orderstatus AS c4,
    ref_0.n_name AS c5,
    CASE WHEN 1 THEN
        ref_0.n_name
    ELSE
        ref_0.n_name
    END AS c6,
    ref_1.o_shippriority AS c7,
    ref_1.o_shippriority AS c8,
    ref_0.n_name AS c9,
    ref_1.o_totalprice AS c10,
    ref_0.n_regionkey AS c11,
    ref_1.o_orderstatus AS c12,
    CASE WHEN (0)
        OR (0) THEN
        ref_0.n_comment
    ELSE
        ref_0.n_comment
    END AS c13,
    ref_1.o_totalprice AS c14,
    ref_1.o_orderdate AS c15,
    ref_0.n_name AS c16,
    ref_1.o_orderpriority AS c17,
    ref_0.n_comment AS c18,
    ref_1.o_orderstatus AS c19,
    ref_0.n_name AS c20,
    ref_1.o_orderkey AS c21,
    ref_0.n_comment AS c22,
    ref_0.n_nationkey AS c23,
    ref_0.n_comment AS c24,
    CAST(coalesce(ref_1.o_shippriority, CASE WHEN 0 THEN
                CASE WHEN ref_1.o_orderdate IS NOT NULL THEN
                    ref_0.n_nationkey
                ELSE
                    ref_0.n_nationkey
                END
            ELSE
                CASE WHEN ref_1.o_orderdate IS NOT NULL THEN
                    ref_0.n_nationkey
                ELSE
                    ref_0.n_nationkey
                END
            END) AS INTEGER) AS c25,
    ref_1.o_orderkey AS c26,
    ref_1.o_orderkey AS c27,
    ref_0.n_comment AS c28,
    ref_1.o_orderdate AS c29,
    CASE WHEN 0 THEN
        ref_0.n_regionkey
    ELSE
        ref_0.n_regionkey
    END AS c30
FROM
    main.nation AS ref_0
    LEFT JOIN main.orders AS ref_1 ON (ref_0.n_nationkey = ref_1.o_orderkey)
WHERE (ref_1.o_orderdate IS NULL)
OR ((1)
    OR (ref_1.o_comment IS NOT NULL))
LIMIT 67
