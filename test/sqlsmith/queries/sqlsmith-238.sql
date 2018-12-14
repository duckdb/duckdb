SELECT
    ref_0.n_regionkey AS c0,
    ref_0.n_comment AS c1,
    ref_0.n_nationkey AS c2,
    ref_0.n_comment AS c3,
    (
        SELECT
            o_clerk
        FROM
            main.orders
        LIMIT 1 offset 5) AS c4,
    CAST(coalesce(ref_0.n_regionkey, ref_0.n_nationkey) AS INTEGER) AS c5,
    ref_0.n_nationkey AS c6,
    CASE WHEN CASE WHEN 1 THEN
            ref_0.n_nationkey
        ELSE
            ref_0.n_nationkey
        END IS NULL THEN
        ref_0.n_nationkey
    ELSE
        ref_0.n_nationkey
    END AS c7,
    ref_0.n_name AS c8,
    ref_0.n_name AS c9,
    ref_0.n_name AS c10,
    ref_0.n_nationkey AS c11,
    ref_0.n_regionkey AS c12,
    ref_0.n_regionkey AS c13,
    ref_0.n_name AS c14,
    ref_0.n_nationkey AS c15,
    ref_0.n_name AS c16,
    ref_0.n_regionkey AS c17,
    ref_0.n_nationkey AS c18,
    ref_0.n_nationkey AS c19,
    ref_0.n_comment AS c20,
    ref_0.n_regionkey AS c21,
    6 AS c22
FROM
    main.nation AS ref_0
WHERE
    ref_0.n_comment IS NOT NULL
LIMIT 112
