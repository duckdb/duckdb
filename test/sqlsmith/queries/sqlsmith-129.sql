SELECT
    ref_0.n_name AS c0,
    ref_0.n_comment AS c1,
    ref_0.n_name AS c2,
    ref_0.n_regionkey AS c3,
    ref_0.n_name AS c4,
    ref_0.n_name AS c5,
    69 AS c6,
    CAST(nullif (ref_0.n_comment, ref_0.n_comment) AS VARCHAR) AS c7,
    ref_0.n_name AS c8,
    ref_0.n_comment AS c9,
    ref_0.n_name AS c10,
    ref_0.n_regionkey AS c11,
    ref_0.n_regionkey AS c12,
    ref_0.n_regionkey AS c13,
    ref_0.n_comment AS c14,
    ref_0.n_name AS c15,
    CASE WHEN ref_0.n_nationkey IS NOT NULL THEN
        CASE WHEN 1 THEN
            ref_0.n_nationkey
        ELSE
            ref_0.n_nationkey
        END
    ELSE
        CASE WHEN 1 THEN
            ref_0.n_nationkey
        ELSE
            ref_0.n_nationkey
        END
    END AS c16,
    ref_0.n_regionkey AS c17,
    ref_0.n_name AS c18,
    ref_0.n_name AS c19,
    ref_0.n_comment AS c20,
    ref_0.n_regionkey AS c21,
    ref_0.n_nationkey AS c22,
    ref_0.n_nationkey AS c23,
    34 AS c24,
    ref_0.n_nationkey AS c25,
    ref_0.n_comment AS c26,
    CAST(coalesce(ref_0.n_regionkey, ref_0.n_regionkey) AS INTEGER) AS c27
FROM
    main.nation AS ref_0
WHERE
    ref_0.n_comment IS NOT NULL
