SELECT
    ref_0.n_nationkey AS c0,
    ref_0.n_comment AS c1,
    ref_0.n_comment AS c2,
    ref_0.n_regionkey AS c3,
    CAST(coalesce((
                SELECT
                    ps_suppkey FROM main.partsupp
                LIMIT 1 offset 6), CASE WHEN 0 THEN
                ref_0.n_nationkey
            ELSE
                ref_0.n_nationkey
            END) AS INTEGER) AS c4,
    ref_0.n_nationkey AS c5
FROM
    main.nation AS ref_0
WHERE
    ref_0.n_comment IS NULL
