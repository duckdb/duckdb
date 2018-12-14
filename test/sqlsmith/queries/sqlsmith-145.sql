SELECT
    ref_0.n_comment AS c0,
    ref_0.n_comment AS c1,
    CAST(coalesce(
            CASE WHEN 0 THEN
                ref_0.n_comment
            ELSE
                ref_0.n_comment
            END, ref_0.n_name) AS VARCHAR) AS c2,
    ref_0.n_nationkey AS c3,
    (
        SELECT
            o_orderstatus
        FROM
            main.orders
        LIMIT 1 offset 1) AS c4,
    ref_0.n_name AS c5,
    ref_0.n_name AS c6,
    ref_0.n_nationkey AS c7,
    ref_0.n_regionkey AS c8
FROM
    main.nation AS ref_0
WHERE
    ref_0.n_comment IS NOT NULL
