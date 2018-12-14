SELECT
    CASE WHEN ref_0.n_regionkey IS NOT NULL THEN
        CASE WHEN (1)
            OR ((0)
                OR (1)) THEN
            ref_0.n_comment
        ELSE
            ref_0.n_comment
        END
    ELSE
        CASE WHEN (1)
            OR ((0)
                OR (1)) THEN
            ref_0.n_comment
        ELSE
            ref_0.n_comment
        END
    END AS c0,
    ref_0.n_nationkey AS c1,
    ref_0.n_regionkey AS c2,
    CASE WHEN 1 THEN
        ref_0.n_nationkey
    ELSE
        ref_0.n_nationkey
    END AS c3,
    ref_0.n_regionkey AS c4
FROM
    main.nation AS ref_0
WHERE
    ref_0.n_comment IS NOT NULL
LIMIT 74
