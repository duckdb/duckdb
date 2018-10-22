SELECT
    ref_0.n_comment AS c0,
    CASE WHEN 0 THEN
        ref_0.n_comment
    ELSE
        ref_0.n_comment
    END AS c1
FROM
    main.nation AS ref_0
WHERE
    ref_0.n_comment IS NULL
LIMIT 80
