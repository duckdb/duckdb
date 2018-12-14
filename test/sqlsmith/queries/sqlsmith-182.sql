SELECT
    ref_0.c_address AS c0,
    ref_0.c_comment AS c1,
    CASE WHEN ref_0.c_acctbal IS NULL THEN
        CASE WHEN 10 IS NOT NULL THEN
            ref_0.c_acctbal
        ELSE
            ref_0.c_acctbal
        END
    ELSE
        CASE WHEN 10 IS NOT NULL THEN
            ref_0.c_acctbal
        ELSE
            ref_0.c_acctbal
        END
    END AS c2,
    ref_0.c_address AS c3,
    CAST(coalesce(ref_0.c_phone, ref_0.c_comment) AS VARCHAR) AS c4,
    ref_0.c_nationkey AS c5
FROM
    main.customer AS ref_0
WHERE
    ref_0.c_comment IS NOT NULL
