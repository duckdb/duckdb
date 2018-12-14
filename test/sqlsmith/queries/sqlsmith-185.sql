SELECT
    CASE WHEN ref_0.s_address IS NOT NULL THEN
        CASE WHEN 1 THEN
            ref_0.s_nationkey
        ELSE
            ref_0.s_nationkey
        END
    ELSE
        CASE WHEN 1 THEN
            ref_0.s_nationkey
        ELSE
            ref_0.s_nationkey
        END
    END AS c0
FROM
    main.supplier AS ref_0
WHERE
    CASE WHEN (ref_0.s_nationkey IS NOT NULL)
        AND (1) THEN
        ref_0.s_acctbal
    ELSE
        ref_0.s_acctbal
    END IS NOT NULL
