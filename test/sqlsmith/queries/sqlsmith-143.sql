SELECT
    ref_0.c_name AS c0,
    CASE WHEN (
            CASE WHEN 0 THEN
                ref_0.c_address
            ELSE
                ref_0.c_address
            END IS NULL)
        OR (1) THEN
        ref_0.c_nationkey
    ELSE
        ref_0.c_nationkey
    END AS c1,
    ref_0.c_custkey AS c2,
    ref_0.c_name AS c3
FROM
    main.customer AS ref_0
WHERE
    ref_0.c_mktsegment IS NULL
