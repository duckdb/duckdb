SELECT
    CAST(coalesce(ref_0.o_comment, CASE WHEN 0 THEN
                ref_0.o_orderstatus
            ELSE
                ref_0.o_orderstatus
            END) AS VARCHAR) AS c0,
    ref_0.o_clerk AS c1,
    ref_0.o_shippriority AS c2,
    ref_0.o_clerk AS c3
FROM
    main.orders AS ref_0
WHERE
    CAST(nullif (ref_0.o_totalprice, CAST(NULL AS DECIMAL)) AS DECIMAL)
    IS NOT NULL
