SELECT
    ref_0.r_regionkey AS c0,
    CAST(coalesce(
            CASE WHEN 1 THEN
                (
                    SELECT
                        o_comment FROM main.orders
                    LIMIT 1 offset 75)
            ELSE
                (
                    SELECT
                        o_comment FROM main.orders
                    LIMIT 1 offset 75)
            END, ref_0.r_comment) AS VARCHAR) AS c1
FROM
    main.region AS ref_0
WHERE
    ref_0.r_name IS NOT NULL
