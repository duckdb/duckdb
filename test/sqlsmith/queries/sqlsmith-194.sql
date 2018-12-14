SELECT
    ref_0.c_comment AS c0,
    79 AS c1,
    CASE WHEN (ref_0.c_phone IS NULL)
        OR (ref_0.c_name IS NULL) THEN
        CASE WHEN 0 THEN
            (
                SELECT
                    s_nationkey
                FROM
                    main.supplier
                LIMIT 1 offset 5)
        ELSE
            (
                SELECT
                    s_nationkey
                FROM
                    main.supplier
                LIMIT 1 offset 5)
        END
    ELSE
        CASE WHEN 0 THEN
            (
                SELECT
                    s_nationkey
                FROM
                    main.supplier
                LIMIT 1 offset 5)
        ELSE
            (
                SELECT
                    s_nationkey
                FROM
                    main.supplier
                LIMIT 1 offset 5)
        END
    END AS c2,
    ref_0.c_comment AS c3
FROM
    main.customer AS ref_0
WHERE ((1)
    AND (1))
AND (ref_0.c_address IS NULL)
LIMIT 142
