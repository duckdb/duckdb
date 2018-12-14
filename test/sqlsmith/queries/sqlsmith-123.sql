SELECT
    subq_0.c1 AS c0,
    subq_0.c3 AS c1,
    CAST(coalesce(
            CASE WHEN 1 THEN
                CAST(nullif (subq_0.c3, subq_0.c3) AS VARCHAR)
            ELSE
                CAST(nullif (subq_0.c3, subq_0.c3) AS VARCHAR)
            END, subq_0.c3) AS VARCHAR) AS c2,
    subq_0.c2 AS c3,
    subq_0.c3 AS c4,
    82 AS c5,
    subq_0.c3 AS c6,
    subq_0.c0 AS c7,
    CAST(coalesce((
                SELECT
                    s_phone FROM main.supplier
                LIMIT 1 offset 4), CASE WHEN subq_0.c3 IS NOT NULL THEN
                subq_0.c2
            ELSE
                subq_0.c2
            END) AS VARCHAR) AS c8,
    27 AS c9
FROM (
    SELECT
        ref_0.r_regionkey AS c0,
        ref_0.r_regionkey AS c1,
        ref_0.r_comment AS c2,
        ref_0.r_name AS c3
    FROM
        main.region AS ref_0
    WHERE (0)
    OR (EXISTS (
            SELECT
                ref_0.r_regionkey AS c0, (
                    SELECT
                        o_shippriority
                    FROM
                        main.orders
                    LIMIT 1 offset 2) AS c1,
                ref_0.r_comment AS c2,
                ref_1.s_acctbal AS c3,
                15 AS c4
            FROM
                main.supplier AS ref_1
            WHERE
                ref_0.r_comment IS NULL))
    LIMIT 89) AS subq_0
WHERE
    subq_0.c0 IS NOT NULL
LIMIT 105
