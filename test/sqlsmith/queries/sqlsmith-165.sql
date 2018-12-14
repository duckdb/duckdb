SELECT
    subq_0.c2 AS c0,
    subq_0.c6 AS c1,
    subq_0.c6 AS c2,
    subq_0.c0 AS c3,
    subq_0.c5 AS c4,
    subq_0.c4 AS c5,
    79 AS c6,
    100 AS c7,
    CASE WHEN (
            SELECT
                c_comment
            FROM
                main.customer
            LIMIT 1 offset 3)
    IS NOT NULL THEN
    CASE WHEN 7 IS NOT NULL THEN
        subq_0.c2
    ELSE
        subq_0.c2
    END
ELSE
    CASE WHEN 7 IS NOT NULL THEN
        subq_0.c2
    ELSE
        subq_0.c2
    END
END AS c8
FROM (
    SELECT
        ref_0.s_address AS c0,
        ref_0.s_acctbal AS c1,
        ref_0.s_name AS c2,
        ref_0.s_suppkey AS c3,
        ref_0.s_nationkey AS c4,
        ref_0.s_address AS c5,
        ref_0.s_phone AS c6,
        ref_0.s_acctbal AS c7
    FROM
        main.supplier AS ref_0
    WHERE ((ref_0.s_nationkey IS NULL)
        AND ((0)
            OR (ref_0.s_name IS NULL)))
    OR ((((ref_0.s_acctbal IS NULL)
                AND (0))
            AND ((0)
                AND (ref_0.s_address IS NOT NULL)))
        OR ((EXISTS (
                    SELECT
                        ref_1.s_suppkey AS c0
                    FROM
                        main.supplier AS ref_1
                    WHERE
                        1
                    LIMIT 87))
            OR ((1)
                AND (ref_0.s_acctbal IS NOT NULL))))
LIMIT 22) AS subq_0
WHERE
    EXISTS (
        SELECT
            subq_0.c0 AS c0, subq_0.c7 AS c1
        FROM
            main.nation AS ref_2
        WHERE
            subq_0.c6 IS NOT NULL
        LIMIT 181)
LIMIT 170
