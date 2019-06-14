SELECT
    subq_1.c2 AS c0,
    subq_1.c1 AS c1
FROM (
    SELECT
        ref_0.l_tax AS c0,
        subq_0.c4 AS c1,
        ref_0.l_extendedprice AS c2
    FROM
        main.lineitem AS ref_0
    LEFT JOIN (
        SELECT
            ref_1.n_regionkey AS c0,
            ref_1.n_comment AS c1,
            ref_1.n_name AS c2,
            90 AS c3,
            ref_1.n_regionkey AS c4,
            ref_1.n_name AS c5,
            ref_1.n_name AS c6,
            (
                SELECT
                    o_comment
                FROM
                    main.orders
                LIMIT 1 offset 6) AS c7
        FROM
            main.nation AS ref_1
        WHERE
            EXISTS (
                SELECT
                    ref_2.s_address AS c0, ref_2.s_phone AS c1, 92 AS c2, ref_1.n_comment AS c3
                FROM
                    main.supplier AS ref_2
                WHERE
                    ref_1.n_comment IS NULL
                LIMIT 86)) AS subq_0 ON ((EXISTS (
                    SELECT
                        ref_0.l_commitdate AS c0
                    FROM
                        main.lineitem AS ref_3
                    WHERE
                        ref_3.l_suppkey IS NULL
                    LIMIT 159))
            OR (subq_0.c0 IS NULL))
    WHERE
        EXISTS (
            SELECT
                ref_0.l_comment AS c0
            FROM
                main.lineitem AS ref_4
            WHERE ((ref_0.l_commitdate IS NULL)
                OR (subq_0.c3 IS NULL))
            AND (1)
        LIMIT 134)
LIMIT 111) AS subq_1
WHERE ((subq_1.c0 IS NOT NULL)
    OR (subq_1.c0 IS NOT NULL))
    OR ((1)
        AND (subq_1.c2 IS NOT NULL))
LIMIT 99
