SELECT
    subq_0.c0 AS c0,
    subq_0.c0 AS c1,
    CASE WHEN ((subq_0.c0 IS NOT NULL)
            OR (0))
        OR ((subq_0.c0 IS NOT NULL)
            AND (subq_0.c0 IS NULL)) THEN
        subq_0.c0
    ELSE
        subq_0.c0
    END AS c2,
    subq_0.c0 AS c3,
    subq_0.c0 AS c4,
    subq_0.c0 AS c5,
    subq_0.c0 AS c6,
    3 AS c7,
    2 AS c8,
    CASE WHEN 1 THEN
        subq_0.c0
    ELSE
        subq_0.c0
    END AS c9,
    subq_0.c0 AS c10,
    subq_0.c0 AS c11
FROM (
    SELECT
        ref_2.p_size AS c0
    FROM
        main.region AS ref_0
        INNER JOIN main.orders AS ref_1
        INNER JOIN main.part AS ref_2 ON (ref_2.p_size IS NOT NULL) ON (ref_0.r_comment = ref_1.o_orderstatus)
    WHERE
        ref_0.r_name IS NULL
    LIMIT 31) AS subq_0
WHERE (((0)
        AND (subq_0.c0 IS NOT NULL))
    AND (1))
OR ((EXISTS (
            SELECT
                subq_0.c0 AS c0, ref_3.o_totalprice AS c1
            FROM
                main.orders AS ref_3
                INNER JOIN main.customer AS ref_4 ON ((0)
                        AND (0))
                LEFT JOIN main.nation AS ref_5 ON (((ref_4.c_mktsegment IS NULL)
                            AND (ref_5.n_regionkey IS NULL))
                        AND ((0)
                            AND (ref_4.c_acctbal IS NULL)))
            WHERE ((1)
                OR (1))
            OR (((0)
                    AND ((1)
                        OR (((ref_4.c_name IS NULL)
                                AND ((0)
                                    OR (subq_0.c0 IS NULL)))
                            OR (subq_0.c0 IS NOT NULL))))
                OR (EXISTS (
                        SELECT
                            subq_0.c0 AS c0
                        FROM
                            main.partsupp AS ref_6
                        WHERE
                            0
                        LIMIT 86)))))
    OR (subq_0.c0 IS NULL))
LIMIT 98
