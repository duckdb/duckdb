SELECT
    subq_0.c0 AS c0,
    subq_0.c0 AS c1,
    subq_0.c0 AS c2,
    35 AS c3,
    CASE WHEN ((0)
            AND (1))
        OR ((subq_0.c0 IS NULL)
            OR (subq_0.c0 IS NOT NULL)) THEN
        subq_0.c0
    ELSE
        subq_0.c0
    END AS c4,
    subq_0.c0 AS c5,
    subq_0.c0 AS c6,
    subq_0.c0 AS c7
FROM (
    SELECT
        CAST(coalesce(ref_3.l_comment, ref_1.o_orderstatus) AS VARCHAR) AS c0
    FROM
        main.nation AS ref_0
        INNER JOIN main.orders AS ref_1
        LEFT JOIN main.nation AS ref_2
        RIGHT JOIN main.lineitem AS ref_3 ON (((1)
                    AND (ref_2.n_comment IS NULL))
                AND ((ref_3.l_returnflag IS NULL)
                    OR (1))) ON (((1)
                    OR (ref_2.n_regionkey IS NULL))
                OR (ref_2.n_name IS NOT NULL)) ON (ref_0.n_name = ref_1.o_orderstatus)
    WHERE
        ref_1.o_comment IS NOT NULL) AS subq_0
WHERE
    subq_0.c0 IS NULL
