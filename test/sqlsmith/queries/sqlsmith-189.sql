SELECT
    subq_0.c2 AS c0,
    subq_0.c3 AS c1
FROM (
    SELECT
        ref_3.l_linenumber AS c0,
        ref_1.s_phone AS c1,
        ref_2.s_comment AS c2,
        ref_0.s_nationkey AS c3,
        ref_0.s_phone AS c4,
        ref_1.s_name AS c5,
        ref_1.s_nationkey AS c6,
        ref_4.o_orderdate AS c7,
        ref_2.s_name AS c8
    FROM
        main.supplier AS ref_0
    LEFT JOIN main.supplier AS ref_1
    LEFT JOIN main.supplier AS ref_2
    INNER JOIN main.lineitem AS ref_3
    INNER JOIN main.orders AS ref_4 ON ((ref_4.o_orderstatus IS NULL)
            OR ((1)
                AND (ref_4.o_orderkey IS NULL))) ON (((((0)
                        AND (0))
                    AND (0))
                OR ((1)
                    OR ((0)
                        OR (41 IS NOT NULL))))
            AND (ref_2.s_address IS NULL)) ON (ref_1.s_name IS NULL) ON (ref_0.s_comment = ref_2.s_name)
WHERE
    ref_3.l_returnflag IS NOT NULL) AS subq_0
WHERE
    subq_0.c6 IS NOT NULL
LIMIT 20
