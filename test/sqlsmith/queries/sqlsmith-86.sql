SELECT
    ref_3.s_comment AS c0
FROM
    main.region AS ref_0
    INNER JOIN main.region AS ref_1
    LEFT JOIN main.region AS ref_2
    RIGHT JOIN main.supplier AS ref_3 ON (ref_3.s_comment IS NOT NULL) ON (ref_1.r_name = ref_2.r_name)
    LEFT JOIN (
        SELECT
            ref_4.ps_comment AS c0,
            ref_4.ps_availqty AS c1,
            72 AS c2,
            ref_4.ps_availqty AS c3,
            ref_4.ps_supplycost AS c4,
            ref_4.ps_suppkey AS c5,
            ref_4.ps_supplycost AS c6,
            ref_4.ps_partkey AS c7,
            ref_4.ps_partkey AS c8,
            24 AS c9,
            ref_4.ps_supplycost AS c10,
            84 AS c11,
            ref_4.ps_supplycost AS c12,
            ref_4.ps_suppkey AS c13,
            ref_4.ps_supplycost AS c14
        FROM
            main.partsupp AS ref_4
        WHERE
            1
        LIMIT 65) AS subq_0 ON (subq_0.c3 IS NOT NULL) ON (ref_0.r_comment = ref_3.s_name)
WHERE (1)
OR ((ref_3.s_phone IS NULL)
    AND (ref_3.s_phone IS NOT NULL))
LIMIT 54
