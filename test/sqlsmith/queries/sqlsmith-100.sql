SELECT
    subq_0.c0 AS c0,
    subq_0.c1 AS c1,
    subq_0.c1 AS c2,
    16 AS c3,
    subq_0.c0 AS c4,
    subq_0.c1 AS c5,
    subq_0.c0 AS c6
FROM (
    SELECT
        ref_2.n_nationkey AS c0,
        ref_2.n_name AS c1,
        ref_1.ps_suppkey AS c2,
        ref_2.n_comment AS c3
    FROM
        main.part AS ref_0
        INNER JOIN main.partsupp AS ref_1
        LEFT JOIN main.nation AS ref_2 ON (ref_2.n_nationkey IS NOT NULL) ON (ref_0.p_brand = ref_1.ps_comment)
    WHERE
        EXISTS (
            SELECT
                ref_1.ps_availqty AS c0, ref_3.s_comment AS c1, 15 AS c2, ref_4.ps_suppkey AS c3, ref_1.ps_supplycost AS c4, ref_1.ps_comment AS c5, ref_4.ps_suppkey AS c6, ref_2.n_nationkey AS c7, ref_0.p_comment AS c8, ref_3.s_acctbal AS c9
            FROM
                main.supplier AS ref_3
                INNER JOIN main.partsupp AS ref_4 ON (ref_3.s_suppkey = ref_4.ps_partkey)
            WHERE
                1
            LIMIT 137)) AS subq_0
WHERE ((subq_0.c3 IS NOT NULL)
    AND (1))
OR (subq_0.c3 IS NOT NULL)
