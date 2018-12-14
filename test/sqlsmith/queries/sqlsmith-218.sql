SELECT
    ref_3.ps_comment AS c0,
    ref_3.ps_partkey AS c1
FROM (
    SELECT
        ref_2.p_mfgr AS c0,
        ref_1.s_name AS c1,
        ref_2.p_type AS c2,
        ref_2.p_retailprice AS c3
    FROM
        main.nation AS ref_0
    RIGHT JOIN main.supplier AS ref_1
    RIGHT JOIN main.part AS ref_2 ON (ref_1.s_nationkey IS NOT NULL) ON (ref_0.n_name = ref_1.s_name)
WHERE
    ref_0.n_nationkey IS NOT NULL
LIMIT 19) AS subq_0
    INNER JOIN main.partsupp AS ref_3 ON (subq_0.c3 = ref_3.ps_supplycost)
WHERE
    subq_0.c0 IS NULL
LIMIT 109
