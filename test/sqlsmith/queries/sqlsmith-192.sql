SELECT
    CASE WHEN subq_0.c4 IS NOT NULL THEN
        subq_0.c2
    ELSE
        subq_0.c2
    END AS c0,
    subq_0.c3 AS c1,
    subq_0.c4 AS c2,
    subq_0.c0 AS c3,
    subq_0.c4 AS c4,
    subq_0.c2 AS c5,
    subq_0.c1 AS c6
FROM (
    SELECT
        ref_2.c_mktsegment AS c0,
        ref_3.p_container AS c1,
        ref_2.c_nationkey AS c2,
        ref_3.p_mfgr AS c3,
        ref_2.c_custkey AS c4,
        ref_1.l_extendedprice AS c5
    FROM
        main.supplier AS ref_0
        INNER JOIN main.lineitem AS ref_1
        RIGHT JOIN main.customer AS ref_2 ON (ref_2.c_comment IS NULL) ON (ref_0.s_address = ref_1.l_returnflag)
        INNER JOIN main.part AS ref_3
        INNER JOIN main.region AS ref_4 ON (ref_3.p_partkey IS NOT NULL) ON (ref_2.c_address = ref_3.p_name)
    WHERE
        CASE WHEN 0 THEN
            ref_1.l_returnflag
        ELSE
            ref_1.l_returnflag
        END IS NULL
    LIMIT 132) AS subq_0
WHERE (subq_0.c4 IS NOT NULL)
AND (subq_0.c5 IS NULL)
LIMIT 137
