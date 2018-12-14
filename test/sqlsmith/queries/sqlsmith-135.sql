SELECT
    subq_0.c1 AS c0,
    subq_0.c5 AS c1
FROM (
    SELECT
        ref_2.n_comment AS c0,
        ref_1.p_container AS c1,
        ref_1.p_comment AS c2,
        (
            SELECT
                ps_supplycost
            FROM
                main.partsupp
            LIMIT 1 offset 6) AS c3,
        ref_0.ps_partkey AS c4,
        ref_1.p_retailprice AS c5
    FROM
        main.partsupp AS ref_0
    LEFT JOIN main.part AS ref_1
    RIGHT JOIN main.nation AS ref_2 ON (ref_1.p_name IS NULL) ON (ref_0.ps_availqty = ref_2.n_nationkey)
WHERE (
    SELECT
        ps_supplycost
    FROM
        main.partsupp
    LIMIT 1 offset 4)
IS NULL
LIMIT 90) AS subq_0
WHERE
    subq_0.c1 IS NULL
