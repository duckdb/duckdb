SELECT
    CAST(nullif ((
            SELECT
                ps_suppkey FROM main.partsupp
            LIMIT 1 offset 2), ref_1.ps_suppkey) AS INTEGER) AS c0,
    ref_0.p_retailprice AS c1,
    ref_1.ps_supplycost AS c2,
    ref_0.p_container AS c3,
    ref_1.ps_comment AS c4
FROM
    main.part AS ref_0
    INNER JOIN main.partsupp AS ref_1 ON (ref_0.p_partkey = ref_1.ps_partkey)
WHERE
    ref_0.p_retailprice IS NOT NULL
LIMIT 101
