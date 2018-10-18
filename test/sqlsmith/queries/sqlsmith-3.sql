SELECT
    subq_0.c0 AS c0,
    30 AS c1,
    subq_0.c2 AS c2
FROM (
    SELECT
        (
            SELECT
                s_acctbal
            FROM
                main.supplier
            LIMIT 1 offset 4) AS c0,
        ref_0.p_comment AS c1,
        ref_0.p_brand AS c2,
        ref_0.p_brand AS c3,
        ref_0.p_container AS c4,
        ref_0.p_partkey AS c5,
        ref_0.p_brand AS c6,
        ref_0.p_type AS c7
    FROM
        main.part AS ref_0
    WHERE
        ref_0.p_type IS NULL
    LIMIT 85) AS subq_0
WHERE
    1
LIMIT 89
