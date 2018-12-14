SELECT
    subq_0.c0 AS c0,
    subq_0.c0 AS c1,
    subq_0.c0 AS c2,
    subq_0.c1 AS c3
FROM (
    SELECT
        ref_2.p_partkey AS c0,
        ref_4.r_name AS c1,
        ref_6.r_name AS c2,
        ref_1.c_custkey AS c3
    FROM
        main.partsupp AS ref_0
        INNER JOIN main.customer AS ref_1
        RIGHT JOIN main.part AS ref_2
        INNER JOIN main.region AS ref_3 ON (ref_2.p_mfgr = ref_3.r_name) ON ((ref_3.r_comment IS NULL)
                AND (1))
            INNER JOIN main.region AS ref_4
            INNER JOIN main.orders AS ref_5
            INNER JOIN main.region AS ref_6 ON (ref_6.r_name IS NULL) ON (ref_4.r_comment IS NULL) ON (ref_1.c_name = ref_5.o_orderstatus) ON (ref_0.ps_comment IS NOT NULL)
        WHERE
            EXISTS (
                SELECT
                    ref_4.r_comment AS c0, ref_7.o_orderstatus AS c1
                FROM
                    main.orders AS ref_7
                WHERE
                    ref_3.r_name IS NOT NULL
                LIMIT 143)
        LIMIT 55) AS subq_0
WHERE
    subq_0.c0 IS NULL
LIMIT 104
