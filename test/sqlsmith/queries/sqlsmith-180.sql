SELECT
    ref_0.p_brand AS c0,
    ref_0.p_size AS c1,
    ref_0.p_retailprice AS c2,
    ref_0.p_type AS c3,
    CAST(nullif (
            CASE WHEN 0 THEN
                ref_0.p_partkey
            ELSE
                ref_0.p_partkey
            END, ref_0.p_partkey) AS INTEGER) AS c4,
    ref_0.p_name AS c5,
    ref_0.p_retailprice AS c6,
    ref_0.p_comment AS c7,
    ref_0.p_container AS c8,
    ref_0.p_mfgr AS c9,
    ref_0.p_partkey AS c10,
    ref_0.p_container AS c11,
    CASE WHEN (ref_0.p_container IS NULL)
        AND (ref_0.p_size IS NULL) THEN
        ref_0.p_mfgr
    ELSE
        ref_0.p_mfgr
    END AS c12
FROM
    main.part AS ref_0
WHERE
    EXISTS (
        SELECT
            93 AS c0
        FROM
            main.supplier AS ref_1
        WHERE
            EXISTS (
                SELECT
                    ref_6.c_comment AS c0, ref_4.l_linestatus AS c1
                FROM
                    main.region AS ref_2
                LEFT JOIN main.orders AS ref_3
                INNER JOIN main.lineitem AS ref_4 ON (56 IS NOT NULL) ON (ref_2.r_comment = ref_3.o_orderstatus)
                INNER JOIN main.region AS ref_5
                INNER JOIN main.customer AS ref_6 ON (ref_5.r_name = ref_6.c_name) ON (((0)
                            AND (1))
                        OR (1))
            WHERE (ref_0.p_retailprice IS NULL)
            AND (ref_1.s_name IS NOT NULL)
        LIMIT 26)
LIMIT 97)
LIMIT 160
