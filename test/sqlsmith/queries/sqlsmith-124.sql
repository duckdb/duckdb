SELECT
    CAST(coalesce(ref_0.c_custkey, ref_0.c_custkey) AS INTEGER) AS c0,
    ref_0.c_address AS c1,
    CASE WHEN 1 THEN
        ref_0.c_mktsegment
    ELSE
        ref_0.c_mktsegment
    END AS c2
FROM
    main.customer AS ref_0
WHERE
    EXISTS (
        SELECT
            ref_4.r_name AS c0, ref_1.s_comment AS c1, ref_4.r_comment AS c2, ref_1.s_phone AS c3
        FROM
            main.supplier AS ref_1
            INNER JOIN main.region AS ref_2
            INNER JOIN main.lineitem AS ref_3 ON (ref_3.l_shipdate IS NULL)
            INNER JOIN main.region AS ref_4 ON ((ref_3.l_orderkey IS NOT NULL)
                    AND (ref_2.r_name IS NOT NULL)) ON (ref_1.s_suppkey = ref_4.r_regionkey)
        WHERE
            1
        LIMIT 186)
LIMIT 153
