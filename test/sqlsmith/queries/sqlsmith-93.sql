SELECT
    ref_0.c_name AS c0,
    ref_0.c_custkey AS c1
FROM
    main.customer AS ref_0
WHERE (ref_0.c_phone IS NULL)
AND (((ref_0.c_nationkey IS NULL)
        OR (1))
    OR (EXISTS (
            SELECT
                DISTINCT ref_3.s_address AS c0, ref_3.s_name AS c1, ref_2.o_custkey AS c2
            FROM
                main.region AS ref_1
            RIGHT JOIN main.orders AS ref_2
            RIGHT JOIN main.supplier AS ref_3 ON (ref_3.s_address IS NOT NULL) ON (ref_1.r_name = ref_2.o_orderstatus)
        WHERE (EXISTS (
                SELECT
                    ref_3.s_acctbal AS c0, ref_0.c_comment AS c1, 10 AS c2
                FROM
                    main.supplier AS ref_4
                WHERE
                    ref_1.r_comment IS NULL
                LIMIT 88))
        OR (1)
    LIMIT 114)))
LIMIT 169
