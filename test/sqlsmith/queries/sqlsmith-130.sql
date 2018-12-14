SELECT
    subq_0.c0 AS c0,
    subq_0.c0 AS c1
FROM (
    SELECT
        ref_1.r_name AS c0
    FROM
        main.customer AS ref_0
        INNER JOIN main.region AS ref_1
        RIGHT JOIN main.region AS ref_2 ON (ref_2.r_regionkey IS NOT NULL) ON (ref_0.c_name = ref_1.r_name)
        INNER JOIN main.nation AS ref_3 ON (ref_0.c_nationkey = ref_3.n_nationkey)
    WHERE (EXISTS (
            SELECT
                ref_3.n_name AS c0, ref_2.r_comment AS c1, ref_3.n_name AS c2, (
                    SELECT
                        p_container
                    FROM
                        main.part
                    LIMIT 1 offset 1) AS c3,
                ref_3.n_regionkey AS c4,
                ref_2.r_comment AS c5,
                ref_1.r_name AS c6,
                (
                    SELECT
                        l_extendedprice
                    FROM
                        main.lineitem
                    LIMIT 1 offset 2) AS c7,
                ref_0.c_acctbal AS c8,
                ref_1.r_name AS c9,
                ref_2.r_comment AS c10,
                ref_1.r_name AS c11,
                ref_1.r_regionkey AS c12,
                ref_3.n_name AS c13,
                ref_1.r_name AS c14,
                ref_1.r_comment AS c15,
                ref_3.n_regionkey AS c16,
                (
                    SELECT
                        p_mfgr
                    FROM
                        main.part
                    LIMIT 1 offset 1) AS c17,
                ref_2.r_name AS c18,
                ref_1.r_name AS c19,
                ref_4.l_shipdate AS c20,
                ref_4.l_comment AS c21,
                ref_1.r_regionkey AS c22,
                ref_1.r_name AS c23,
                ref_2.r_regionkey AS c24,
                ref_2.r_regionkey AS c25
            FROM
                main.lineitem AS ref_4
            WHERE
                ref_3.n_nationkey IS NULL))
        AND (
            CASE WHEN 40 IS NOT NULL THEN
                ref_0.c_nationkey
            ELSE
                ref_0.c_nationkey
            END IS NOT NULL)) AS subq_0
WHERE (
    SELECT
        o_orderstatus
    FROM
        main.orders
    LIMIT 1 offset 24)
    IS NULL
