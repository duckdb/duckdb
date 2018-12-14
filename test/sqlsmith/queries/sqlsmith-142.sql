SELECT
    (
        SELECT
            o_orderstatus
        FROM
            main.orders
        LIMIT 1 offset 3) AS c0,
    subq_0.c1 AS c1,
    20 AS c2
FROM (
    SELECT
        ref_1.s_suppkey AS c0,
        ref_0.c_phone AS c1,
        ref_1.s_acctbal AS c2,
        ref_0.c_acctbal AS c3,
        ref_2.l_returnflag AS c4
    FROM
        main.customer AS ref_0
        INNER JOIN main.supplier AS ref_1
        INNER JOIN main.lineitem AS ref_2 ON ((ref_2.l_partkey IS NOT NULL)
                AND (1)) ON (ref_0.c_name = ref_1.s_name)
    WHERE (ref_1.s_nationkey IS NULL)
    OR ((1)
        OR ((EXISTS (
                    SELECT
                        ref_3.n_nationkey AS c0, ref_0.c_nationkey AS c1, ref_2.l_returnflag AS c2, ref_0.c_phone AS c3, ref_2.l_linenumber AS c4, ref_3.n_name AS c5, 96 AS c6, ref_2.l_linenumber AS c7, ref_2.l_orderkey AS c8, ref_0.c_phone AS c9, ref_2.l_commitdate AS c10, ref_3.n_regionkey AS c11, ref_0.c_custkey AS c12, ref_0.c_acctbal AS c13, ref_3.n_regionkey AS c14, ref_3.n_comment AS c15, 22 AS c16
                    FROM
                        main.nation AS ref_3
                    WHERE (
                        SELECT
                            l_partkey
                        FROM
                            main.lineitem
                        LIMIT 1 offset 6)
                    IS NULL))
            OR (EXISTS (
                    SELECT
                        ref_4.r_comment AS c0,
                        ref_4.r_regionkey AS c1,
                        ref_2.l_quantity AS c2,
                        ref_0.c_mktsegment AS c3,
                        68 AS c4
                    FROM
                        main.region AS ref_4
                    WHERE
                        ref_0.c_nationkey IS NULL
                    LIMIT 81))))
LIMIT 146) AS subq_0
WHERE
    subq_0.c0 IS NOT NULL
LIMIT 165
