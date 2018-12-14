SELECT
    subq_0.c0 AS c0,
    subq_0.c2 AS c1,
    subq_0.c2 AS c2,
    subq_0.c2 AS c3,
    (
        SELECT
            n_comment
        FROM
            main.nation
        LIMIT 1 offset 5) AS c4,
    subq_0.c1 AS c5,
    subq_0.c1 AS c6,
    CAST(nullif (subq_0.c2, subq_0.c2) AS INTEGER) AS c7,
    subq_0.c2 AS c8,
    subq_0.c1 AS c9,
    subq_0.c3 AS c10,
    subq_0.c1 AS c11,
    subq_0.c1 AS c12,
    subq_0.c0 AS c13,
    subq_0.c1 AS c14,
    subq_0.c0 AS c15,
    subq_0.c1 AS c16
FROM (
    SELECT
        ref_2.l_shipmode AS c0,
        ref_1.p_mfgr AS c1,
        ref_2.l_quantity AS c2,
        ref_3.p_retailprice AS c3
    FROM
        main.lineitem AS ref_0
        INNER JOIN main.part AS ref_1
        INNER JOIN main.lineitem AS ref_2
        INNER JOIN main.part AS ref_3 ON (ref_2.l_partkey = ref_3.p_partkey)
        RIGHT JOIN main.orders AS ref_4 ON ((ref_4.o_comment IS NOT NULL)
                AND (ref_3.p_container IS NULL)) ON (ref_2.l_discount IS NOT NULL) ON (ref_0.l_shipdate = ref_2.l_shipdate)
    WHERE
        EXISTS (
            SELECT
                ref_5.l_shipdate AS c0, ref_3.p_container AS c1, ref_4.o_clerk AS c2, ref_1.p_size AS c3
            FROM
                main.lineitem AS ref_5
            WHERE (ref_0.l_receiptdate IS NULL)
            OR (EXISTS (
                    SELECT
                        91 AS c0, ref_6.ps_suppkey AS c1, ref_0.l_linenumber AS c2, ref_1.p_brand AS c3, ref_0.l_suppkey AS c4, ref_5.l_comment AS c5, ref_6.ps_suppkey AS c6, ref_3.p_size AS c7, ref_0.l_linenumber AS c8, ref_6.ps_availqty AS c9, ref_4.o_shippriority AS c10
                    FROM
                        main.partsupp AS ref_6
                    WHERE (EXISTS (
                            SELECT
                                (
                                    SELECT
                                        s_acctbal
                                    FROM
                                        main.supplier
                                    LIMIT 1 offset 5) AS c0,
                                ref_6.ps_comment AS c1,
                                ref_7.n_regionkey AS c2,
                                ref_0.l_linestatus AS c3,
                                ref_1.p_size AS c4,
                                18 AS c5,
                                ref_5.l_shipmode AS c6,
                                ref_1.p_mfgr AS c7,
                                ref_5.l_quantity AS c8,
                                ref_7.n_name AS c9,
                                ref_7.n_name AS c10,
                                ref_3.p_type AS c11,
                                ref_7.n_name AS c12,
                                ref_2.l_receiptdate AS c13,
                                ref_3.p_name AS c14,
                                ref_7.n_name AS c15,
                                ref_0.l_quantity AS c16,
                                ref_5.l_linestatus AS c17,
                                ref_1.p_mfgr AS c18,
                                ref_1.p_comment AS c19,
                                ref_1.p_mfgr AS c20,
                                ref_4.o_totalprice AS c21,
                                ref_3.p_comment AS c22,
                                ref_0.l_quantity AS c23,
                                ref_3.p_partkey AS c24,
                                ref_3.p_comment AS c25,
                                ref_7.n_nationkey AS c26
                            FROM
                                main.nation AS ref_7
                            WHERE
                                EXISTS (
                                    SELECT
                                        ref_7.n_nationkey AS c0, ref_3.p_comment AS c1, ref_8.n_name AS c2, ref_6.ps_partkey AS c3, ref_1.p_comment AS c4, ref_0.l_receiptdate AS c5
                                    FROM
                                        main.nation AS ref_8
                                    WHERE
                                        0
                                    LIMIT 80)))
                        AND ((ref_1.p_mfgr IS NOT NULL)
                            OR (EXISTS (
                                    SELECT
                                        ref_5.l_extendedprice AS c0
                                    FROM
                                        main.customer AS ref_9
                                    WHERE
                                        1
                                    LIMIT 146)))))
            LIMIT 123)
    LIMIT 143) AS subq_0
WHERE
    CAST(nullif (subq_0.c1, subq_0.c0) AS VARCHAR)
    IS NULL
