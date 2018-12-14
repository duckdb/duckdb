SELECT
    subq_1.c4 AS c0,
    subq_1.c3 AS c1,
    subq_1.c2 AS c2,
    subq_0.c0 AS c3,
    (
        SELECT
            p_mfgr
        FROM
            main.part
        LIMIT 1 offset 2) AS c4,
    subq_1.c3 AS c5,
    subq_0.c4 AS c6,
    subq_1.c3 AS c7
FROM (
    SELECT
        ref_2.p_brand AS c0,
        ref_1.n_regionkey AS c1,
        ref_1.n_comment AS c2,
        ref_0.c_address AS c3,
        ref_0.c_address AS c4,
        ref_0.c_name AS c5
    FROM
        main.customer AS ref_0
    RIGHT JOIN main.nation AS ref_1
    LEFT JOIN main.part AS ref_2 ON ((1)
            OR ((ref_2.p_retailprice IS NOT NULL)
                AND (1))) ON (ref_0.c_address = ref_1.n_name)
WHERE ((ref_0.c_comment IS NULL)
    OR ((((17 IS NOT NULL)
                AND ((0)
                    AND ((EXISTS (
                                SELECT
                                    ref_3.p_partkey AS c0, ref_2.p_name AS c1, ref_3.p_mfgr AS c2, ref_2.p_partkey AS c3, ref_1.n_nationkey AS c4, ref_3.p_name AS c5
                                FROM
                                    main.part AS ref_3
                                WHERE
                                    0
                                LIMIT 61))
                        OR (ref_2.p_container IS NOT NULL))))
            AND (EXISTS (
                    SELECT
                        91 AS c0,
                        ref_2.p_name AS c1,
                        ref_4.ps_partkey AS c2,
                        ref_1.n_nationkey AS c3,
                        ref_4.ps_comment AS c4,
                        ref_2.p_name AS c5,
                        19 AS c6,
                        ref_1.n_regionkey AS c7,
                        ref_4.ps_supplycost AS c8,
                        (
                            SELECT
                                p_type
                            FROM
                                main.part
                            LIMIT 1 offset 5) AS c9,
                        ref_0.c_mktsegment AS c10,
                        ref_1.n_nationkey AS c11,
                        ref_1.n_nationkey AS c12
                    FROM
                        main.partsupp AS ref_4
                    WHERE
                        1
                    LIMIT 171)))
        OR (((1)
                OR (0))
            OR (ref_2.p_container IS NULL))))
AND ((ref_0.c_nationkey IS NULL)
    AND (1))
LIMIT 180) AS subq_0
    LEFT JOIN (
        SELECT
            ref_5.l_discount AS c0,
            ref_5.l_comment AS c1,
            ref_5.l_tax AS c2,
            ref_5.l_linenumber AS c3,
            ref_5.l_linestatus AS c4,
            ref_5.l_linestatus AS c5
        FROM
            main.lineitem AS ref_5
        WHERE ((((62 IS NULL)
                    AND (1))
                AND (ref_5.l_shipinstruct IS NOT NULL))
            AND ((0)
                AND ((ref_5.l_tax IS NOT NULL)
                    OR (((EXISTS (
                                    SELECT
                                        ref_5.l_tax AS c0, ref_5.l_shipmode AS c1, (
                                            SELECT
                                                o_shippriority
                                            FROM
                                                main.orders
                                            LIMIT 1 offset 5) AS c2,
                                        ref_6.r_comment AS c3,
                                        ref_5.l_returnflag AS c4,
                                        ref_6.r_name AS c5
                                    FROM
                                        main.region AS ref_6
                                    WHERE
                                        0
                                    LIMIT 54))
                            AND (ref_5.l_linestatus IS NULL))
                        AND (0)))))
        AND ((0)
            AND (ref_5.l_receiptdate IS NOT NULL))
    LIMIT 105) AS subq_1 ON ((subq_0.c2 IS NOT NULL)
        OR (((subq_0.c5 IS NULL)
                AND (subq_0.c5 IS NULL))
            OR (subq_0.c1 IS NOT NULL)))
WHERE (subq_1.c5 IS NULL)
AND (1)
LIMIT 99
