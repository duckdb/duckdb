SELECT
    (
        SELECT
            ps_supplycost
        FROM
            main.partsupp
        LIMIT 1 offset 1) AS c0,
    ref_0.l_comment AS c1
FROM
    main.lineitem AS ref_0
WHERE ((ref_0.l_suppkey IS NOT NULL)
    AND (((34 IS NOT NULL)
            AND ((((ref_0.l_discount IS NULL)
                        AND (ref_0.l_receiptdate IS NULL))
                    OR ((ref_0.l_linestatus IS NULL)
                        OR (ref_0.l_linestatus IS NOT NULL)))
                OR (ref_0.l_linestatus IS NULL)))
        AND ((EXISTS (
                    SELECT
                        ref_1.p_container AS c0
                    FROM
                        main.part AS ref_1
                    LEFT JOIN main.part AS ref_2
                    INNER JOIN main.nation AS ref_3 ON ((ref_2.p_size IS NULL)
                            OR (1)) ON (ref_1.p_brand = ref_2.p_name)
                WHERE ((ref_2.p_partkey IS NOT NULL)
                    OR (1))
                AND (EXISTS (
                        SELECT
                            ref_3.n_regionkey AS c0, ref_4.p_container AS c1, ref_2.p_partkey AS c2, ref_3.n_nationkey AS c3, ref_4.p_brand AS c4, ref_4.p_container AS c5, 90 AS c6, ref_1.p_container AS c7, 42 AS c8, ref_0.l_shipmode AS c9
                        FROM
                            main.part AS ref_4
                        WHERE
                            EXISTS (
                                SELECT
                                    ref_2.p_retailprice AS c0, ref_0.l_returnflag AS c1, ref_2.p_type AS c2, ref_2.p_container AS c3, ref_1.p_comment AS c4, ref_5.s_nationkey AS c5
                                FROM
                                    main.supplier AS ref_5
                                WHERE
                                    1
                                LIMIT 79)
                        LIMIT 69))
            LIMIT 154))
    OR (0))))
OR (ref_0.l_orderkey IS NULL)
LIMIT 168
