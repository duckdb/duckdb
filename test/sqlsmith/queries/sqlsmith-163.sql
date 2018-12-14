SELECT
    ref_0.c_custkey AS c0,
    ref_0.c_custkey AS c1,
    ref_0.c_address AS c2,
    ref_0.c_name AS c3,
    ref_0.c_phone AS c4,
    CAST(nullif (60, ref_0.c_custkey) AS INTEGER) AS c5,
    ref_0.c_acctbal AS c6,
    ref_0.c_phone AS c7,
    CAST(coalesce(ref_0.c_nationkey, ref_0.c_custkey) AS INTEGER) AS c8,
    ref_0.c_mktsegment AS c9,
    ref_0.c_name AS c10,
    ref_0.c_nationkey AS c11,
    ref_0.c_nationkey AS c12,
    ref_0.c_nationkey AS c13,
    ref_0.c_name AS c14,
    ref_0.c_comment AS c15,
    ref_0.c_name AS c16,
    ref_0.c_comment AS c17,
    CASE WHEN CASE WHEN 0 THEN
            ref_0.c_nationkey
        ELSE
            ref_0.c_nationkey
        END IS NOT NULL THEN
        ref_0.c_phone
    ELSE
        ref_0.c_phone
    END AS c18,
    CASE WHEN (ref_0.c_comment IS NULL)
        OR (((((EXISTS (
                                SELECT
                                    ref_1.n_comment AS c0,
                                    ref_1.n_name AS c1
                                FROM
                                    main.nation AS ref_1
                                WHERE ((1)
                                    AND (ref_1.n_regionkey IS NULL))
                                OR (EXISTS (
                                        SELECT
                                            ref_1.n_name AS c0, ref_1.n_name AS c1, ref_0.c_nationkey AS c2, ref_1.n_comment AS c3, ref_0.c_mktsegment AS c4, ref_2.s_comment AS c5, ref_1.n_regionkey AS c6, ref_2.s_phone AS c7
                                        FROM
                                            main.supplier AS ref_2
                                        WHERE (ref_0.c_comment IS NOT NULL)
                                        AND (0)))
                            LIMIT 21))
                    OR (EXISTS (
                            SELECT
                                ref_0.c_nationkey AS c0,
                                ref_3.o_custkey AS c1,
                                ref_3.o_clerk AS c2
                            FROM
                                main.orders AS ref_3
                            WHERE
                                0
                            LIMIT 87)))
                OR ((0)
                    OR (EXISTS (
                            SELECT
                                ref_0.c_mktsegment AS c0,
                                7 AS c1,
                                67 AS c2
                            FROM
                                main.orders AS ref_4
                            WHERE
                                0))))
                AND (EXISTS (
                        SELECT
                            ref_5.c_address AS c0, ref_5.c_phone AS c1, ref_6.c_name AS c2, ref_5.c_custkey AS c3, ref_6.c_address AS c4, ref_6.c_address AS c5
                        FROM
                            main.customer AS ref_5
                            INNER JOIN main.customer AS ref_6 ON (ref_5.c_comment = ref_6.c_name)
                        WHERE ((ref_5.c_phone IS NULL)
                            OR (1))
                        OR (1)
                    LIMIT 24)))
        OR (((ref_0.c_mktsegment IS NOT NULL)
                AND (0))
            OR (((1)
                    OR ((((1)
                                OR (EXISTS (
                                        SELECT
                                            ref_7.l_shipinstruct AS c0,
                                            ref_7.l_shipinstruct AS c1,
                                            ref_7.l_linenumber AS c2
                                        FROM
                                            main.lineitem AS ref_7
                                        WHERE
                                            EXISTS (
                                                SELECT
                                                    ref_8.ps_availqty AS c0, ref_8.ps_supplycost AS c1
                                                FROM
                                                    main.partsupp AS ref_8
                                                WHERE
                                                    0)
                                            LIMIT 103)))
                                OR (1))
                            AND ((EXISTS (
                                        SELECT
                                            ref_9.l_shipinstruct AS c0,
                                            ref_0.c_phone AS c1,
                                            ref_0.c_nationkey AS c2
                                        FROM
                                            main.lineitem AS ref_9
                                        WHERE
                                            1))
                                    AND (ref_0.c_nationkey IS NOT NULL))))
                        OR (((99 IS NULL)
                                OR (((1)
                                        AND (((EXISTS (
                                                        SELECT
                                                            ref_10.p_comment AS c0, ref_10.p_mfgr AS c1, ref_0.c_name AS c2, ref_0.c_custkey AS c3, ref_10.p_name AS c4, ref_10.p_container AS c5, ref_10.p_container AS c6, 90 AS c7, ref_0.c_acctbal AS c8, 75 AS c9, 54 AS c10
                                                        FROM
                                                            main.part AS ref_10
                                                        WHERE
                                                            0))
                                                    AND (0))
                                                OR ((((1)
                                                            AND ((1)
                                                                AND (ref_0.c_address IS NULL)))
                                                        AND (EXISTS (
                                                                SELECT
                                                                    ref_0.c_mktsegment AS c0, ref_11.c_phone AS c1
                                                                FROM
                                                                    main.customer AS ref_11
                                                                WHERE ((0)
                                                                    OR (0))
                                                                AND (1))))
                                                    OR (1))))
                                        AND (1)))
                                AND (1))))) THEN
                ref_0.c_comment
            ELSE
                ref_0.c_comment
            END AS c19, ref_0.c_nationkey AS c20
        FROM
            main.customer AS ref_0
        WHERE
            ref_0.c_comment IS NOT NULL
        LIMIT 101
