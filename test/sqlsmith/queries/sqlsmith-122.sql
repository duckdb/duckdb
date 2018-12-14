SELECT
    ref_1.p_retailprice AS c0,
    ref_1.p_type AS c1,
    ref_0.c_custkey AS c2
FROM
    main.customer AS ref_0
    LEFT JOIN main.part AS ref_1
    LEFT JOIN main.partsupp AS ref_2 ON ((1)
            OR (1)) ON (ref_0.c_phone = ref_2.ps_comment)
WHERE ((1)
    OR ((ref_1.p_size IS NOT NULL)
        OR (EXISTS (
                SELECT
                    ref_0.c_nationkey AS c0, ref_2.ps_availqty AS c1, ref_0.c_phone AS c2, ref_3.l_receiptdate AS c3, ref_3.l_commitdate AS c4, ref_0.c_address AS c5, ref_3.l_orderkey AS c6, ref_2.ps_suppkey AS c7, ref_2.ps_partkey AS c8, ref_3.l_linestatus AS c9, ref_2.ps_suppkey AS c10, ref_1.p_mfgr AS c11
                FROM
                    main.lineitem AS ref_3
                WHERE (((ref_0.c_custkey IS NOT NULL)
                        AND (1))
                    AND ((((1)
                                OR (((((ref_0.c_nationkey IS NOT NULL)
                                                OR (ref_2.ps_supplycost IS NOT NULL))
                                            AND ((((ref_2.ps_supplycost IS NOT NULL)
                                                        OR (EXISTS (
                                                                SELECT
                                                                    ref_1.p_name AS c0, ref_3.l_returnflag AS c1, ref_0.c_acctbal AS c2, ref_4.p_mfgr AS c3
                                                                FROM
                                                                    main.part AS ref_4
                                                                WHERE (1)
                                                                OR ((0)
                                                                    AND ((ref_3.l_orderkey IS NULL)
                                                                        OR (1)))
                                                            LIMIT 113)))
                                                OR ((1 IS NOT NULL)
                                                    OR (EXISTS (
                                                            SELECT
                                                                ref_5.ps_comment AS c0,
                                                                ref_1.p_retailprice AS c1,
                                                                ref_5.ps_comment AS c2,
                                                                (
                                                                    SELECT
                                                                        n_comment
                                                                    FROM
                                                                        main.nation
                                                                    LIMIT 1 offset 2) AS c3
                                                            FROM
                                                                main.partsupp AS ref_5
                                                            WHERE (EXISTS (
                                                                    SELECT
                                                                        ref_5.ps_availqty AS c0, ref_2.ps_comment AS c1, ref_2.ps_comment AS c2, ref_1.p_partkey AS c3, ref_0.c_phone AS c4, ref_6.n_regionkey AS c5, (
                                                                            SELECT
                                                                                p_size
                                                                            FROM
                                                                                main.part
                                                                            LIMIT 1 offset 3) AS c6,
                                                                        ref_2.ps_partkey AS c7,
                                                                        ref_0.c_phone AS c8,
                                                                        ref_0.c_comment AS c9,
                                                                        ref_0.c_address AS c10
                                                                    FROM
                                                                        main.nation AS ref_6
                                                                    WHERE
                                                                        0
                                                                    LIMIT 115))
                                                            OR (ref_3.l_linestatus IS NULL)
                                                        LIMIT 140))))
                                        AND (((1)
                                                OR (0))
                                            OR ((1)
                                                OR (((0)
                                                        AND ((
                                                                SELECT
                                                                    l_commitdate
                                                                FROM
                                                                    main.lineitem
                                                                LIMIT 1 offset 5)
                                                            IS NULL))
                                                    OR ((
                                                            SELECT
                                                                l_comment
                                                            FROM
                                                                main.lineitem
                                                            LIMIT 1 offset 5)
                                                        IS NULL))))))
                                OR (ref_3.l_extendedprice IS NULL))
                            OR (((0)
                                    AND (EXISTS (
                                            SELECT
                                                ref_1.p_type AS c0,
                                                ref_1.p_type AS c1,
                                                ref_0.c_address AS c2,
                                                ref_2.ps_supplycost AS c3
                                            FROM
                                                main.nation AS ref_7
                                            WHERE
                                                0
                                            LIMIT 36)))
                                OR (1))))
                    OR ((ref_3.l_quantity IS NULL)
                        OR (0)))
                AND ((1)
                    AND (1))))
        AND ((ref_1.p_name IS NOT NULL)
            AND (ref_3.l_orderkey IS NOT NULL))))))
OR ((((((ref_2.ps_partkey IS NULL)
                    AND (ref_2.ps_suppkey IS NULL))
                OR ((((EXISTS (
                                    SELECT
                                        ref_0.c_acctbal AS c0,
                                        ref_1.p_size AS c1,
                                        ref_1.p_container AS c2,
                                        ref_8.n_comment AS c3,
                                        ref_0.c_acctbal AS c4,
                                        ref_2.ps_comment AS c5
                                    FROM
                                        main.nation AS ref_8
                                    WHERE
                                        1))
                                OR (ref_0.c_acctbal IS NULL))
                            OR (1))
                        OR ((((ref_1.p_name IS NULL)
                                    AND (0))
                                AND (0))
                            AND (EXISTS (
                                    SELECT
                                        ref_0.c_comment AS c0, ref_1.p_retailprice AS c1
                                    FROM
                                        main.part AS ref_9
                                    WHERE ((((0)
                                                AND ((ref_0.c_nationkey IS NOT NULL)
                                                    OR (ref_0.c_nationkey IS NOT NULL)))
                                            OR ((0)
                                                OR (1)))
                                        OR (0))
                                    OR (((EXISTS (
                                                    SELECT
                                                        ref_2.ps_partkey AS c0, ref_2.ps_comment AS c1, ref_9.p_partkey AS c2, ref_2.ps_suppkey AS c3, ref_1.p_name AS c4, ref_0.c_custkey AS c5, ref_1.p_partkey AS c6, ref_9.p_container AS c7, ref_1.p_name AS c8, ref_1.p_name AS c9, ref_10.ps_suppkey AS c10, ref_10.ps_suppkey AS c11, ref_2.ps_suppkey AS c12, ref_0.c_mktsegment AS c13, ref_10.ps_availqty AS c14, ref_9.p_size AS c15
                                                    FROM
                                                        main.partsupp AS ref_10
                                                    WHERE
                                                        EXISTS (
                                                            SELECT
                                                                ref_10.ps_suppkey AS c0, ref_2.ps_comment AS c1
                                                            FROM
                                                                main.nation AS ref_11
                                                            WHERE ((1)
                                                                AND (EXISTS (
                                                                        SELECT
                                                                            ref_9.p_comment AS c0, ref_12.s_name AS c1, ref_1.p_mfgr AS c2, ref_10.ps_suppkey AS c3, ref_11.n_regionkey AS c4, ref_12.s_nationkey AS c5
                                                                        FROM
                                                                            main.supplier AS ref_12
                                                                        WHERE ((1)
                                                                            OR (1))
                                                                        OR (1)
                                                                    LIMIT 155)))
                                                        OR (0))))
                                            OR (0))
                                        OR (ref_0.c_nationkey IS NULL))
                                LIMIT 62)))))
            AND ((1)
                AND ((
                        SELECT
                            l_shipmode
                        FROM
                            main.lineitem
                        LIMIT 1 offset 21)
                    IS NULL)))
        AND ((((ref_1.p_comment IS NULL)
                    OR (1))
                OR (1))
            OR ((((EXISTS (
                                SELECT
                                    ref_13.n_regionkey AS c0,
                                    ref_13.n_regionkey AS c1,
                                    ref_1.p_partkey AS c2,
                                    ref_2.ps_availqty AS c3,
                                    ref_0.c_custkey AS c4,
                                    ref_0.c_custkey AS c5,
                                    ref_1.p_name AS c6,
                                    ref_1.p_comment AS c7
                                FROM
                                    main.nation AS ref_13
                                WHERE
                                    0
                                LIMIT 48))
                        OR (EXISTS (
                                SELECT
                                    ref_1.p_brand AS c0
                                FROM
                                    main.orders AS ref_14
                                WHERE
                                    1
                                LIMIT 178)))
                    AND (ref_2.ps_partkey IS NULL))
                OR (ref_1.p_comment IS NOT NULL))))
    AND ((
            SELECT
                ps_suppkey
            FROM
                main.partsupp
            LIMIT 1 offset 3)
        IS NOT NULL))
