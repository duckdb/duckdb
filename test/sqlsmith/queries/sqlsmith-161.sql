SELECT
    32 AS c0
FROM (
    SELECT
        ref_0.o_custkey AS c0,
        ref_2.s_name AS c1,
        ref_1.p_name AS c2
    FROM
        main.orders AS ref_0
        INNER JOIN main.part AS ref_1 ON (ref_1.p_name IS NULL)
        INNER JOIN main.supplier AS ref_2
        INNER JOIN main.orders AS ref_3
        INNER JOIN main.lineitem AS ref_4 ON (ref_3.o_totalprice IS NOT NULL) ON (ref_2.s_address = ref_3.o_orderstatus) ON (ref_0.o_comment = ref_3.o_orderstatus)
    WHERE (0)
    AND (0)
LIMIT 100) AS subq_0
    RIGHT JOIN (
        SELECT
            ref_5.p_partkey AS c0,
            ref_5.p_partkey AS c1
        FROM
            main.part AS ref_5
        WHERE
            1
        LIMIT 106) AS subq_1 ON ((subq_1.c0 IS NULL)
        AND ((subq_1.c1 IS NOT NULL)
            OR (subq_1.c1 IS NULL)))
WHERE ((subq_1.c0 IS NOT NULL)
    OR (EXISTS (
            SELECT
                subq_0.c1 AS c0, subq_1.c0 AS c1, subq_1.c0 AS c2, ref_6.p_brand AS c3
            FROM
                main.part AS ref_6
            WHERE (((1)
                    AND (0))
                AND ((subq_1.c1 IS NOT NULL)
                    OR (((0)
                            AND (EXISTS (
                                    SELECT
                                        ref_6.p_mfgr AS c0, subq_0.c0 AS c1, subq_1.c1 AS c2, subq_1.c1 AS c3, ref_6.p_mfgr AS c4, ref_6.p_brand AS c5, subq_1.c1 AS c6, ref_7.p_comment AS c7, ref_7.p_name AS c8, subq_1.c1 AS c9, ref_7.p_retailprice AS c10, subq_1.c0 AS c11, subq_1.c0 AS c12, ref_6.p_name AS c13, subq_1.c0 AS c14, subq_1.c0 AS c15, (
                                            SELECT
                                                ps_comment
                                            FROM
                                                main.partsupp
                                            LIMIT 1 offset 3) AS c16,
                                        subq_1.c0 AS c17,
                                        ref_7.p_comment AS c18,
                                        ref_6.p_size AS c19,
                                        ref_7.p_mfgr AS c20
                                    FROM
                                        main.part AS ref_7
                                    WHERE ((((EXISTS (
                                                        SELECT
                                                            ref_6.p_container AS c0, subq_1.c1 AS c1
                                                        FROM
                                                            main.nation AS ref_8
                                                        WHERE
                                                            subq_0.c2 IS NULL
                                                        LIMIT 90))
                                                AND (((subq_1.c1 IS NULL)
                                                        OR (ref_7.p_partkey IS NULL))
                                                    OR (0)))
                                            AND (1))
                                        AND (ref_7.p_container IS NOT NULL))
                                    OR (EXISTS (
                                            SELECT
                                                subq_1.c1 AS c0,
                                                ref_9.l_comment AS c1
                                            FROM
                                                main.lineitem AS ref_9
                                            WHERE
                                                EXISTS (
                                                    SELECT
                                                        subq_0.c2 AS c0, subq_1.c1 AS c1, subq_1.c1 AS c2, ref_7.p_mfgr AS c3, ref_10.c_comment AS c4, ref_10.c_name AS c5, ref_10.c_comment AS c6, ref_10.c_custkey AS c7, subq_1.c1 AS c8, subq_0.c1 AS c9, (
                                                            SELECT
                                                                c_comment
                                                            FROM
                                                                main.customer
                                                            LIMIT 1 offset 2) AS c10,
                                                        subq_0.c2 AS c11,
                                                        ref_10.c_acctbal AS c12
                                                    FROM
                                                        main.customer AS ref_10
                                                    WHERE (0)
                                                    AND (((0)
                                                            OR (EXISTS (
                                                                    SELECT
                                                                        ref_11.ps_supplycost AS c0, ref_9.l_shipdate AS c1, ref_6.p_size AS c2, ref_7.p_type AS c3, ref_6.p_container AS c4, ref_9.l_extendedprice AS c5
                                                                    FROM
                                                                        main.partsupp AS ref_11
                                                                    WHERE (0)
                                                                    OR (ref_11.ps_availqty IS NOT NULL)
                                                                LIMIT 92)))
                                                    OR (((subq_0.c0 IS NULL)
                                                            AND ((
                                                                    SELECT
                                                                        o_orderpriority
                                                                    FROM
                                                                        main.orders
                                                                    LIMIT 1 offset 3)
                                                                IS NOT NULL))
                                                        OR (EXISTS (
                                                                SELECT
                                                                    subq_1.c0 AS c0,
                                                                    subq_0.c2 AS c1
                                                                FROM
                                                                    main.partsupp AS ref_12
                                                                WHERE (1)
                                                                OR (subq_0.c1 IS NULL)
                                                            LIMIT 84))))
                                        LIMIT 111)))
                        LIMIT 85)))
            OR ((1)
                OR ((0)
                    AND ((ref_6.p_size IS NULL)
                        AND ((EXISTS (
                                    SELECT
                                        ref_13.n_nationkey AS c0
                                    FROM
                                        main.nation AS ref_13
                                    WHERE (EXISTS (
                                            SELECT
                                                ref_13.n_name AS c0, ref_6.p_brand AS c1, ref_13.n_nationkey AS c2, ref_14.n_comment AS c3, ref_13.n_regionkey AS c4, subq_1.c1 AS c5, ref_13.n_comment AS c6, ref_14.n_name AS c7, ref_6.p_brand AS c8, subq_0.c0 AS c9
                                            FROM
                                                main.nation AS ref_14
                                            WHERE
                                                EXISTS (
                                                    SELECT
                                                        39 AS c0, ref_14.n_regionkey AS c1
                                                    FROM
                                                        main.partsupp AS ref_15
                                                    WHERE
                                                        1
                                                    LIMIT 58)))
                                        OR (1)
                                    LIMIT 186))
                            AND ((1)
                                AND (((EXISTS (
                                                SELECT
                                                    ref_6.p_brand AS c0,
                                                    ref_6.p_type AS c1,
                                                    ref_16.o_shippriority AS c2,
                                                    ref_16.o_clerk AS c3,
                                                    (
                                                        SELECT
                                                            ps_comment
                                                        FROM
                                                            main.partsupp
                                                        LIMIT 1 offset 4) AS c4,
                                                    ref_6.p_size AS c5,
                                                    ref_16.o_comment AS c6,
                                                    ref_6.p_mfgr AS c7,
                                                    ref_6.p_container AS c8,
                                                    subq_1.c0 AS c9,
                                                    subq_1.c0 AS c10,
                                                    ref_16.o_orderpriority AS c11,
                                                    ref_6.p_container AS c12,
                                                    subq_1.c0 AS c13,
                                                    subq_1.c1 AS c14,
                                                    ref_6.p_type AS c15
                                                FROM
                                                    main.orders AS ref_16
                                                WHERE ((EXISTS (
                                                            SELECT
                                                                subq_1.c0 AS c0, (
                                                                    SELECT
                                                                        n_regionkey
                                                                    FROM
                                                                        main.nation
                                                                    LIMIT 1 offset 3) AS c1
                                                            FROM
                                                                main.region AS ref_17
                                                            WHERE ((((0)
                                                                        AND (0))
                                                                    OR ((EXISTS (
                                                                                SELECT
                                                                                    subq_1.c1 AS c0, ref_17.r_regionkey AS c1
                                                                                FROM
                                                                                    main.part AS ref_18
                                                                                WHERE
                                                                                    1
                                                                                LIMIT 46))
                                                                        AND (0)))
                                                                OR (((0)
                                                                        AND (((EXISTS (
                                                                                        SELECT
                                                                                            ref_6.p_type AS c0,
                                                                                            subq_1.c0 AS c1,
                                                                                            ref_16.o_orderstatus AS c2,
                                                                                            subq_1.c0 AS c3,
                                                                                            (
                                                                                                SELECT
                                                                                                    p_container
                                                                                                FROM
                                                                                                    main.part
                                                                                                LIMIT 1 offset 6) AS c4,
                                                                                            subq_0.c2 AS c5,
                                                                                            ref_16.o_clerk AS c6
                                                                                        FROM
                                                                                            main.customer AS ref_19
                                                                                        WHERE
                                                                                            1
                                                                                        LIMIT 101))
                                                                                OR (0))
                                                                            AND (1)))
                                                                    AND (1)))
                                                            OR (0)))
                                                    OR (1))
                                                OR (EXISTS (
                                                        SELECT
                                                            subq_0.c2 AS c0
                                                        FROM
                                                            main.region AS ref_20
                                                        WHERE
                                                            0))))
                                            OR (0))
                                        AND ((1)
                                            OR (1)))))))))))
    OR (((subq_0.c2 IS NULL)
            AND ((EXISTS (
                        SELECT
                            subq_1.c1 AS c0, ref_6.p_mfgr AS c1, subq_1.c1 AS c2, ref_21.r_name AS c3, ref_21.r_name AS c4, ref_21.r_name AS c5, subq_1.c1 AS c6, ref_6.p_type AS c7, ref_6.p_mfgr AS c8, subq_1.c1 AS c9
                        FROM
                            main.region AS ref_21
                        WHERE
                            0
                        LIMIT 95))
                AND (subq_1.c1 IS NOT NULL)))
        OR (1)))))
AND ((EXISTS (
            SELECT
                ref_22.r_name AS c0
            FROM
                main.region AS ref_22
            RIGHT JOIN main.nation AS ref_23 ON (ref_22.r_comment = ref_23.n_name)
        WHERE (
            SELECT
                l_tax
            FROM
                main.lineitem
            LIMIT 1 offset 30)
        IS NOT NULL
    LIMIT 136))
OR (1))
LIMIT 80
