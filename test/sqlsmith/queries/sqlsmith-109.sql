SELECT
    subq_0.c2 AS c0,
    subq_0.c6 AS c1,
    subq_0.c6 AS c2,
    subq_0.c9 AS c3
FROM (
    SELECT
        ref_2.n_comment AS c0,
        ref_1.p_mfgr AS c1,
        ref_0.r_regionkey AS c2,
        ref_1.p_comment AS c3,
        ref_1.p_size AS c4,
        ref_0.r_name AS c5,
        ref_0.r_name AS c6,
        ref_0.r_regionkey AS c7,
        ref_2.n_regionkey AS c8,
        ref_2.n_regionkey AS c9
    FROM
        main.region AS ref_0
    LEFT JOIN main.part AS ref_1
    INNER JOIN main.nation AS ref_2 ON (ref_1.p_type IS NOT NULL) ON (ref_0.r_comment = ref_1.p_name)
WHERE ((83 IS NULL)
    AND ((ref_1.p_type IS NULL)
        AND (EXISTS (
                SELECT
                    ref_0.r_regionkey AS c0, ref_3.n_name AS c1, ref_1.p_type AS c2, ref_1.p_mfgr AS c3, ref_3.n_name AS c4, ref_3.n_nationkey AS c5, ref_1.p_type AS c6, (
                        SELECT
                            o_custkey
                        FROM
                            main.orders
                        LIMIT 1 offset 72) AS c7,
                    ref_0.r_regionkey AS c8,
                    ref_0.r_regionkey AS c9
                FROM
                    main.nation AS ref_3
                WHERE ((((1)
                            AND ((0)
                                OR (1)))
                        AND (1))
                    AND ((0)
                        OR ((((ref_3.n_nationkey IS NULL)
                                    OR (ref_0.r_comment IS NOT NULL))
                                OR ((EXISTS (
                                            SELECT
                                                79 AS c0, ref_2.n_name AS c1, ref_2.n_name AS c2, ref_4.ps_comment AS c3, ref_0.r_comment AS c4, ref_0.r_comment AS c5, (
                                                    SELECT
                                                        c_custkey
                                                    FROM
                                                        main.customer
                                                    LIMIT 1 offset 2) AS c6,
                                                ref_4.ps_supplycost AS c7,
                                                ref_4.ps_supplycost AS c8,
                                                ref_4.ps_comment AS c9
                                            FROM
                                                main.partsupp AS ref_4
                                            WHERE
                                                1))
                                        OR ((
                                                SELECT
                                                    r_regionkey
                                                FROM
                                                    main.region
                                                LIMIT 1 offset 89)
                                            IS NULL)))
                                AND (0))))
                    AND ((ref_1.p_brand IS NULL)
                        AND (0))))))
    OR ((ref_2.n_regionkey IS NOT NULL)
        AND ((45 IS NOT NULL)
            AND (((1)
                    OR (EXISTS (
                            SELECT
                                ref_5.o_custkey AS c0,
                                ref_2.n_comment AS c1
                            FROM
                                main.orders AS ref_5
                            WHERE
                                0)))
                    AND (1))))) AS subq_0
WHERE ((subq_0.c1 IS NULL)
    AND (subq_0.c1 IS NULL))
OR (((EXISTS (
                SELECT
                    ref_7.ps_availqty AS c0, ref_8.s_comment AS c1, (
                        SELECT
                            l_tax
                        FROM
                            main.lineitem
                        LIMIT 1 offset 21) AS c2,
                    ref_6.r_regionkey AS c3,
                    subq_0.c4 AS c4,
                    subq_0.c7 AS c5
                FROM
                    main.region AS ref_6
                LEFT JOIN main.partsupp AS ref_7
                RIGHT JOIN main.supplier AS ref_8 ON ((ref_7.ps_supplycost IS NULL)
                        AND (0)) ON ((ref_7.ps_supplycost IS NOT NULL)
                        OR (1))
            WHERE (0)
            OR (ref_6.r_name IS NULL)
        LIMIT 103))
AND (((((EXISTS (
                        SELECT
                            subq_0.c9 AS c0
                        FROM
                            main.nation AS ref_9
                        WHERE ((EXISTS (
                                    SELECT
                                        27 AS c0, ref_10.n_nationkey AS c1, subq_0.c3 AS c2, subq_0.c3 AS c3, ref_10.n_comment AS c4
                                    FROM
                                        main.nation AS ref_10
                                    WHERE
                                        1))
                                AND (1))
                            OR (EXISTS (
                                    SELECT
                                        subq_0.c1 AS c0, ref_11.r_regionkey AS c1, subq_0.c6 AS c2, ref_9.n_comment AS c3, (
                                            SELECT
                                                o_orderkey
                                            FROM
                                                main.orders
                                            LIMIT 1 offset 2) AS c4,
                                        ref_11.r_comment AS c5
                                    FROM
                                        main.region AS ref_11
                                    WHERE (1)
                                    AND (0)
                                LIMIT 106))))
                AND (1))
            OR (EXISTS (
                    SELECT
                        ref_12.c_phone AS c0,
                        subq_0.c0 AS c1,
                        ref_12.c_phone AS c2,
                        ref_12.c_acctbal AS c3,
                        99 AS c4,
                        ref_12.c_custkey AS c5,
                        subq_0.c5 AS c6,
                        ref_12.c_name AS c7,
                        ref_12.c_custkey AS c8,
                        ref_12.c_phone AS c9
                    FROM
                        main.customer AS ref_12
                    WHERE (1)
                    AND ((ref_12.c_phone IS NULL)
                        AND (((subq_0.c3 IS NULL)
                                AND (EXISTS (
                                        SELECT
                                            ref_12.c_custkey AS c0, ref_13.c_nationkey AS c1, 55 AS c2, ref_13.c_phone AS c3
                                        FROM
                                            main.customer AS ref_13
                                        WHERE
                                            1
                                        LIMIT 15)))
                            OR (0)))
                LIMIT 89)))
    OR (0))
AND (((0)
        AND (subq_0.c5 IS NOT NULL))
    OR (0))))
OR (1))
LIMIT 81
