SELECT
    subq_0.c7 AS c0,
    subq_0.c2 AS c1,
    subq_0.c5 AS c2,
    subq_0.c5 AS c3,
    subq_0.c4 AS c4
FROM (
    SELECT
        ref_3.r_comment AS c0,
        ref_2.ps_supplycost AS c1,
        CASE WHEN (((EXISTS (
                            SELECT
                                ref_2.ps_suppkey AS c0,
                                ref_3.r_comment AS c1,
                                ref_3.r_name AS c2,
                                ref_2.ps_supplycost AS c3,
                                ref_1.r_name AS c4,
                                (
                                    SELECT
                                        r_comment
                                    FROM
                                        main.region
                                    LIMIT 1 offset 4) AS c5,
                                34 AS c6,
                                ref_0.p_brand AS c7,
                                ref_4.s_comment AS c8,
                                ref_1.r_name AS c9,
                                ref_3.r_comment AS c10,
                                ref_4.s_phone AS c11,
                                ref_2.ps_suppkey AS c12
                            FROM
                                main.supplier AS ref_4
                            WHERE
                                EXISTS (
                                    SELECT
                                        ref_2.ps_availqty AS c0, ref_1.r_comment AS c1, ref_0.p_partkey AS c2, ref_4.s_phone AS c3, ref_4.s_phone AS c4
                                    FROM
                                        main.region AS ref_5
                                    WHERE
                                        0
                                    LIMIT 98)
                            LIMIT 115))
                    AND ((
                            SELECT
                                n_name
                            FROM
                                main.nation
                            LIMIT 1 offset 6)
                        IS NULL))
                OR (((0)
                        AND ((((1)
                                    AND (1))
                                OR ((((((EXISTS (
                                                            SELECT
                                                                ref_3.r_comment AS c0
                                                            FROM
                                                                main.partsupp AS ref_6
                                                            WHERE
                                                                EXISTS (
                                                                    SELECT
                                                                        ref_6.ps_comment AS c0, ref_0.p_retailprice AS c1, (
                                                                            SELECT
                                                                                o_orderpriority
                                                                            FROM
                                                                                main.orders
                                                                            LIMIT 1 offset 14) AS c2,
                                                                        ref_7.l_extendedprice AS c3,
                                                                        ref_7.l_shipinstruct AS c4,
                                                                        ref_7.l_quantity AS c5,
                                                                        ref_7.l_suppkey AS c6,
                                                                        ref_1.r_regionkey AS c7
                                                                    FROM
                                                                        main.lineitem AS ref_7
                                                                    WHERE
                                                                        66 IS NOT NULL)
                                                                LIMIT 47))
                                                        AND (((ref_1.r_comment IS NOT NULL)
                                                                AND (0))
                                                            OR (0)))
                                                    OR (0))
                                                OR (1))
                                            AND (0))
                                        OR (0)))
                                OR (1)))
                        AND (((((1)
                                        AND ((ref_3.r_comment IS NULL)
                                            AND ((ref_2.ps_comment IS NULL)
                                                AND (1))))
                                    OR (((1)
                                            AND (1))
                                        OR (0)))
                                AND (0))
                            OR (0))))
                OR (0) THEN
                ref_2.ps_partkey
            ELSE
                ref_2.ps_partkey
            END AS c2,
            (
                SELECT
                    o_clerk
                FROM
                    main.orders
                LIMIT 1 offset 3) AS c3,
            ref_3.r_comment AS c4,
            ref_3.r_regionkey AS c5,
            ref_2.ps_suppkey AS c6,
            ref_3.r_regionkey AS c7
        FROM
            main.part AS ref_0
            INNER JOIN main.region AS ref_1
            RIGHT JOIN main.partsupp AS ref_2
            INNER JOIN main.region AS ref_3 ON ((ref_3.r_name IS NULL)
                    OR ((ref_3.r_name IS NOT NULL)
                        AND (1))) ON (ref_1.r_comment = ref_2.ps_comment) ON ((0)
                    OR ((1)
                        OR (0)))
        WHERE (EXISTS (
                SELECT
                    ref_3.r_name AS c0, ref_8.p_brand AS c1, ref_0.p_brand AS c2, ref_3.r_regionkey AS c3, ref_8.p_name AS c4
                FROM
                    main.part AS ref_8
                WHERE (ref_8.p_type IS NOT NULL)
                OR (((1)
                        OR ((1)
                            OR (EXISTS (
                                    SELECT
                                        ref_1.r_regionkey AS c0, ref_3.r_comment AS c1, ref_3.r_regionkey AS c2, ref_8.p_type AS c3, ref_0.p_container AS c4, ref_9.n_regionkey AS c5
                                    FROM
                                        main.nation AS ref_9
                                    WHERE
                                        1
                                    LIMIT 51))))
                    OR ((((((0)
                                        AND ((((((ref_1.r_regionkey IS NULL)
                                                            OR (((0)
                                                                    AND (EXISTS (
                                                                            SELECT
                                                                                ref_2.ps_availqty AS c0,
                                                                                ref_2.ps_supplycost AS c1,
                                                                                ref_1.r_comment AS c2,
                                                                                ref_1.r_regionkey AS c3,
                                                                                ref_10.n_nationkey AS c4
                                                                            FROM
                                                                                main.nation AS ref_10
                                                                            WHERE (EXISTS (
                                                                                    SELECT
                                                                                        ref_1.r_comment AS c0, ref_11.n_comment AS c1, ref_11.n_regionkey AS c2, ref_8.p_size AS c3, ref_8.p_retailprice AS c4
                                                                                    FROM
                                                                                        main.nation AS ref_11
                                                                                    WHERE
                                                                                        1))
                                                                                OR (0))))
                                                                    AND (0)))
                                                            AND (ref_0.p_container IS NULL))
                                                        AND (ref_8.p_size IS NULL))
                                                    OR (((0)
                                                            AND (ref_8.p_retailprice IS NOT NULL))
                                                        AND ((1)
                                                            AND (((ref_8.p_retailprice IS NULL)
                                                                    AND (((0)
                                                                            OR (ref_0.p_type IS NULL))
                                                                        AND ((ref_0.p_name IS NOT NULL)
                                                                            OR (0))))
                                                                OR (ref_8.p_comment IS NOT NULL)))))
                                                OR (EXISTS (
                                                        SELECT
                                                            ref_1.r_name AS c0
                                                        FROM
                                                            main.partsupp AS ref_12
                                                        WHERE
                                                            0
                                                        LIMIT 65))))
                                        OR (1))
                                    AND (0))
                                AND ((0)
                                    OR (1)))
                            AND (0)))
                LIMIT 88))
        AND (1)
    LIMIT 154) AS subq_0
WHERE (EXISTS (
        SELECT
            ref_13.r_comment AS c0
        FROM
            main.region AS ref_13
        WHERE
            ref_13.r_regionkey IS NOT NULL
        LIMIT 116))
OR ((subq_0.c4 IS NOT NULL)
    OR (EXISTS (
            SELECT
                ref_14.s_nationkey AS c0
            FROM
                main.supplier AS ref_14
            WHERE
                0
            LIMIT 139)))
LIMIT 150
