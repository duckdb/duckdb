INSERT INTO main.orders
    VALUES (26, 54, CAST(coalesce(
                CASE WHEN 77 IS NOT NULL THEN
                    CAST(NULL AS VARCHAR)
                ELSE
                    CAST(NULL AS VARCHAR)
                END, CAST(NULL AS VARCHAR)) AS VARCHAR), CASE WHEN ((50 IS NULL)
            OR ((0)
                OR (((((0)
                                OR (0))
                            OR (1))
                        OR (1))
                    OR ((33 IS NULL)
                        AND ((((((EXISTS (
                                                    SELECT
                                                        ref_0.p_comment AS c0,
                                                        ref_0.p_name AS c1,
                                                        (
                                                            SELECT
                                                                n_comment
                                                            FROM
                                                                main.nation
                                                            LIMIT 1 offset 6) AS c2,
                                                        ref_0.p_partkey AS c3,
                                                        ref_0.p_partkey AS c4,
                                                        (
                                                            SELECT
                                                                ps_availqty
                                                            FROM
                                                                main.partsupp
                                                            LIMIT 1 offset 3) AS c5,
                                                        ref_0.p_mfgr AS c6,
                                                        ref_0.p_brand AS c7,
                                                        ref_0.p_size AS c8,
                                                        ref_0.p_name AS c9,
                                                        ref_0.p_size AS c10,
                                                        ref_0.p_brand AS c11,
                                                        ref_0.p_partkey AS c12,
                                                        22 AS c13,
                                                        ref_0.p_brand AS c14,
                                                        ref_0.p_container AS c15,
                                                        ref_0.p_brand AS c16,
                                                        ref_0.p_name AS c17,
                                                        ref_0.p_container AS c18,
                                                        ref_0.p_comment AS c19,
                                                        ref_0.p_partkey AS c20,
                                                        ref_0.p_container AS c21,
                                                        ref_0.p_brand AS c22,
                                                        ref_0.p_mfgr AS c23,
                                                        ref_0.p_size AS c24,
                                                        ref_0.p_type AS c25,
                                                        ref_0.p_type AS c26,
                                                        ref_0.p_brand AS c27
                                                    FROM
                                                        main.part AS ref_0
                                                    WHERE (
                                                        SELECT
                                                            c_custkey
                                                        FROM
                                                            main.customer
                                                        LIMIT 1 offset 5) IS NOT NULL
                                                LIMIT 86))
                                        AND (81 IS NOT NULL))
                                    AND (90 IS NULL))
                                AND (1))
                            AND ((35 IS NULL)
                                OR (0)))
                        OR (EXISTS (
                                SELECT
                                    ref_1.ps_supplycost AS c0,
                                    ref_1.ps_availqty AS c1,
                                    ref_1.ps_suppkey AS c2,
                                    ref_1.ps_comment AS c3,
                                    ref_1.ps_supplycost AS c4,
                                    ref_1.ps_partkey AS c5,
                                    ref_1.ps_suppkey AS c6,
                                    ref_1.ps_availqty AS c7,
                                    ref_1.ps_supplycost AS c8,
                                    ref_1.ps_partkey AS c9,
                                    ref_1.ps_partkey AS c10
                                FROM
                                    main.partsupp AS ref_1
                                WHERE (ref_1.ps_suppkey IS NULL)
                                OR ((EXISTS (
                                            SELECT
                                                subq_0.c1 AS c0, subq_0.c0 AS c1, ref_2.o_shippriority AS c2, (
                                                    SELECT
                                                        r_regionkey
                                                    FROM
                                                        main.region
                                                    LIMIT 1 offset 1) AS c3,
                                                subq_0.c2 AS c4,
                                                ref_2.o_custkey AS c5,
                                                subq_0.c1 AS c6,
                                                subq_0.c2 AS c7,
                                                ref_2.o_orderpriority AS c8,
                                                62 AS c9
                                            FROM
                                                main.orders AS ref_2,
                                                LATERAL (
                                                    SELECT
                                                        ref_1.ps_supplycost AS c0,
                                                        ref_1.ps_suppkey AS c1,
                                                        ref_3.s_comment AS c2
                                                    FROM
                                                        main.supplier AS ref_3
                                                    WHERE
                                                        0
                                                    LIMIT 83) AS subq_0
                                            WHERE (0)
                                            AND (1)
                                        LIMIT 99))
                                OR (0))
                        LIMIT 81)))))))
    OR ((0)
            OR (((1)
                    AND (1))
                OR ((EXISTS (
                            SELECT
                                subq_1.c1 AS c0,
                                ref_4.l_receiptdate AS c1,
                                subq_1.c0 AS c2,
                                (
                                    SELECT
                                        r_name
                                    FROM
                                        main.region
                                    LIMIT 1 offset 1) AS c3,
                                subq_1.c0 AS c4
                            FROM
                                main.lineitem AS ref_4,
                                LATERAL (
                                    SELECT
                                        ref_5.p_partkey AS c0,
                                        ref_4.l_shipdate AS c1
                                    FROM
                                        main.part AS ref_5
                                    WHERE (((((0)
                                                    OR (1))
                                                OR (ref_4.l_discount IS NOT NULL))
                                            OR (1))
                                        AND ((ref_5.p_size IS NULL)
                                            AND (EXISTS (
                                                    SELECT
                                                        ref_4.l_quantity AS c0
                                                    FROM
                                                        main.supplier AS ref_6
                                                    WHERE
                                                        1
                                                    LIMIT 41))))
                                    AND (ref_5.p_size IS NOT NULL)
                                LIMIT 73) AS subq_1
                        WHERE
                            EXISTS (
                                SELECT
                                    ref_7.n_regionkey AS c0, ref_7.n_name AS c1, 67 AS c2
                                FROM
                                    main.nation AS ref_7
                                WHERE
                                    subq_1.c0 IS NOT NULL)))
                        AND (1 IS NOT NULL)))) THEN
            CAST(NULL AS DECIMAL)
        ELSE
            CAST(NULL AS DECIMAL)
        END, DEFAULT, CAST(NULL AS VARCHAR), DEFAULT, 97, CASE WHEN 1 THEN
            CAST(NULL AS VARCHAR)
        ELSE
            CAST(NULL AS VARCHAR)
        END)
