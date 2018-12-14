SELECT
    CASE WHEN ((ref_0.ps_suppkey IS NULL)
            AND ((ref_0.ps_supplycost IS NOT NULL)
                AND (((0)
                        OR ((1)
                            OR ((ref_0.ps_suppkey IS NOT NULL)
                                AND (((0)
                                        AND (EXISTS (
                                                SELECT
                                                    ref_1.p_size AS c0,
                                                    ref_0.ps_comment AS c1,
                                                    ref_0.ps_partkey AS c2,
                                                    ref_1.p_size AS c3,
                                                    95 AS c4,
                                                    ref_1.p_container AS c5,
                                                    ref_0.ps_availqty AS c6,
                                                    ref_0.ps_partkey AS c7,
                                                    ref_1.p_retailprice AS c8,
                                                    ref_1.p_size AS c9,
                                                    (
                                                        SELECT
                                                            ps_suppkey
                                                        FROM
                                                            main.partsupp
                                                        LIMIT 1 offset 3) AS c10,
                                                    ref_1.p_container AS c11,
                                                    ref_0.ps_comment AS c12,
                                                    ref_0.ps_comment AS c13,
                                                    ref_0.ps_comment AS c14,
                                                    ref_1.p_type AS c15,
                                                    ref_1.p_name AS c16,
                                                    ref_0.ps_partkey AS c17
                                                FROM
                                                    main.part AS ref_1
                                                WHERE (1)
                                                AND ((ref_0.ps_partkey IS NULL)
                                                    OR (ref_0.ps_supplycost IS NOT NULL))
                                            LIMIT 68)))
                                OR (EXISTS (
                                        SELECT
                                            ref_2.r_regionkey AS c0,
                                            ref_0.ps_comment AS c1,
                                            ref_2.r_regionkey AS c2,
                                            ref_0.ps_suppkey AS c3,
                                            ref_0.ps_availqty AS c4,
                                            ref_0.ps_supplycost AS c5,
                                            68 AS c6
                                        FROM
                                            main.region AS ref_2
                                        WHERE
                                            0
                                        LIMIT 94))))))
                AND ((1)
                    AND ((((EXISTS (
                                        SELECT
                                            ref_3.ps_partkey AS c0,
                                            96 AS c1,
                                            (
                                                SELECT
                                                    c_comment
                                                FROM
                                                    main.customer
                                                LIMIT 1 offset 72) AS c2,
                                            ref_3.ps_supplycost AS c3
                                        FROM
                                            main.partsupp AS ref_3
                                        WHERE
                                            0))
                                    OR (ref_0.ps_availqty IS NOT NULL))
                                AND ((ref_0.ps_comment IS NOT NULL)
                                    OR ((EXISTS (
                                                SELECT
                                                    ref_4.s_suppkey AS c0
                                                FROM
                                                    main.supplier AS ref_4
                                                WHERE
                                                    1))
                                            OR ((ref_0.ps_partkey IS NULL)
                                                OR ((EXISTS (
                                                            SELECT
                                                                ref_5.ps_partkey AS c0, ref_0.ps_partkey AS c1, ref_0.ps_comment AS c2
                                                            FROM
                                                                main.partsupp AS ref_5
                                                            WHERE
                                                                0))
                                                        OR (0))))))
                                    AND (ref_0.ps_comment IS NULL))))))
                OR ((((EXISTS (
                                    SELECT
                                        ref_6.p_brand AS c0, ref_6.p_brand AS c1, ref_6.p_partkey AS c2, ref_6.p_name AS c3, ref_6.p_retailprice AS c4
                                    FROM
                                        main.part AS ref_6
                                    WHERE
                                        ref_0.ps_availqty IS NOT NULL))
                                AND (ref_0.ps_supplycost IS NULL))
                            AND (0))
                        OR ((ref_0.ps_supplycost IS NOT NULL)
                            OR (ref_0.ps_supplycost IS NOT NULL))) THEN
                    ref_0.ps_suppkey
                ELSE
                    ref_0.ps_suppkey
                END AS c0, CASE WHEN ref_0.ps_comment IS NULL THEN
                    CASE WHEN 1 THEN
                        ref_0.ps_suppkey
                    ELSE
                        ref_0.ps_suppkey
                    END
                ELSE
                    CASE WHEN 1 THEN
                        ref_0.ps_suppkey
                    ELSE
                        ref_0.ps_suppkey
                    END
                END AS c1, CASE WHEN ((ref_0.ps_comment IS NULL)
                        OR ((1)
                            OR (1)))
                    OR (ref_0.ps_comment IS NULL) THEN
                    ref_0.ps_partkey
                ELSE
                    ref_0.ps_partkey
                END AS c2, ref_0.ps_partkey AS c3, ref_0.ps_partkey AS c4, ref_0.ps_suppkey AS c5, ref_0.ps_suppkey AS c6, ref_0.ps_partkey AS c7
            FROM
                main.partsupp AS ref_0
            WHERE
                ref_0.ps_comment IS NOT NULL
