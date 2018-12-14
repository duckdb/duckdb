SELECT
    ref_0.ps_availqty AS c0,
    ref_0.ps_supplycost AS c1,
    (
        SELECT
            ps_suppkey
        FROM
            main.partsupp
        LIMIT 1 offset 3) AS c2,
    ref_0.ps_availqty AS c3,
    ref_0.ps_suppkey AS c4,
    ref_0.ps_partkey AS c5,
    ref_0.ps_suppkey AS c6
FROM
    main.partsupp AS ref_0
WHERE (((0)
        OR ((((((1)
                            AND (((EXISTS (
                                            SELECT
                                                ref_0.ps_comment AS c0, ref_1.c_phone AS c1, ref_1.c_custkey AS c2, ref_0.ps_supplycost AS c3, ref_1.c_nationkey AS c4, ref_0.ps_supplycost AS c5, ref_0.ps_comment AS c6, ref_1.c_phone AS c7
                                            FROM
                                                main.customer AS ref_1
                                            WHERE (0)
                                            AND ((0)
                                                OR (EXISTS (
                                                        SELECT
                                                            ref_0.ps_partkey AS c0, ref_1.c_phone AS c1, ref_0.ps_partkey AS c2, ref_0.ps_availqty AS c3, ref_0.ps_suppkey AS c4, ref_2.l_comment AS c5, ref_2.l_receiptdate AS c6, ref_2.l_receiptdate AS c7, ref_0.ps_comment AS c8
                                                        FROM
                                                            main.lineitem AS ref_2
                                                        WHERE
                                                            1)))))
                                        AND (EXISTS (
                                                SELECT
                                                    ref_0.ps_availqty AS c0, ref_3.n_nationkey AS c1
                                                FROM
                                                    main.nation AS ref_3
                                                WHERE (1)
                                                AND ((((1)
                                                            OR ((0)
                                                                OR ((ref_3.n_nationkey IS NULL)
                                                                    OR (ref_3.n_nationkey IS NULL))))
                                                        AND ((0)
                                                            OR (29 IS NULL)))
                                                    AND ((1)
                                                        AND (EXISTS (
                                                                SELECT
                                                                    ref_4.l_linestatus AS c0, ref_3.n_comment AS c1
                                                                FROM
                                                                    main.lineitem AS ref_4
                                                                WHERE (((1)
                                                                        AND (ref_0.ps_suppkey IS NULL))
                                                                    AND (ref_3.n_name IS NULL))
                                                                AND (1)
                                                            LIMIT 107))))
                                        LIMIT 72)))
                            OR (1)))
                    OR (((((1)
                                    AND ((
                                            SELECT
                                                n_name
                                            FROM
                                                main.nation
                                            LIMIT 1 offset 1)
                                        IS NULL))
                                AND ((ref_0.ps_supplycost IS NULL)
                                    OR ((
                                            SELECT
                                                s_acctbal
                                            FROM
                                                main.supplier
                                            LIMIT 1 offset 4)
                                        IS NOT NULL)))
                            AND ((0)
                                OR (0)))
                        OR ((
                                SELECT
                                    n_name
                                FROM
                                    main.nation
                                LIMIT 1 offset 1)
                            IS NULL)))
                OR (ref_0.ps_comment IS NULL))
            OR (((EXISTS (
                            SELECT
                                ref_5.n_regionkey AS c0,
                                ref_5.n_nationkey AS c1,
                                ref_5.n_comment AS c2,
                                ref_0.ps_comment AS c3,
                                ref_5.n_regionkey AS c4,
                                ref_0.ps_supplycost AS c5,
                                ref_5.n_comment AS c6,
                                ref_5.n_comment AS c7,
                                6 AS c8,
                                20 AS c9,
                                ref_5.n_name AS c10,
                                ref_0.ps_comment AS c11,
                                ref_5.n_comment AS c12,
                                ref_0.ps_supplycost AS c13,
                                (
                                    SELECT
                                        o_orderdate
                                    FROM
                                        main.orders
                                    LIMIT 1 offset 4) AS c14,
                                (
                                    SELECT
                                        p_type
                                    FROM
                                        main.part
                                    LIMIT 1 offset 3) AS c15,
                                ref_0.ps_availqty AS c16,
                                ref_5.n_regionkey AS c17,
                                ref_0.ps_suppkey AS c18
                            FROM
                                main.nation AS ref_5
                            WHERE
                                1
                            LIMIT 46))
                    OR (((((0)
                                    AND (0))
                                OR ((0)
                                    AND (ref_0.ps_availqty IS NOT NULL)))
                            AND (0))
                        AND ((0)
                            AND (1))))
                OR (ref_0.ps_suppkey IS NOT NULL)))
        AND (ref_0.ps_availqty IS NULL)))
OR (1))
AND (ref_0.ps_comment IS NOT NULL)
