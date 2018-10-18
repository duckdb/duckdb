SELECT
    subq_1.c0 AS c0,
    subq_1.c0 AS c1,
    subq_1.c0 AS c2
FROM (
    SELECT
        ref_2.n_regionkey AS c0
    FROM (
        SELECT
            ref_0.ps_comment AS c0,
            ref_0.ps_comment AS c1,
            ref_0.ps_supplycost AS c2,
            ref_0.ps_availqty AS c3,
            ref_0.ps_partkey AS c4,
            ref_0.ps_partkey AS c5,
            ref_0.ps_supplycost AS c6,
            ref_0.ps_supplycost AS c7, (
                SELECT
                    n_comment
                FROM
                    main.nation
                LIMIT 1 offset 2) AS c8,
            ref_0.ps_partkey AS c9
        FROM
            main.partsupp AS ref_0
        WHERE (1)
        AND (EXISTS (
                SELECT
                    ref_1.p_comment AS c0, ref_1.p_partkey AS c1, ref_1.p_container AS c2
                FROM
                    main.part AS ref_1
                WHERE
                    ref_0.ps_supplycost IS NULL))) AS subq_0
    RIGHT JOIN main.nation AS ref_2 ON (((ref_2.n_name IS NOT NULL)
                OR ((
                        SELECT
                            p_comment
                        FROM
                            main.part
                        LIMIT 1 offset 2)
                    IS NULL))
            OR (((ref_2.n_comment IS NULL)
                    AND (1))
                AND ((1)
                    OR ((subq_0.c8 IS NULL)
                        OR (((subq_0.c2 IS NOT NULL)
                                AND ((EXISTS (
                                            SELECT
                                                ref_3.ps_availqty AS c0,
                                                67 AS c1,
                                                98 AS c2,
                                                ref_3.ps_partkey AS c3,
                                                ref_3.ps_availqty AS c4,
                                                ref_2.n_name AS c5,
                                                ref_2.n_comment AS c6,
                                                subq_0.c5 AS c7,
                                                subq_0.c3 AS c8
                                            FROM
                                                main.partsupp AS ref_3
                                            WHERE 0
                                        LIMIT 138))
                                AND (((0)
                                        OR ((1)
                                            OR (ref_2.n_comment IS NULL)))
                                    OR (EXISTS (
                                            SELECT
                                                subq_0.c7 AS c0
                                            FROM
                                                main.lineitem AS ref_4
                                            WHERE
                                                EXISTS (
                                                    SELECT
                                                        (
                                                            SELECT
                                                                l_partkey
                                                            FROM
                                                                main.lineitem
                                                            LIMIT 1 offset 2) AS c0
                                                    FROM
                                                        main.region AS ref_5
                                                    WHERE (((1)
                                                            AND ((ref_5.r_comment IS NOT NULL)
                                                                OR ((0)
                                                                    AND (0))))
                                                        AND ((0)
                                                            OR (0)))
                                                    AND (0)
                                                LIMIT 109))))))
                        OR (subq_0.c6 IS NULL))))))
WHERE (0)
AND ((1)
    AND ((EXISTS (
                SELECT
                    (
                        SELECT
                            n_name
                        FROM
                            main.nation
                        LIMIT 1 offset 4) AS c0,
                    5 AS c1,
                    ref_6.p_brand AS c2,
                    ref_6.p_size AS c3,
                    subq_0.c8 AS c4,
                    subq_0.c7 AS c5,
                    ref_2.n_nationkey AS c6,
                    ref_6.p_comment AS c7,
                    subq_0.c7 AS c8,
                    ref_6.p_retailprice AS c9,
                    34 AS c10,
                    subq_0.c5 AS c11,
                    subq_0.c6 AS c12
                FROM
                    main.part AS ref_6
                WHERE
                    1
                LIMIT 112))
        AND ((1)
            AND ((EXISTS (
                        SELECT
                            (
                                SELECT
                                    s_comment
                                FROM
                                    main.supplier
                                LIMIT 1 offset 3) AS c0
                        FROM
                            main.region AS ref_7
                        WHERE 0
                    LIMIT 164))
            OR (((1)
                    AND (0))
                OR (EXISTS (
                        SELECT
                            subq_0.c5 AS c0,
                            ref_2.n_nationkey AS c1,
                            ref_8.s_acctbal AS c2, (
                                SELECT
                                    c_mktsegment
                                FROM
                                    main.customer
                                LIMIT 1 offset 6) AS c3,
                            subq_0.c2 AS c4,
                            89 AS c5,
                            ref_8.s_comment AS c6,
                            subq_0.c5 AS c7
                        FROM
                            main.supplier AS ref_8
                        WHERE
                            1
                        LIMIT 99)))))))) AS subq_1
WHERE (subq_1.c0 IS NOT NULL)
OR ((0)
    AND ((
            SELECT
                l_suppkey
            FROM
                main.lineitem
            LIMIT 1 offset 6)
        IS NULL))
LIMIT 109
