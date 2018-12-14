SELECT
    subq_0.c0 AS c0,
    subq_0.c2 AS c1,
    subq_0.c0 AS c2
FROM (
    SELECT
        ref_1.ps_supplycost AS c0,
        ref_2.s_name AS c1,
        ref_0.p_name AS c2,
        58 AS c3
    FROM
        main.part AS ref_0
    LEFT JOIN main.partsupp AS ref_1
    RIGHT JOIN main.supplier AS ref_2 ON ((1)
            OR (0)) ON (ref_0.p_container = ref_1.ps_comment)
WHERE (ref_1.ps_partkey IS NULL)
OR (ref_0.p_partkey IS NOT NULL)
LIMIT 91) AS subq_0
WHERE (0)
OR ((subq_0.c2 IS NULL)
    OR ((((EXISTS (
                        SELECT
                            ref_3.n_regionkey AS c0, (
                                SELECT
                                    r_regionkey
                                FROM
                                    main.region
                                LIMIT 1 offset 1) AS c1,
                            52 AS c2,
                            subq_0.c1 AS c3,
                            subq_0.c3 AS c4,
                            (
                                SELECT
                                    n_name
                                FROM
                                    main.nation
                                LIMIT 1 offset 3) AS c5,
                            subq_0.c1 AS c6,
                            subq_0.c2 AS c7
                        FROM
                            main.nation AS ref_3
                        WHERE
                            0))
                    AND ((EXISTS (
                                SELECT
                                    ref_4.o_clerk AS c0, (
                                        SELECT
                                            r_name
                                        FROM
                                            main.region
                                        LIMIT 1 offset 3) AS c1,
                                    ref_4.o_orderpriority AS c2,
                                    subq_0.c0 AS c3,
                                    ref_4.o_custkey AS c4,
                                    subq_0.c1 AS c5,
                                    ref_4.o_comment AS c6,
                                    subq_0.c2 AS c7
                                FROM
                                    main.orders AS ref_4
                                WHERE
                                    EXISTS (
                                        SELECT
                                            ref_5.c_name AS c0, ref_4.o_shippriority AS c1
                                        FROM
                                            main.customer AS ref_5
                                        WHERE
                                            1)
                                    LIMIT 76))
                            AND (0)))
                    AND (0))
                AND ((subq_0.c1 IS NULL)
                    OR ((subq_0.c1 IS NULL)
                        AND (EXISTS (
                                SELECT
                                    ref_6.s_name AS c0,
                                    subq_0.c2 AS c1,
                                    subq_0.c1 AS c2
                                FROM
                                    main.supplier AS ref_6
                                WHERE (subq_0.c1 IS NULL)
                                AND (subq_0.c2 IS NOT NULL)
                            LIMIT 140))))))
LIMIT 97
