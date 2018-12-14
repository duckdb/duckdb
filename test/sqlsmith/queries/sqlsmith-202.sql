SELECT
    CAST(coalesce(subq_0.c1, subq_0.c1) AS INTEGER) AS c0
FROM (
    SELECT
        CASE WHEN ref_0.s_comment IS NOT NULL THEN
            ref_0.s_suppkey
        ELSE
            ref_0.s_suppkey
        END AS c0,
        ref_0.s_suppkey AS c1,
        ref_0.s_acctbal AS c2,
        ref_0.s_acctbal AS c3,
        ref_0.s_phone AS c4,
        ref_0.s_comment AS c5
    FROM
        main.supplier AS ref_0
    WHERE
        ref_0.s_comment IS NOT NULL
    LIMIT 129) AS subq_0
WHERE (subq_0.c1 IS NOT NULL)
AND (((EXISTS (
                SELECT
                    ref_2.n_regionkey AS c0, ref_1.s_phone AS c1, 54 AS c2, subq_0.c2 AS c3
                FROM
                    main.supplier AS ref_1
                LEFT JOIN main.nation AS ref_2
                INNER JOIN main.orders AS ref_3 ON ((1)
                        AND (1)) ON (ref_1.s_comment = ref_2.n_name)
            WHERE ((ref_3.o_orderdate IS NOT NULL)
                AND (((0)
                        AND (((1)
                                AND ((ref_2.n_regionkey IS NULL)
                                    OR (EXISTS (
                                            SELECT
                                                ref_2.n_regionkey AS c0, ref_2.n_regionkey AS c1, ref_1.s_name AS c2
                                            FROM
                                                main.nation AS ref_4
                                            WHERE
                                                1))))
                                OR (ref_3.o_orderkey IS NULL)))
                        AND ((((31 IS NOT NULL)
                                    OR (1))
                                OR (((subq_0.c5 IS NOT NULL)
                                        OR (1))
                                    AND ((1)
                                        OR (EXISTS (
                                                SELECT
                                                    ref_3.o_comment AS c0, 50 AS c1, subq_0.c4 AS c2
                                                FROM
                                                    main.partsupp AS ref_5
                                                WHERE
                                                    ref_2.n_name IS NULL
                                                LIMIT 93)))))
                            OR (((ref_2.n_comment IS NULL)
                                    AND (ref_2.n_nationkey IS NOT NULL))
                                AND (1)))))
                OR (EXISTS (
                        SELECT
                            ref_1.s_name AS c0
                        FROM
                            main.partsupp AS ref_6
                        WHERE
                            subq_0.c1 IS NOT NULL
                        LIMIT 91))
            LIMIT 101))
    OR (1))
OR (EXISTS (
        SELECT
            subq_0.c4 AS c0,
            ref_8.p_brand AS c1,
            90 AS c2,
            subq_0.c0 AS c3,
            subq_0.c5 AS c4,
            subq_0.c5 AS c5
        FROM
            main.partsupp AS ref_7
            INNER JOIN main.part AS ref_8 ON (ref_7.ps_partkey IS NOT NULL)
        WHERE
            ref_7.ps_supplycost IS NOT NULL
        LIMIT 96)))
LIMIT 106
