SELECT
    subq_0.c5 AS c0,
    subq_0.c6 AS c1,
    subq_0.c2 AS c2,
    subq_0.c0 AS c3,
    subq_0.c6 AS c4
FROM (
    SELECT
        ref_2.n_comment AS c0,
        CASE WHEN (EXISTS (
                    SELECT
                        16 AS c0,
                        (
                            SELECT
                                n_regionkey
                            FROM
                                main.nation
                            LIMIT 1 offset 5) AS c1,
                        ref_2.n_regionkey AS c2
                    FROM
                        main.region AS ref_3
                    WHERE
                        1
                    LIMIT 84))
            AND (ref_1.n_regionkey IS NOT NULL) THEN
            ref_0.o_clerk
        ELSE
            ref_0.o_clerk
        END AS c1,
        ref_2.n_nationkey AS c2,
        ref_0.o_clerk AS c3,
        ref_0.o_orderkey AS c4,
        ref_0.o_orderpriority AS c5,
        ref_0.o_orderpriority AS c6,
        ref_1.n_name AS c7
    FROM
        main.orders AS ref_0
        INNER JOIN main.nation AS ref_1
        INNER JOIN main.nation AS ref_2 ON (ref_1.n_comment IS NULL) ON (ref_0.o_orderkey = ref_1.n_nationkey)
    WHERE
        1
    LIMIT 147) AS subq_0
WHERE (EXISTS (
        SELECT
            ref_4.p_mfgr AS c0, subq_0.c6 AS c1, ref_4.p_retailprice AS c2
        FROM
            main.part AS ref_4
        WHERE
            EXISTS (
                SELECT
                    subq_0.c4 AS c0, ref_4.p_brand AS c1
                FROM
                    main.partsupp AS ref_5
                WHERE (0)
                OR (((((EXISTS (
                                        SELECT
                                            ref_6.n_comment AS c0, ref_6.n_comment AS c1, ref_4.p_type AS c2, subq_0.c3 AS c3, 6 AS c4, ref_6.n_name AS c5, ref_5.ps_supplycost AS c6, ref_4.p_container AS c7, ref_5.ps_availqty AS c8, subq_0.c1 AS c9, ref_6.n_comment AS c10
                                        FROM
                                            main.nation AS ref_6
                                        WHERE
                                            1
                                        LIMIT 117))
                                OR ((EXISTS (
                                            SELECT
                                                ref_5.ps_availqty AS c0,
                                                ref_7.r_regionkey AS c1,
                                                subq_0.c5 AS c2,
                                                ref_5.ps_supplycost AS c3,
                                                ref_7.r_regionkey AS c4,
                                                ref_5.ps_partkey AS c5,
                                                ref_4.p_name AS c6,
                                                ref_4.p_comment AS c7,
                                                ref_7.r_comment AS c8
                                            FROM
                                                main.region AS ref_7
                                            WHERE
                                                1
                                            LIMIT 97))
                                    OR (EXISTS (
                                            SELECT
                                                subq_0.c4 AS c0,
                                                24 AS c1,
                                                subq_0.c2 AS c2
                                            FROM
                                                main.partsupp AS ref_8
                                            WHERE
                                                1
                                            LIMIT 114))))
                            AND (1))
                        OR ((ref_4.p_container IS NULL)
                            OR (1)))
                    OR (EXISTS (
                            SELECT
                                ref_5.ps_partkey AS c0,
                                subq_0.c5 AS c1,
                                ref_5.ps_supplycost AS c2,
                                subq_0.c3 AS c3,
                                subq_0.c4 AS c4
                            FROM
                                main.supplier AS ref_9
                            WHERE (44 IS NULL)
                            AND ((ref_4.p_partkey IS NULL)
                                AND (0))
                        LIMIT 49)))
        LIMIT 100)
LIMIT 154))
AND (subq_0.c2 IS NULL)
LIMIT 102
