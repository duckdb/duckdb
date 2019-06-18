WITH jennifer_0 AS (
    SELECT
        (
            SELECT
                s_address
            FROM
                main.supplier
            LIMIT 1 offset 6) AS c0
    FROM
        main.region AS sample_0 TABLESAMPLE BERNOULLI (0.7)
        RIGHT JOIN main.customer AS ref_0 ON (sample_0.r_comment IS NULL)
        INNER JOIN main.part AS sample_1 TABLESAMPLE BERNOULLI (6)
        INNER JOIN (
            SELECT
                68 AS c0,
                ref_1.l_partkey AS c1,
                sample_2.c_address AS c2,
                ref_1.l_commitdate AS c3,
                ref_1.l_suppkey AS c4,
                sample_2.c_phone AS c5,
                sample_2.c_custkey AS c6
            FROM
                main.customer AS sample_2 TABLESAMPLE SYSTEM (2.9)
                RIGHT JOIN main.lineitem AS ref_1 ON (EXISTS (
                        SELECT
                            ref_1.l_comment AS c0,
                            sample_2.c_phone AS c1,
                            ref_1.l_tax AS c2,
                            ref_1.l_shipdate AS c3,
                            52 AS c4
                        FROM
                            main.customer AS sample_3 TABLESAMPLE BERNOULLI (2.3)
                        WHERE (sample_3.c_nationkey IS NOT NULL)
                        OR (ref_1.l_comment IS NOT NULL)
                    LIMIT 150))
        WHERE
            ref_1.l_suppkey IS NULL
        LIMIT 142) AS subq_0 ON ((sample_1.p_mfgr IS NOT NULL)
        AND (0)) ON (EXISTS (
            SELECT
                47 AS c0,
                ref_0.c_nationkey AS c1,
                ref_0.c_address AS c2,
                ref_0.c_address AS c3
            FROM
                main.supplier AS ref_2
            WHERE
                EXISTS (
                    SELECT
                        subq_0.c5 AS c0,
                        sample_1.p_type AS c1,
                        (
                            SELECT
                                c_custkey
                            FROM
                                main.customer
                            LIMIT 1 offset 6) AS c2,
                        ref_2.s_name AS c3,
                        (
                            SELECT
                                s_phone
                            FROM
                                main.supplier
                            LIMIT 1 offset 16) AS c4,
                        ref_0.c_comment AS c5,
                        (
                            SELECT
                                n_name
                            FROM
                                main.nation
                            LIMIT 1 offset 2) AS c6
                    FROM
                        main.partsupp AS sample_4 TABLESAMPLE SYSTEM (4.3)
                    WHERE (((1)
                            OR (subq_0.c4 IS NOT NULL))
                        OR (80 IS NULL))
                    AND (ref_2.s_suppkey IS NOT NULL))
            LIMIT 136))
WHERE (1)
AND (0)
LIMIT 59
),
jennifer_1 AS (
    SELECT
        subq_5.c10 AS c0,
        subq_1.c0 AS c1
    FROM (
        SELECT
            ref_3.o_clerk AS c0
        FROM
            main.orders AS ref_3
        LEFT JOIN main.customer AS sample_5 TABLESAMPLE BERNOULLI (8.5) ON ((sample_5.c_comment IS NULL)
                OR (0))
            INNER JOIN main.orders AS sample_6 TABLESAMPLE SYSTEM (5.9)
            INNER JOIN main.nation AS ref_4 ON ((1)
                    OR ((ref_4.n_name IS NULL)
                        OR (1))) ON ((sample_5.c_nationkey IS NOT NULL)
                    AND (ref_4.n_name IS NULL))
        WHERE
            ref_3.o_clerk IS NOT NULL
        LIMIT 92) AS subq_1,
    LATERAL (
        SELECT
            subq_1.c0 AS c0,
            sample_7.p_size AS c1
        FROM
            main.part AS sample_7 TABLESAMPLE BERNOULLI (8.5)
                INNER JOIN main.supplier AS ref_5 ON ((sample_7.p_retailprice IS NULL)
                        AND (ref_5.s_phone IS NULL))
            WHERE
                1
            LIMIT 121) AS subq_2,
        LATERAL (
            SELECT
                subq_1.c0 AS c0,
                46 AS c1,
                (
                    SELECT
                        o_shippriority
                    FROM
                        main.orders
                    LIMIT 1 offset 4) AS c2,
                subq_1.c0 AS c3,
                (
                    SELECT
                        n_comment
                    FROM
                        main.nation
                    LIMIT 1 offset 61) AS c4,
                subq_2.c0 AS c5,
                subq_4.c3 AS c6,
                subq_2.c1 AS c7,
                subq_2.c1 AS c8,
                subq_4.c4 AS c9,
                subq_1.c0 AS c10
            FROM (
                SELECT
                    subq_2.c1 AS c0,
                    (
                        SELECT
                            l_returnflag
                        FROM
                            main.lineitem
                        LIMIT 1 offset 2) AS c1,
                    subq_1.c0 AS c2,
                    ref_6.c_address AS c3,
                    ref_6.c_comment AS c4
                FROM
                    main.customer AS ref_6,
                    LATERAL (
                        SELECT
                            subq_1.c0 AS c0,
                            subq_2.c1 AS c1,
                            ref_7.s_phone AS c2,
                            subq_1.c0 AS c3,
                            ref_7.s_suppkey AS c4,
                            (
                                SELECT
                                    s_suppkey
                                FROM
                                    main.supplier
                                LIMIT 1 offset 2) AS c5,
                            subq_1.c0 AS c6,
                            subq_1.c0 AS c7,
                            ref_7.s_phone AS c8,
                            subq_2.c1 AS c9,
                            subq_1.c0 AS c10,
                            ref_6.c_nationkey AS c11
                        FROM
                            main.supplier AS ref_7
                        WHERE
                            1) AS subq_3
                    WHERE ((EXISTS (
                                SELECT
                                    ref_8.r_regionkey AS c0,
                                    subq_2.c0 AS c1,
                                    subq_2.c1 AS c2,
                                    ref_6.c_comment AS c3,
                                    ref_6.c_mktsegment AS c4,
                                    ref_6.c_custkey AS c5
                                FROM
                                    main.region AS ref_8
                                WHERE
                                    subq_1.c0 IS NOT NULL))
                            AND (EXISTS (
                                    SELECT
                                        ref_6.c_address AS c0,
                                        subq_1.c0 AS c1,
                                        sample_8.r_name AS c2,
                                        (
                                            SELECT
                                                n_regionkey
                                            FROM
                                                main.nation
                                            LIMIT 1 offset 6) AS c3,
                                        subq_3.c9 AS c4,
                                        subq_1.c0 AS c5,
                                        ref_6.c_nationkey AS c6,
                                        sample_8.r_regionkey AS c7,
                                        31 AS c8,
                                        sample_8.r_name AS c9
                                    FROM
                                        main.region AS sample_8 TABLESAMPLE SYSTEM (2.6)
                                    WHERE
                                        0
                                    LIMIT 71)))
                        OR ((((EXISTS (
                                            SELECT
                                                ref_9.s_name AS c0,
                                                subq_2.c0 AS c1,
                                                ref_9.s_address AS c2
                                            FROM
                                                main.supplier AS ref_9
                                            WHERE
                                                0
                                            LIMIT 158))
                                    OR (subq_2.c0 IS NULL))
                                AND (ref_6.c_comment IS NOT NULL))
                            AND (ref_6.c_custkey IS NULL))
                    LIMIT 63) AS subq_4
            WHERE (1)
            AND ((subq_2.c1 IS NULL)
                AND ((
                        SELECT
                            c_address
                        FROM
                            main.customer
                        LIMIT 1 offset 2) IS NULL))
        LIMIT 115) AS subq_5
WHERE (0)
AND (0)
LIMIT 159
)
SELECT
    subq_6.c0 AS c0
FROM (
    SELECT
        ref_10.s_nationkey AS c0
    FROM
        main.supplier AS ref_10
        INNER JOIN main.supplier AS ref_11 ON (ref_10.s_nationkey = ref_11.s_suppkey)
    WHERE
        1
    LIMIT 153) AS subq_6
WHERE
    1
LIMIT 59
