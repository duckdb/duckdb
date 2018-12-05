SELECT
    CASE WHEN subq_1.c4 IS NOT NULL THEN
        CAST(nullif (subq_1.c3, subq_1.c1) AS VARCHAR)
    ELSE
        CAST(nullif (subq_1.c3, subq_1.c1) AS VARCHAR)
    END AS c0,
    (
        SELECT
            n_regionkey
        FROM
            main.nation
        LIMIT 1 offset 1) AS c1,
    subq_1.c2 AS c2,
    subq_1.c3 AS c3,
    subq_1.c4 AS c4,
    CASE WHEN 0 THEN
        subq_1.c4
    ELSE
        subq_1.c4
    END AS c5
FROM (
    SELECT
        ref_0.r_name AS c0,
        ref_1.o_comment AS c1,
        72 AS c2,
        ref_0.r_comment AS c3,
        ref_0.r_regionkey AS c4
    FROM
        main.region AS ref_0
    LEFT JOIN main.orders AS ref_1 ON (((1)
                OR (((((((ref_0.r_regionkey IS NULL)
                                        AND ((EXISTS (
                                                    SELECT
                                                        ref_2.p_retailprice AS c0,
                                                        ref_1.o_custkey AS c1,
                                                        (
                                                            SELECT
                                                                l_commitdate
                                                            FROM
                                                                main.lineitem
                                                            LIMIT 1 offset 5) AS c2,
                                                        ref_0.r_name AS c3
                                                    FROM
                                                        main.part AS ref_2
                                                WHERE
                                                    EXISTS (
                                                        SELECT
                                                            ref_3.ps_comment AS c0, ref_0.r_comment AS c1, (
                                                                SELECT
                                                                    r_comment
                                                                FROM
                                                                    main.region
                                                                LIMIT 1 offset 4) AS c2
                                                        FROM
                                                            main.partsupp AS ref_3
                                                        WHERE (EXISTS (
                                                                SELECT
                                                                    42 AS c0, ref_0.r_name AS c1, (
                                                                        SELECT
                                                                            l_receiptdate
                                                                        FROM
                                                                            main.lineitem
                                                                        LIMIT 1 offset 2) AS c2,
                                                                    ref_4.s_phone AS c3,
                                                                    ref_3.ps_supplycost AS c4
                                                                FROM
                                                                    main.supplier AS ref_4
                                                                WHERE
                                                                    0))
                                                            AND (((0)
                                                                    OR ((0)
                                                                        AND (EXISTS (
                                                                                SELECT
                                                                                    ref_1.o_orderdate AS c0, ref_1.o_custkey AS c1, ref_0.r_regionkey AS c2, ref_5.n_comment AS c3
                                                                                FROM
                                                                                    main.nation AS ref_5
                                                                                WHERE (ref_5.n_name IS NOT NULL)
                                                                                OR (ref_2.p_container IS NULL)
                                                                            LIMIT 132))))
                                                            OR (ref_0.r_regionkey IS NULL))
                                                    LIMIT 126)
                                            LIMIT 134))
                                    AND (1)))
                            AND (0))
                        OR (EXISTS (
                                SELECT
                                    ref_0.r_regionkey AS c0,
                                    ref_1.o_clerk AS c1,
                                    ref_0.r_comment AS c2,
                                    ref_6.p_type AS c3,
                                    ref_1.o_orderdate AS c4
                                FROM
                                    main.part AS ref_6
                                WHERE ((((((1)
                                                    AND ((1)
                                                        OR ((1)
                                                            AND (ref_6.p_type IS NOT NULL))))
                                                AND ((0)
                                                    AND (0)))
                                            AND (EXISTS (
                                                    SELECT
                                                        ref_7.r_regionkey AS c0, ref_0.r_regionkey AS c1, 1 AS c2, ref_7.r_regionkey AS c3, ref_1.o_totalprice AS c4, 81 AS c5
                                                    FROM
                                                        main.region AS ref_7
                                                    WHERE
                                                        1
                                                    LIMIT 54)))
                                        OR (0))
                                    AND (EXISTS (
                                            SELECT
                                                ref_6.p_name AS c0,
                                                ref_6.p_container AS c1,
                                                ref_6.p_partkey AS c2
                                            FROM
                                                main.region AS ref_8
                                            WHERE
                                                1
                                            LIMIT 108)))
                                AND ((0)
                                    AND (0))
                            LIMIT 114)))
                OR (1))
            AND (0))
        OR ((ref_1.o_custkey IS NOT NULL)
            AND (0))))
AND (ref_0.r_name IS NULL)),
LATERAL (
    SELECT
        (
            SELECT
                n_comment
            FROM
                main.nation
            LIMIT 1 offset 5) AS c0,
        ref_9.n_nationkey AS c1
    FROM
        main.nation AS ref_9
    WHERE
        ref_9.n_name IS NOT NULL) AS subq_0
WHERE (ref_0.r_comment IS NOT NULL)
OR ((1)
    OR (ref_0.r_name IS NULL))
LIMIT 151) AS subq_1
WHERE
    1
LIMIT 66
