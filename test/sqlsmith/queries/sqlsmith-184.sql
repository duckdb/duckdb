SELECT
    subq_0.c3 AS c0,
    CASE WHEN subq_0.c0 IS NULL THEN
        subq_0.c3
    ELSE
        subq_0.c3
    END AS c1,
    subq_0.c0 AS c2,
    66 AS c3,
    CASE WHEN (((EXISTS (
                        SELECT
                            ref_5.n_regionkey AS c0,
                            21 AS c1,
                            ref_5.n_name AS c2
                        FROM
                            main.nation AS ref_5
                        WHERE (0)
                        OR (0)
                    LIMIT 139))
            OR ((((1)
                        AND ((subq_0.c2 IS NULL)
                            AND (EXISTS (
                                    SELECT
                                        subq_0.c3 AS c0,
                                        ref_6.c_phone AS c1,
                                        subq_0.c0 AS c2,
                                        32 AS c3,
                                        ref_6.c_acctbal AS c4,
                                        ref_6.c_acctbal AS c5,
                                        subq_0.c1 AS c6,
                                        ref_6.c_phone AS c7
                                    FROM
                                        main.customer AS ref_6
                                    WHERE
                                        0
                                    LIMIT 90))))
                    AND (EXISTS (
                            SELECT
                                ref_7.l_commitdate AS c0,
                                subq_0.c1 AS c1,
                                subq_0.c1 AS c2,
                                subq_0.c3 AS c3,
                                subq_0.c2 AS c4
                            FROM
                                main.lineitem AS ref_7
                            WHERE (((1)
                                    AND (((1)
                                            AND (0))
                                        OR ((EXISTS (
                                                    SELECT
                                                        ref_7.l_linestatus AS c0, ref_7.l_orderkey AS c1
                                                    FROM
                                                        main.supplier AS ref_8
                                                    WHERE
                                                        0
                                                    LIMIT 105))
                                            AND ((((
                                                            SELECT
                                                                l_comment
                                                            FROM
                                                                main.lineitem
                                                            LIMIT 1 offset 6)
                                                        IS NOT NULL)
                                                    OR (EXISTS (
                                                            SELECT
                                                                ref_9.ps_partkey AS c0,
                                                                ref_7.l_partkey AS c1,
                                                                ref_7.l_commitdate AS c2,
                                                                ref_7.l_partkey AS c3,
                                                                (
                                                                    SELECT
                                                                        r_name
                                                                    FROM
                                                                        main.region
                                                                    LIMIT 1 offset 65) AS c4,
                                                                ref_7.l_extendedprice AS c5,
                                                                ref_7.l_shipmode AS c6
                                                            FROM
                                                                main.partsupp AS ref_9
                                                            WHERE
                                                                ref_9.ps_suppkey IS NOT NULL)))
                                                    OR (((EXISTS (
                                                                    SELECT
                                                                        ref_10.n_regionkey AS c0, ref_10.n_name AS c1, subq_0.c1 AS c2
                                                                    FROM
                                                                        main.nation AS ref_10
                                                                    WHERE
                                                                        1
                                                                    LIMIT 174))
                                                            AND (ref_7.l_receiptdate IS NULL))
                                                        OR (((1)
                                                                OR ((1)
                                                                    AND (1)))
                                                            AND (1)))))))
                                    AND (EXISTS (
                                            SELECT
                                                subq_0.c1 AS c0
                                            FROM
                                                main.customer AS ref_11
                                            WHERE
                                                EXISTS (
                                                    SELECT
                                                        subq_0.c0 AS c0
                                                    FROM
                                                        main.lineitem AS ref_12
                                                    WHERE
                                                        subq_0.c2 IS NOT NULL
                                                    LIMIT 13)
                                            LIMIT 59)))
                                OR (ref_7.l_commitdate IS NULL))))
                    AND ((1)
                        OR (1))))
            OR ((
                    SELECT
                        r_regionkey
                    FROM
                        main.region
                    LIMIT 1 offset 5)
                IS NOT NULL))
        AND (((subq_0.c0 IS NOT NULL)
                AND (77 IS NOT NULL))
            OR (0)) THEN
        subq_0.c2
    ELSE
        subq_0.c2
    END AS c4,
    CAST(nullif (subq_0.c3, subq_0.c3) AS VARCHAR) AS c5
FROM (
    SELECT
        ref_0.o_clerk AS c0,
        (
            SELECT
                p_retailprice
            FROM
                main.part
            LIMIT 1 offset 4) AS c1,
        ref_0.o_orderkey AS c2,
        ref_1.n_comment AS c3
    FROM
        main.orders AS ref_0
        INNER JOIN main.nation AS ref_1 ON (ref_0.o_comment = ref_1.n_name)
    WHERE
        EXISTS (
            SELECT
                ref_1.n_regionkey AS c0, ref_1.n_comment AS c1, ref_3.s_suppkey AS c2
            FROM
                main.region AS ref_2
                INNER JOIN main.supplier AS ref_3
                LEFT JOIN main.lineitem AS ref_4 ON (ref_3.s_phone IS NULL) ON (ref_2.r_comment = ref_3.s_name)
            WHERE
                ref_1.n_comment IS NOT NULL
            LIMIT 75)
    LIMIT 125) AS subq_0
WHERE (subq_0.c3 IS NULL)
AND (((EXISTS (
                SELECT
                    subq_0.c3 AS c0, ref_13.ps_availqty AS c1, subq_0.c2 AS c2, ref_13.ps_supplycost AS c3, subq_0.c2 AS c4
                FROM
                    main.partsupp AS ref_13
                WHERE ((ref_13.ps_supplycost IS NULL)
                    OR (ref_13.ps_availqty IS NOT NULL))
                AND (((ref_13.ps_comment IS NULL)
                        AND ((1)
                            OR (1)))
                    OR (subq_0.c2 IS NOT NULL))
            LIMIT 64))
    OR ((subq_0.c0 IS NOT NULL)
        OR ((EXISTS (
                    SELECT
                        subq_0.c3 AS c0,
                        ref_14.r_regionkey AS c1,
                        ref_14.r_comment AS c2,
                        13 AS c3,
                        subq_0.c2 AS c4,
                        15 AS c5
                    FROM
                        main.region AS ref_14
                    WHERE (1)
                    AND (((ref_14.r_regionkey IS NOT NULL)
                            OR (1))
                        OR (1))
                LIMIT 112))
        OR ((
                SELECT
                    n_regionkey
                FROM
                    main.nation
                LIMIT 1 offset 5)
            IS NOT NULL))))
OR (((((((subq_0.c0 IS NULL)
                        AND (EXISTS (
                                SELECT
                                    86 AS c0
                                FROM
                                    main.region AS ref_15
                                WHERE
                                    1)))
                        AND ((subq_0.c1 IS NOT NULL)
                            AND (0)))
                    OR ((((subq_0.c2 IS NULL)
                                OR ((((1)
                                            AND (subq_0.c1 IS NOT NULL))
                                        OR (1))
                                    AND (((0)
                                            AND (1))
                                        AND (0))))
                            AND (((subq_0.c2 IS NULL)
                                    OR (0))
                                OR (((subq_0.c2 IS NOT NULL)
                                        AND ((1)
                                            OR (1)))
                                    AND (1))))
                        AND (0)))
                AND (subq_0.c0 IS NULL))
            OR (subq_0.c3 IS NOT NULL))
        AND ((1)
            AND (subq_0.c3 IS NOT NULL))))
LIMIT 161
