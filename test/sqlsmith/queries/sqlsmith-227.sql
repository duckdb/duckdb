SELECT
    ref_0.r_regionkey AS c0,
    CAST(nullif (ref_0.r_name, CAST(coalesce(
                    CASE WHEN 0 THEN
                        ref_0.r_comment
                    ELSE
                        ref_0.r_comment
                    END, ref_0.r_comment) AS VARCHAR)) AS VARCHAR) AS c1,
    (
        SELECT
            o_shippriority
        FROM
            main.orders
        LIMIT 1 offset 2) AS c2,
    CASE WHEN (((1)
                OR (EXISTS (
                        SELECT
                            ref_1.l_discount AS c0,
                            ref_1.l_quantity AS c1,
                            (
                                SELECT
                                    c_name
                                FROM
                                    main.customer
                                LIMIT 1 offset 87) AS c2,
                            ref_0.r_comment AS c3,
                            ref_0.r_comment AS c4,
                            ref_0.r_comment AS c5,
                            ref_0.r_comment AS c6
                        FROM
                            main.lineitem AS ref_1
                        WHERE
                            ref_1.l_commitdate IS NOT NULL)))
                OR ((((1)
                            OR (EXISTS (
                                    SELECT
                                        ref_2.c_acctbal AS c0, ref_2.c_address AS c1
                                    FROM
                                        main.customer AS ref_2
                                    WHERE
                                        EXISTS (
                                            SELECT
                                                ref_2.c_acctbal AS c0, ref_0.r_name AS c1, ref_2.c_address AS c2, 48 AS c3
                                            FROM
                                                main.part AS ref_3
                                            WHERE (ref_3.p_comment IS NOT NULL)
                                            AND (((ref_2.c_mktsegment IS NOT NULL)
                                                    AND (((0)
                                                            OR (ref_2.c_name IS NULL))
                                                        AND ((1)
                                                            OR (0))))
                                                AND (ref_2.c_mktsegment IS NULL)))
                                    LIMIT 53)))
                        AND (EXISTS (
                                SELECT
                                    ref_0.r_regionkey AS c0,
                                    ref_4.p_size AS c1,
                                    ref_4.p_partkey AS c2,
                                    ref_0.r_name AS c3,
                                    ref_4.p_mfgr AS c4,
                                    ref_4.p_size AS c5,
                                    ref_0.r_regionkey AS c6
                                FROM
                                    main.part AS ref_4
                                WHERE (EXISTS (
                                        SELECT
                                            ref_0.r_comment AS c0, ref_5.c_comment AS c1, ref_0.r_comment AS c2, (
                                                SELECT
                                                    o_orderpriority
                                                FROM
                                                    main.orders
                                                LIMIT 1 offset 4) AS c3,
                                            ref_0.r_name AS c4,
                                            ref_4.p_container AS c5,
                                            ref_5.c_comment AS c6,
                                            ref_4.p_container AS c7
                                        FROM
                                            main.customer AS ref_5
                                        WHERE (0)
                                        AND (EXISTS (
                                                SELECT
                                                    ref_6.ps_supplycost AS c0, ref_5.c_comment AS c1
                                                FROM
                                                    main.partsupp AS ref_6
                                                WHERE
                                                    EXISTS (
                                                        SELECT
                                                            ref_0.r_name AS c0, ref_7.ps_suppkey AS c1
                                                        FROM
                                                            main.partsupp AS ref_7
                                                        WHERE (ref_5.c_custkey IS NOT NULL)
                                                        OR ((1)
                                                            AND ((0)
                                                                AND ((0)
                                                                    AND (EXISTS (
                                                                            SELECT
                                                                                ref_4.p_mfgr AS c0
                                                                            FROM
                                                                                main.partsupp AS ref_8
                                                                            WHERE ((0)
                                                                                AND (((1)
                                                                                        OR (1))
                                                                                    OR ((ref_4.p_brand IS NOT NULL)
                                                                                        OR ((0)
                                                                                            AND ((ref_7.ps_availqty IS NULL)
                                                                                                OR (0))))))
                                                                            OR (ref_6.ps_suppkey IS NOT NULL)
                                                                        LIMIT 137)))))
                                                LIMIT 54)
                                        LIMIT 128))))
                        AND ((1)
                            AND (1))
                    LIMIT 86)))
        OR ((1)
            AND ((1)
                AND ((EXISTS (
                            SELECT
                                ref_9.c_comment AS c0,
                                ref_9.c_mktsegment AS c1,
                                ref_0.r_name AS c2,
                                ref_0.r_comment AS c3
                            FROM
                                main.customer AS ref_9
                            WHERE ((0)
                                AND ((
                                        SELECT
                                            r_regionkey
                                        FROM
                                            main.region
                                        LIMIT 1 offset 5)
                                    IS NULL))
                            AND (ref_9.c_comment IS NOT NULL)
                        LIMIT 90))
                OR (1))))))
AND (EXISTS (
        SELECT
            ref_0.r_regionkey AS c0
        FROM
            main.part AS ref_10
        WHERE
            0
        LIMIT 101)) THEN
ref_0.r_name
ELSE
    ref_0.r_name
END AS c3,
93 AS c4,
ref_0.r_comment AS c5,
ref_0.r_name AS c6
FROM
    main.region AS ref_0
WHERE (ref_0.r_name IS NOT NULL)
OR (ref_0.r_comment IS NOT NULL)
