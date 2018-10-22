SELECT
    ref_0.s_name AS c0,
    ref_0.s_acctbal AS c1,
    ref_0.s_suppkey AS c2,
    24 AS c3,
    94 AS c4,
    ref_0.s_nationkey AS c5,
    CAST(nullif (ref_0.s_comment, CAST(nullif (ref_0.s_name, ref_0.s_name) AS VARCHAR)) AS VARCHAR) AS c6,
    ref_0.s_acctbal AS c7,
    CASE WHEN ref_0.s_name IS NOT NULL THEN
        ref_0.s_suppkey
    ELSE
        ref_0.s_suppkey
    END AS c8,
    CASE WHEN (((EXISTS (
                        SELECT
                            ref_1.o_orderpriority AS c0,
                            ref_1.o_orderpriority AS c1,
                            ref_0.s_acctbal AS c2,
                            ref_0.s_comment AS c3
                        FROM
                            main.orders AS ref_1
                        WHERE
                            ref_0.s_name IS NOT NULL
                        LIMIT 77))
                AND ((((ref_0.s_phone IS NULL)
                            OR ((ref_0.s_name IS NULL)
                                AND (((0)
                                        OR (0))
                                    OR (EXISTS (
                                            SELECT
                                                ref_0.s_nationkey AS c0
                                            FROM
                                                main.customer AS ref_2
                                            WHERE
                                                1
                                            LIMIT 149)))))
                        AND (EXISTS (
                                SELECT
                                    (
                                        SELECT
                                            p_container
                                        FROM
                                            main.part
                                        LIMIT 1 offset 2) AS c0,
                                    ref_0.s_address AS c1,
                                    ref_3.p_comment AS c2
                                FROM
                                    main.part AS ref_3
                                WHERE (0)
                                AND ((ref_0.s_address IS NOT NULL)
                                    OR (EXISTS (
                                            SELECT
                                                99 AS c0
                                            FROM
                                                main.lineitem AS ref_4
                                            WHERE
                                                1
                                            LIMIT 110)))
                            LIMIT 117)))
                AND (0)))
        AND (0))
    AND (
        CASE WHEN 93 IS NOT NULL THEN
            ref_0.s_suppkey
        ELSE
            ref_0.s_suppkey
        END IS NOT NULL) THEN
    ref_0.s_nationkey
ELSE
    ref_0.s_nationkey
END AS c9,
ref_0.s_name AS c10,
ref_0.s_phone AS c11,
ref_0.s_address AS c12
FROM
    main.supplier AS ref_0
WHERE (ref_0.s_address IS NOT NULL)
OR (((ref_0.s_suppkey IS NULL)
        AND (ref_0.s_comment IS NULL))
    OR (((1)
            OR (ref_0.s_nationkey IS NULL))
        OR ((1)
            AND ((1)
                OR ((((0)
                            OR ((0)
                                AND (1)))
                        OR (1))
                    OR (ref_0.s_comment IS NULL))))))
LIMIT 66
