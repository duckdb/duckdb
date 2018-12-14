SELECT
    CASE WHEN (subq_0.c0 IS NOT NULL)
        OR (0) THEN
        subq_0.c3
    ELSE
        subq_0.c3
    END AS c0,
    CASE WHEN 1 THEN
        14
    ELSE
        14
    END AS c1,
    subq_0.c2 AS c2,
    subq_0.c3 AS c3,
    subq_0.c0 AS c4,
    subq_0.c3 AS c5
FROM (
    SELECT
        ref_2.c_phone AS c0,
        CAST(coalesce(ref_1.r_name, ref_3.c_comment) AS VARCHAR) AS c1,
        ref_3.c_mktsegment AS c2,
        ref_3.c_name AS c3
    FROM
        main.customer AS ref_0
        INNER JOIN main.region AS ref_1
        INNER JOIN main.customer AS ref_2
        INNER JOIN main.customer AS ref_3
        LEFT JOIN main.orders AS ref_4 ON (ref_3.c_comment = ref_4.o_orderstatus) ON (ref_3.c_nationkey IS NOT NULL) ON (ref_1.r_comment = ref_3.c_name) ON (ref_0.c_name = ref_3.c_name)
    WHERE
        CASE WHEN ((ref_1.r_name IS NOT NULL)
                AND ((EXISTS (
                            SELECT
                                ref_1.r_comment AS c0
                            FROM
                                main.nation AS ref_5
                            WHERE
                                EXISTS (
                                    SELECT
                                        ref_3.c_nationkey AS c0, 31 AS c1, ref_6.l_partkey AS c2
                                    FROM
                                        main.lineitem AS ref_6
                                    WHERE (((1)
                                            OR (0))
                                        AND (1))
                                    OR (1)
                                LIMIT 138)))
                    OR ((1)
                        AND (0))))
            AND (EXISTS (
                    SELECT
                        ref_3.c_phone AS c0
                    FROM
                        main.partsupp AS ref_7
                    WHERE
                        EXISTS (
                            SELECT
                                ref_1.r_comment AS c0, ref_2.c_phone AS c1, ref_1.r_comment AS c2, ref_4.o_custkey AS c3, ref_8.c_address AS c4, ref_2.c_mktsegment AS c5
                            FROM
                                main.customer AS ref_8
                            WHERE
                                0
                            LIMIT 19)
                    LIMIT 46)) THEN
            ref_3.c_phone
        ELSE
            ref_3.c_phone
        END IS NOT NULL
    LIMIT 101) AS subq_0
WHERE
    CASE WHEN subq_0.c2 IS NULL THEN
        CASE WHEN (subq_0.c1 IS NULL)
            OR ((EXISTS (
                        SELECT
                            ref_18.r_regionkey AS c0, subq_0.c0 AS c1, ref_18.r_comment AS c2, subq_0.c0 AS c3, subq_0.c3 AS c4, ref_18.r_name AS c5, subq_0.c1 AS c6, ref_18.r_comment AS c7, subq_0.c0 AS c8, ref_18.r_name AS c9, 77 AS c10
                        FROM
                            main.region AS ref_18
                        WHERE
                            87 IS NOT NULL
                        LIMIT 99))
                OR (subq_0.c1 IS NOT NULL)) THEN
            subq_0.c3
        ELSE
            subq_0.c3
        END
    ELSE
        CASE WHEN (subq_0.c1 IS NULL)
            OR ((EXISTS (
                        SELECT
                            ref_18.r_regionkey AS c0,
                            subq_0.c0 AS c1,
                            ref_18.r_comment AS c2,
                            subq_0.c0 AS c3,
                            subq_0.c3 AS c4,
                            ref_18.r_name AS c5,
                            subq_0.c1 AS c6,
                            ref_18.r_comment AS c7,
                            subq_0.c0 AS c8,
                            ref_18.r_name AS c9,
                            77 AS c10
                        FROM
                            main.region AS ref_18
                        WHERE
                            87 IS NOT NULL
                        LIMIT 99))
                OR (subq_0.c1 IS NOT NULL)) THEN
            subq_0.c3
        ELSE
            subq_0.c3
        END
    END IS NOT NULL
LIMIT 46
