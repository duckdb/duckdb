SELECT
    (
        SELECT
            l_discount
        FROM
            main.lineitem
        LIMIT 1 offset 6) AS c0,
    subq_0.c18 AS c1,
    (
        SELECT
            c_mktsegment
        FROM
            main.customer
        LIMIT 1 offset 15) AS c2,
    subq_0.c13 AS c3,
    CAST(nullif (subq_0.c26, subq_0.c27) AS VARCHAR) AS c4,
    subq_0.c2 AS c5,
    subq_0.c18 AS c6,
    subq_0.c27 AS c7,
    subq_0.c18 AS c8,
    subq_0.c21 AS c9,
    subq_0.c11 AS c10,
    subq_0.c9 AS c11,
    subq_0.c22 AS c12,
    subq_0.c7 AS c13,
    subq_0.c7 AS c14,
    17 AS c15,
    CASE WHEN subq_0.c22 IS NULL THEN
        subq_0.c8
    ELSE
        subq_0.c8
    END AS c16,
    CASE WHEN EXISTS (
            SELECT
                ref_1.l_linestatus AS c0,
                subq_0.c13 AS c1,
                ref_1.l_quantity AS c2,
                ref_1.l_receiptdate AS c3,
                ref_1.l_shipdate AS c4,
                ref_1.l_extendedprice AS c5,
                subq_0.c1 AS c6,
                subq_0.c3 AS c7,
                ref_1.l_returnflag AS c8
            FROM
                main.lineitem AS ref_1
            WHERE (0)
            OR (((subq_0.c21 IS NOT NULL)
                    OR ((EXISTS (
                                SELECT
                                    ref_2.c_acctbal AS c0, ref_2.c_name AS c1, ref_2.c_mktsegment AS c2, ref_1.l_quantity AS c3, subq_0.c11 AS c4, ref_2.c_phone AS c5, ref_2.c_comment AS c6, ref_2.c_comment AS c7, subq_0.c13 AS c8, ref_1.l_shipdate AS c9
                                FROM
                                    main.customer AS ref_2
                                WHERE
                                    EXISTS (
                                        SELECT
                                            ref_2.c_phone AS c0, ref_3.n_regionkey AS c1
                                        FROM
                                            main.nation AS ref_3
                                        WHERE
                                            1)))
                                AND (1)))
                        OR (1))
                LIMIT 162) THEN
            subq_0.c11
        ELSE
            subq_0.c11
        END AS c17,
        subq_0.c26 AS c18,
        CASE WHEN (((subq_0.c21 IS NOT NULL)
                    OR (subq_0.c5 IS NULL))
                AND (EXISTS (
                        SELECT
                            subq_0.c24 AS c0,
                            subq_0.c6 AS c1,
                            ref_4.r_name AS c2,
                            subq_0.c11 AS c3
                        FROM
                            main.region AS ref_4
                        WHERE
                            0
                        LIMIT 110)))
            OR (subq_0.c18 IS NULL) THEN
            CAST(coalesce(13, subq_0.c0) AS INTEGER)
    ELSE
        CAST(coalesce(13, subq_0.c0) AS INTEGER)
    END AS c19
FROM (
    SELECT
        ref_0.r_regionkey AS c0,
        ref_0.r_comment AS c1,
        ref_0.r_comment AS c2,
        ref_0.r_comment AS c3,
        ref_0.r_regionkey AS c4,
        ref_0.r_regionkey AS c5,
        ref_0.r_regionkey AS c6,
        (
            SELECT
                n_regionkey
            FROM
                main.nation
            LIMIT 1 offset 3) AS c7,
        ref_0.r_comment AS c8,
        ref_0.r_comment AS c9,
        ref_0.r_regionkey AS c10,
        CASE WHEN 0 THEN
            ref_0.r_name
        ELSE
            ref_0.r_name
        END AS c11,
        ref_0.r_name AS c12,
        (
            SELECT
                r_regionkey
            FROM
                main.region
            LIMIT 1 offset 6) AS c13,
        ref_0.r_name AS c14,
        ref_0.r_name AS c15,
        (
            SELECT
                p_type
            FROM
                main.part
            LIMIT 1 offset 80) AS c16,
        ref_0.r_name AS c17,
        ref_0.r_regionkey AS c18,
        ref_0.r_regionkey AS c19,
        ref_0.r_comment AS c20,
        ref_0.r_regionkey AS c21,
        ref_0.r_name AS c22,
        ref_0.r_comment AS c23,
        ref_0.r_name AS c24,
        ref_0.r_name AS c25,
        ref_0.r_comment AS c26,
        ref_0.r_comment AS c27
    FROM
        main.region AS ref_0
    WHERE
        1
    LIMIT 95) AS subq_0
WHERE
    subq_0.c15 IS NOT NULL
LIMIT 115
