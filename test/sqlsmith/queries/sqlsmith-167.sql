SELECT
    (
        SELECT
            l_shipinstruct
        FROM
            main.lineitem
        LIMIT 1 offset 6) AS c0,
    subq_0.c3 AS c1,
    subq_0.c0 AS c2,
    subq_0.c0 AS c3,
    subq_0.c2 AS c4,
    78 AS c5,
    subq_0.c4 AS c6,
    (
        SELECT
            p_partkey
        FROM
            main.part
        LIMIT 1 offset 3) AS c7,
    subq_0.c5 AS c8,
    subq_0.c0 AS c9,
    subq_0.c2 AS c10,
    subq_0.c3 AS c11,
    subq_0.c5 AS c12,
    subq_0.c5 AS c13,
    subq_0.c2 AS c14,
    subq_0.c3 AS c15,
    subq_0.c2 AS c16,
    CAST(coalesce(subq_0.c5, subq_0.c4) AS INTEGER) AS c17,
    subq_0.c5 AS c18,
    subq_0.c0 AS c19,
    CASE WHEN (1)
        OR (0) THEN
        CASE WHEN subq_0.c1 IS NULL THEN
            subq_0.c1
        ELSE
            subq_0.c1
        END
    ELSE
        CASE WHEN subq_0.c1 IS NULL THEN
            subq_0.c1
        ELSE
            subq_0.c1
        END
    END AS c20,
    subq_0.c0 AS c21,
    CASE WHEN subq_0.c5 IS NULL THEN
        subq_0.c3
    ELSE
        subq_0.c3
    END AS c22,
    subq_0.c4 AS c23,
    subq_0.c2 AS c24,
    subq_0.c0 AS c25,
    subq_0.c3 AS c26,
    subq_0.c3 AS c27,
    subq_0.c1 AS c28,
    subq_0.c2 AS c29,
    CAST(nullif (subq_0.c5, subq_0.c2) AS INTEGER) AS c30
    FROM (
        SELECT
            ref_0.p_brand AS c0,
            ref_0.p_name AS c1,
            ref_0.p_size AS c2,
            ref_0.p_container AS c3,
            ref_0.p_size AS c4,
            ref_0.p_size AS c5
        FROM
            main.part AS ref_0
        WHERE (((0)
                AND (EXISTS (
                        SELECT
                            ref_1.l_comment AS c0
                        FROM
                            main.lineitem AS ref_1
                        WHERE
                            EXISTS (
                                SELECT
                                    ref_2.c_phone AS c0
                                FROM
                                    main.customer AS ref_2
                                WHERE
                                    83 IS NOT NULL)
                            LIMIT 112)))
                AND (((0)
                        AND ((ref_0.p_size IS NULL)
                            AND ((ref_0.p_partkey IS NOT NULL)
                                OR (EXISTS (
                                        SELECT
                                            ref_3.s_name AS c0,
                                            ref_3.s_name AS c1,
                                            18 AS c2,
                                            ref_3.s_phone AS c3,
                                            ref_0.p_size AS c4,
                                            ref_0.p_retailprice AS c5,
                                            ref_0.p_mfgr AS c6,
                                            ref_0.p_container AS c7,
                                            (
                                                SELECT
                                                    r_regionkey
                                                FROM
                                                    main.region
                                                LIMIT 1 offset 89) AS c8,
                                            ref_0.p_comment AS c9
                                        FROM
                                            main.supplier AS ref_3
                                        WHERE
                                            ref_3.s_comment IS NOT NULL
                                        LIMIT 65)))))
                    AND (0)))
            OR (EXISTS (
                    SELECT
                        ref_4.c_address AS c0,
                        ref_4.c_address AS c1,
                        ref_4.c_mktsegment AS c2,
                        ref_0.p_retailprice AS c3,
                        ref_0.p_type AS c4,
                        ref_4.c_address AS c5,
                        ref_4.c_address AS c6,
                        ref_0.p_type AS c7,
                        ref_4.c_custkey AS c8,
                        ref_4.c_acctbal AS c9
                    FROM
                        main.customer AS ref_4
                    WHERE
                        EXISTS (
                            SELECT
                                DISTINCT ref_4.c_phone AS c0, ref_4.c_phone AS c1, ref_4.c_custkey AS c2, ref_0.p_type AS c3, ref_5.s_acctbal AS c4
                            FROM
                                main.supplier AS ref_5
                            WHERE
                                ref_5.s_name IS NOT NULL
                            LIMIT 33)
                    LIMIT 32))) AS subq_0
WHERE
    CASE WHEN EXISTS (
            SELECT
                subq_0.c1 AS c0, subq_0.c3 AS c1, CAST(nullif (ref_7.c_address, ref_6.c_mktsegment) AS VARCHAR) AS c2, subq_0.c5 AS c3, ref_6.c_mktsegment AS c4, subq_0.c4 AS c5, subq_0.c5 AS c6, ref_6.c_name AS c7
            FROM
                main.customer AS ref_6
            RIGHT JOIN main.customer AS ref_7 ON (ref_7.c_custkey IS NULL)
        WHERE (0)
        AND (ref_7.c_address IS NULL)) THEN
    subq_0.c5
ELSE
    subq_0.c5
END IS NOT NULL
LIMIT 111
