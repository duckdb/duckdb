SELECT
    CASE WHEN CASE WHEN 0 THEN
            subq_1.c2
        ELSE
            subq_1.c2
        END IS NOT NULL THEN
        subq_1.c3
    ELSE
        subq_1.c3
    END AS c0,
    subq_1.c2 AS c1,
    subq_1.c2 AS c2,
    subq_1.c2 AS c3
FROM (
    SELECT
        subq_0.c3 AS c0,
        subq_0.c9 AS c1,
        subq_0.c9 AS c2,
        subq_0.c7 AS c3
    FROM (
        SELECT
            ref_0.c_nationkey AS c0,
            ref_0.c_name AS c1,
            ref_0.c_address AS c2,
            ref_0.c_name AS c3,
            ref_0.c_nationkey AS c4,
            ref_0.c_nationkey AS c5,
            ref_0.c_custkey AS c6,
            ref_0.c_address AS c7,
            ref_0.c_mktsegment AS c8,
            ref_0.c_phone AS c9
        FROM
            main.customer AS ref_0
        WHERE ((1)
            OR (((ref_0.c_comment IS NULL)
                    OR (ref_0.c_acctbal IS NOT NULL))
                AND ((EXISTS (
                            SELECT
                                ref_0.c_custkey AS c0, ref_0.c_mktsegment AS c1, ref_0.c_custkey AS c2, ref_1.p_name AS c3, ref_1.p_size AS c4, ref_1.p_retailprice AS c5, ref_0.c_comment AS c6
                            FROM
                                main.part AS ref_1
                            WHERE
                                1
                            LIMIT 85))
                    AND (ref_0.c_acctbal IS NOT NULL))))
        OR ((1)
            AND ((ref_0.c_name IS NOT NULL)
                OR (1)))
    LIMIT 83) AS subq_0
WHERE
    subq_0.c2 IS NULL) AS subq_1
WHERE ((EXISTS (
            SELECT
                ref_2.n_comment AS c0, subq_1.c2 AS c1, ref_2.n_nationkey AS c2, subq_1.c0 AS c3, subq_1.c0 AS c4, ref_2.n_name AS c5, subq_1.c2 AS c6, 16 AS c7, subq_1.c2 AS c8, ref_2.n_nationkey AS c9, ref_2.n_regionkey AS c10, subq_1.c3 AS c11, ref_2.n_nationkey AS c12, ref_2.n_nationkey AS c13, (
                    SELECT
                        p_mfgr
                    FROM
                        main.part
                    LIMIT 1 offset 81) AS c14,
                ref_2.n_nationkey AS c15,
                subq_1.c0 AS c16,
                subq_1.c2 AS c17,
                subq_1.c2 AS c18,
                subq_1.c2 AS c19
            FROM
                main.nation AS ref_2
            WHERE
                1
            LIMIT 131))
    AND (
        CASE WHEN ((subq_1.c2 IS NOT NULL)
                OR (subq_1.c0 IS NULL))
            AND (subq_1.c1 IS NULL) THEN
            subq_1.c3
        ELSE
            subq_1.c3
        END IS NULL))
OR ((0)
    OR ((((EXISTS (
                        SELECT
                            subq_1.c3 AS c0,
                            subq_1.c0 AS c1,
                            ref_3.ps_suppkey AS c2,
                            subq_1.c1 AS c3
                        FROM
                            main.partsupp AS ref_3
                        WHERE
                            subq_1.c1 IS NULL))
                    AND (((1)
                            AND (0))
                        OR (((1)
                                OR (((0)
                                        AND (1))
                                    AND ((
                                            SELECT
                                                p_comment
                                            FROM
                                                main.part
                                            LIMIT 1 offset 2)
                                        IS NOT NULL)))
                            AND (0))))
                OR (EXISTS (
                        SELECT
                            subq_1.c0 AS c0,
                            ref_4.n_comment AS c1,
                            ref_4.n_regionkey AS c2,
                            ref_4.n_regionkey AS c3,
                            ref_4.n_regionkey AS c4
                        FROM
                            main.nation AS ref_4
                        WHERE
                            0
                        LIMIT 32)))
            OR (((((subq_1.c3 IS NULL)
                            OR ((
                                    SELECT
                                        r_name
                                    FROM
                                        main.region
                                    LIMIT 1 offset 6)
                                IS NULL))
                        OR (EXISTS (
                                SELECT
                                    subq_1.c2 AS c0
                                FROM
                                    main.nation AS ref_5
                                WHERE (0)
                                AND (0))))
                    AND ((
                            SELECT
                                p_name
                            FROM
                                main.part
                            LIMIT 1 offset 3)
                        IS NULL))
                OR (EXISTS (
                        SELECT
                            subq_1.c2 AS c0,
                            ref_6.c_nationkey AS c1,
                            ref_6.c_acctbal AS c2,
                            subq_1.c2 AS c3,
                            subq_1.c2 AS c4,
                            ref_6.c_nationkey AS c5,
                            ref_6.c_comment AS c6,
                            subq_1.c1 AS c7,
                            subq_1.c0 AS c8,
                            ref_6.c_mktsegment AS c9,
                            subq_1.c0 AS c10,
                            subq_1.c2 AS c11,
                            ref_6.c_mktsegment AS c12,
                            subq_1.c3 AS c13
                        FROM
                            main.customer AS ref_6
                        WHERE
                            1)))))
    LIMIT 91
