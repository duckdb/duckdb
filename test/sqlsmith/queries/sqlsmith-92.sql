SELECT
    subq_0.c3 AS c0,
    subq_0.c5 AS c1,
    subq_0.c9 AS c2
FROM (
    SELECT
        ref_0.r_name AS c0,
        ref_0.r_name AS c1,
        ref_0.r_comment AS c2,
        ref_0.r_comment AS c3,
        ref_0.r_comment AS c4,
        ref_0.r_regionkey AS c5,
        ref_0.r_name AS c6,
        ref_0.r_comment AS c7,
        CAST(coalesce(
                CASE WHEN 1 THEN
                    ref_0.r_regionkey
                ELSE
                    ref_0.r_regionkey
                END, ref_0.r_regionkey) AS INTEGER) AS c8,
        ref_0.r_name AS c9,
        ref_0.r_name AS c10
    FROM
        main.region AS ref_0
    WHERE (ref_0.r_name IS NOT NULL)
    OR (0)
LIMIT 81) AS subq_0
WHERE (EXISTS (
        SELECT
            ref_1.r_name AS c0, ref_1.r_regionkey AS c1, ref_1.r_regionkey AS c2, subq_0.c2 AS c3
        FROM
            main.region AS ref_1
        WHERE
            subq_0.c8 IS NOT NULL))
    OR ((
            CASE WHEN subq_0.c4 IS NOT NULL THEN
                CAST(nullif (subq_0.c4, subq_0.c3) AS VARCHAR)
            ELSE
                CAST(nullif (subq_0.c4, subq_0.c3) AS VARCHAR)
            END IS NOT NULL)
        AND (((EXISTS (
                        SELECT
                            ref_3.s_phone AS c0
                        FROM
                            main.part AS ref_2
                        LEFT JOIN main.supplier AS ref_3 ON (ref_2.p_type = ref_3.s_name)
                    WHERE (0)
                    AND (EXISTS (
                            SELECT
                                ref_4.p_size AS c0, ref_3.s_comment AS c1, subq_0.c1 AS c2, ref_3.s_acctbal AS c3, subq_0.c9 AS c4, ref_3.s_nationkey AS c5, (
                                    SELECT
                                        ps_comment
                                    FROM
                                        main.partsupp
                                    LIMIT 1 offset 6) AS c6,
                                ref_2.p_retailprice AS c7,
                                ref_2.p_partkey AS c8,
                                (
                                    SELECT
                                        n_nationkey
                                    FROM
                                        main.nation
                                    LIMIT 1 offset 4) AS c9
                            FROM
                                main.part AS ref_4
                            WHERE ((1)
                                AND ((EXISTS (
                                            SELECT
                                                subq_0.c3 AS c0, (
                                                    SELECT
                                                        s_address
                                                    FROM
                                                        main.supplier
                                                    LIMIT 1 offset 41) AS c1,
                                                subq_0.c9 AS c2,
                                                ref_4.p_name AS c3,
                                                ref_4.p_brand AS c4,
                                                ref_3.s_suppkey AS c5
                                            FROM
                                                main.part AS ref_5
                                            WHERE
                                                ref_5.p_container IS NOT NULL
                                            LIMIT 69))
                                    OR (1)))
                            OR (EXISTS (
                                    SELECT
                                        ref_2.p_container AS c0,
                                        (
                                            SELECT
                                                s_phone
                                            FROM
                                                main.supplier
                                            LIMIT 1 offset 6) AS c1,
                                        ref_3.s_suppkey AS c2,
                                        ref_4.p_type AS c3,
                                        ref_2.p_name AS c4,
                                        ref_3.s_nationkey AS c5,
                                        ref_3.s_address AS c6,
                                        ref_6.l_returnflag AS c7,
                                        subq_0.c3 AS c8,
                                        ref_4.p_retailprice AS c9,
                                        subq_0.c7 AS c10,
                                        ref_4.p_mfgr AS c11,
                                        subq_0.c6 AS c12,
                                        5 AS c13
                                    FROM
                                        main.lineitem AS ref_6
                                    WHERE
                                        EXISTS (
                                            SELECT
                                                91 AS c0, subq_0.c0 AS c1, ref_7.r_comment AS c2, ref_7.r_comment AS c3
                                            FROM
                                                main.region AS ref_7
                                            WHERE (0)
                                            OR ((ref_4.p_name IS NULL)
                                                AND (1))
                                        LIMIT 187)
                                LIMIT 154))))))
        AND (1))
    AND ((subq_0.c1 IS NULL)
        OR (((subq_0.c2 IS NOT NULL)
                OR (subq_0.c3 IS NULL))
            OR ((1)
                AND (0))))))
LIMIT 168
