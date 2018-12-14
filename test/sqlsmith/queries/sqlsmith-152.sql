SELECT
    subq_0.c0 AS c0,
    subq_0.c0 AS c1,
    CAST(coalesce(
            CASE WHEN 1 THEN
                subq_0.c0
            ELSE
                subq_0.c0
            END, subq_0.c0) AS VARCHAR) AS c2,
    subq_0.c0 AS c3,
    subq_0.c1 AS c4,
    subq_0.c0 AS c5,
    subq_0.c0 AS c6,
    CASE WHEN EXISTS (
            SELECT
                ref_1.p_container AS c0,
                ref_1.p_brand AS c1,
                ref_1.p_type AS c2,
                subq_0.c1 AS c3,
                ref_1.p_partkey AS c4,
                subq_0.c1 AS c5,
                subq_0.c0 AS c6,
                (
                    SELECT
                        s_nationkey
                    FROM
                        main.supplier
                    LIMIT 1 offset 2) AS c7,
                ref_1.p_name AS c8,
                ref_1.p_type AS c9,
                subq_0.c0 AS c10,
                subq_0.c1 AS c11,
                ref_1.p_comment AS c12,
                ref_1.p_size AS c13,
                subq_0.c0 AS c14,
                subq_0.c1 AS c15
            FROM
                main.part AS ref_1
            WHERE
                0) THEN
            5
        ELSE
            5
        END AS c7, subq_0.c0 AS c8
    FROM (
        SELECT
            ref_0.r_name AS c0,
            ref_0.r_name AS c1
        FROM
            main.region AS ref_0
        WHERE
            ref_0.r_comment IS NOT NULL) AS subq_0
WHERE (subq_0.c1 IS NULL)
OR ((EXISTS (
            SELECT
                ref_4.s_nationkey AS c0, subq_0.c0 AS c1, subq_0.c0 AS c2, ref_4.s_address AS c3, subq_0.c0 AS c4, ref_4.s_comment AS c5, subq_0.c0 AS c6, subq_0.c1 AS c7, ref_4.s_address AS c8, ref_4.s_nationkey AS c9, ref_4.s_acctbal AS c10, subq_0.c0 AS c11
            FROM
                main.supplier AS ref_4
            WHERE
                EXISTS (
                    SELECT
                        subq_0.c0 AS c0
                    FROM
                        main.nation AS ref_5
                    WHERE
                        0)
                LIMIT 75))
        OR (subq_0.c0 IS NULL))
