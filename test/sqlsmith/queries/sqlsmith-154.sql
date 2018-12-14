SELECT
    subq_0.c0 AS c0
FROM
    main.lineitem AS ref_0
    LEFT JOIN (
        SELECT
            CAST(coalesce(ref_1.c_address, ref_1.c_mktsegment) AS VARCHAR) AS c0
        FROM
            main.customer AS ref_1
        WHERE (ref_1.c_address IS NULL)
        OR ((ref_1.c_comment IS NULL)
            OR ((0)
                OR (1)))
    LIMIT 81) AS subq_0 ON (ref_0.l_comment = subq_0.c0)
WHERE (
    CASE WHEN 1 THEN
        ref_0.l_orderkey
    ELSE
        ref_0.l_orderkey
    END IS NOT NULL)
OR ((((EXISTS (
                    SELECT
                        ref_0.l_returnflag AS c0, subq_0.c0 AS c1, ref_2.p_type AS c2, ref_2.p_partkey AS c3, subq_0.c0 AS c4
                    FROM
                        main.part AS ref_2
                    WHERE
                        1
                    LIMIT 92))
            AND (subq_0.c0 IS NOT NULL))
        OR ((
                SELECT
                    n_nationkey
                FROM
                    main.nation
                LIMIT 1 offset 6)
            IS NULL))
    OR (EXISTS (
            SELECT
                ref_3.r_name AS c0,
                ref_0.l_discount AS c1,
                ref_3.r_comment AS c2
            FROM
                main.region AS ref_3
            WHERE
                ref_0.l_comment IS NOT NULL)))
LIMIT 117
