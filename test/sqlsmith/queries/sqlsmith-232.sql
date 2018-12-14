SELECT
    ref_0.s_address AS c0,
    ref_0.s_address AS c1,
    ref_0.s_suppkey AS c2,
    CASE WHEN ref_0.s_nationkey IS NULL THEN
        ref_0.s_suppkey
    ELSE
        ref_0.s_suppkey
    END AS c3,
    ref_0.s_name AS c4,
    ref_0.s_acctbal AS c5,
    CASE WHEN (1)
        OR ((ref_0.s_comment IS NOT NULL)
            OR ((ref_0.s_name IS NULL)
                AND (ref_0.s_name IS NOT NULL))) THEN
        CASE WHEN 0 THEN
            ref_0.s_name
        ELSE
            ref_0.s_name
        END
    ELSE
        CASE WHEN 0 THEN
            ref_0.s_name
        ELSE
            ref_0.s_name
        END
    END AS c6,
    CASE WHEN (ref_0.s_address IS NOT NULL)
        OR (1) THEN
        ref_0.s_address
    ELSE
        ref_0.s_address
    END AS c7,
    ref_0.s_suppkey AS c8,
    ref_0.s_suppkey AS c9,
    CASE WHEN ref_0.s_nationkey IS NOT NULL THEN
        ref_0.s_nationkey
    ELSE
        ref_0.s_nationkey
    END AS c10,
    ref_0.s_nationkey AS c11,
    CASE WHEN ((ref_0.s_comment IS NULL)
            AND (EXISTS (
                    SELECT
                        ref_1.p_comment AS c0,
                        ref_0.s_phone AS c1
                    FROM
                        main.part AS ref_1
                    WHERE (
                        SELECT
                            n_comment
                        FROM
                            main.nation
                        LIMIT 1 offset 6)
                    IS NOT NULL)))
        OR ((((ref_0.s_suppkey IS NULL)
                    AND ((ref_0.s_address IS NULL)
                        OR (ref_0.s_nationkey IS NULL)))
                AND (((0)
                        AND (0))
                    OR (1)))
            AND ((1)
                OR (((0)
                        AND ((0)
                            OR (1)))
                    AND (ref_0.s_name IS NULL)))) THEN
        ref_0.s_address
    ELSE
        ref_0.s_address
    END AS c12,
    ref_0.s_phone AS c13,
    ref_0.s_address AS c14,
    ref_0.s_suppkey AS c15,
    ref_0.s_phone AS c16,
    ref_0.s_suppkey AS c17,
    ref_0.s_name AS c18
FROM
    main.supplier AS ref_0
WHERE
    ref_0.s_address IS NOT NULL
LIMIT 63
