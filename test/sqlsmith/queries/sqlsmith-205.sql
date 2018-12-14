SELECT
    ref_0.c_acctbal AS c0,
    CASE WHEN ((ref_0.c_address IS NOT NULL)
            AND (ref_0.c_acctbal IS NOT NULL))
        AND (ref_0.c_nationkey IS NOT NULL) THEN
        ref_0.c_nationkey
    ELSE
        ref_0.c_nationkey
    END AS c1,
    ref_0.c_phone AS c2,
    CASE WHEN (((0)
                OR (((ref_0.c_comment IS NOT NULL)
                        AND (((
                                    SELECT
                                        c_acctbal
                                    FROM
                                        main.customer
                                    LIMIT 1 offset 5)
                                IS NOT NULL)
                            OR (((
                                        SELECT
                                            c_address
                                        FROM
                                            main.customer
                                        LIMIT 1 offset 5)
                                    IS NOT NULL)
                                AND ((1)
                                    OR (1)))))
                    AND (1)))
            AND (ref_0.c_custkey IS NULL))
        AND (ref_0.c_mktsegment IS NULL) THEN
        ref_0.c_nationkey
    ELSE
        ref_0.c_nationkey
    END AS c3,
    (
        SELECT
            c_acctbal
        FROM
            main.customer
        LIMIT 1 offset 46) AS c4
FROM
    main.customer AS ref_0
WHERE
    EXISTS (
        SELECT
            ref_1.s_phone AS c0, ref_1.s_suppkey AS c1, CASE WHEN ref_0.c_acctbal IS NULL THEN
                ref_1.s_suppkey
            ELSE
                ref_1.s_suppkey
            END AS c2, ref_0.c_phone AS c3, ref_0.c_comment AS c4
        FROM
            main.supplier AS ref_1
        WHERE (
            SELECT
                n_regionkey
            FROM
                main.nation
            LIMIT 1 offset 5)
        IS NOT NULL)
