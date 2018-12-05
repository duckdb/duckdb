SELECT
    subq_1.c3 AS c0,
    subq_1.c0 AS c1,
    subq_1.c8 AS c2,
    subq_1.c1 AS c3,
    CAST(coalesce((
                SELECT
                    s_acctbal FROM main.supplier
                LIMIT 1 offset 4), CAST(NULL AS DECIMAL)) AS DECIMAL) AS c4,
    subq_1.c5 AS c5
FROM (
    SELECT
        subq_0.c1 AS c0,
        subq_0.c0 AS c1,
        CAST(coalesce(subq_0.c0, subq_0.c1) AS VARCHAR) AS c2,
        subq_0.c2 AS c3,
        (
            SELECT
                s_phone
            FROM
                main.supplier
            LIMIT 1 offset 2) AS c4,
        subq_0.c1 AS c5,
        subq_0.c0 AS c6,
        (
            SELECT
                l_receiptdate
            FROM
                main.lineitem
            LIMIT 1 offset 84) AS c7,
        subq_0.c1 AS c8
    FROM (
        SELECT
            ref_1.s_comment AS c0,
            ref_0.s_name AS c1,
            ref_0.s_comment AS c2
        FROM
            main.supplier AS ref_0
            INNER JOIN main.supplier AS ref_1
            INNER JOIN main.nation AS ref_2 ON (ref_1.s_suppkey = ref_2.n_nationkey) ON ((1)
                    OR (EXISTS (
                            SELECT
                                ref_0.s_acctbal AS c0,
                                ref_3.o_shippriority AS c1,
                                60 AS c2,
                                ref_3.o_orderstatus AS c3,
                                ref_2.n_regionkey AS c4
                            FROM
                                main.orders AS ref_3
                        WHERE
                            EXISTS (
                                SELECT
                                    ref_2.n_regionkey AS c0, ref_0.s_phone AS c1, ref_1.s_suppkey AS c2, ref_2.n_comment AS c3, ref_1.s_nationkey AS c4, 19 AS c5, ref_0.s_comment AS c6
                                FROM
                                    main.partsupp AS ref_4
                                WHERE (
                                    SELECT
                                        n_comment
                                    FROM
                                        main.nation
                                    LIMIT 1 offset 2)
                                IS NOT NULL)
                        LIMIT 48)))
        WHERE (((ref_1.s_comment IS NULL)
                OR (ref_1.s_nationkey IS NULL))
            OR (ref_2.n_name IS NULL))
        OR (ref_0.s_nationkey IS NULL)
    LIMIT 100) AS subq_0
WHERE
    CAST(coalesce(subq_0.c1, subq_0.c2) AS VARCHAR)
    IS NOT NULL
LIMIT 158) AS subq_1
WHERE
    1
LIMIT 147
