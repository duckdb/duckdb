SELECT
    subq_1.c0 AS c0,
    subq_1.c0 AS c1,
    subq_1.c0 AS c2,
    subq_1.c0 AS c3,
    subq_1.c0 AS c4,
    subq_1.c0 AS c5,
    CASE WHEN (subq_1.c0 IS NOT NULL)
        OR (((EXISTS (
                        SELECT
                            49 AS c0
                        FROM
                            main.customer AS ref_1
                        WHERE
                            EXISTS (
                                SELECT
                                    ref_2.r_name AS c0, ref_1.c_acctbal AS c1, ref_1.c_acctbal AS c2, ref_2.r_regionkey AS c3
                                FROM
                                    main.region AS ref_2
                                WHERE
                                    0
                                LIMIT 125)
                        LIMIT 69))
                AND (1))
            OR (EXISTS (
                    SELECT
                        77 AS c0,
                        ref_3.n_name AS c1,
                        ref_4.r_regionkey AS c2,
                        subq_1.c0 AS c3
                    FROM
                        main.nation AS ref_3
                        INNER JOIN main.region AS ref_4
                        INNER JOIN main.region AS ref_5 ON (ref_5.r_name IS NULL) ON (ref_3.n_comment = ref_4.r_name)
                    WHERE
                        subq_1.c0 IS NOT NULL
                    LIMIT 127))) THEN
        subq_1.c0
    ELSE
        subq_1.c0
    END AS c6,
    CAST(coalesce(53, subq_1.c0) AS INTEGER) AS c7,
    subq_1.c0 AS c8,
    subq_1.c0 AS c9,
    CASE WHEN (
            SELECT
                s_nationkey
            FROM
                main.supplier
            LIMIT 1 offset 3)
    IS NULL THEN
    subq_1.c0
ELSE
    subq_1.c0
END AS c10,
subq_1.c0 AS c11,
subq_1.c0 AS c12,
subq_1.c0 AS c13,
subq_1.c0 AS c14
FROM (
    SELECT
        subq_0.c4 AS c0
    FROM (
        SELECT
            (
                SELECT
                    o_orderpriority
                FROM
                    main.orders
                LIMIT 1 offset 5) AS c0,
            ref_0.o_orderkey AS c1,
            ref_0.o_orderdate AS c2,
            ref_0.o_orderdate AS c3,
            ref_0.o_orderkey AS c4,
            ref_0.o_orderkey AS c5
        FROM
            main.orders AS ref_0
        WHERE
            ref_0.o_totalprice IS NOT NULL) AS subq_0
    WHERE (subq_0.c1 IS NULL)
    OR (subq_0.c0 IS NOT NULL)) AS subq_1
WHERE
    subq_1.c0 IS NOT NULL
LIMIT 70
