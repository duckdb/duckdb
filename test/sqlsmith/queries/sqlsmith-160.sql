WITH jennifer_0 AS (
    SELECT
        subq_1.c0 AS c0,
        subq_1.c0 AS c1,
        subq_0.c1 AS c2,
        subq_1.c0 AS c3,
        subq_1.c0 AS c4,
        36 AS c5,
        subq_1.c0 AS c6,
        CASE WHEN EXISTS (
                SELECT
                    subq_0.c3 AS c0,
                    subq_0.c1 AS c1,
                    subq_0.c0 AS c2,
                    subq_1.c0 AS c3,
                    subq_1.c0 AS c4,
                    subq_1.c0 AS c5,
                    subq_0.c0 AS c6,
                    ref_3.o_shippriority AS c7,
                    ref_3.o_orderdate AS c8,
                    subq_1.c0 AS c9,
                    subq_1.c0 AS c10,
                    ref_3.o_clerk AS c11,
                    subq_0.c2 AS c12
                FROM
                    main.orders AS ref_3
                WHERE
                    1
                LIMIT 136) THEN
            subq_1.c0
        ELSE
            subq_1.c0
        END AS c7,
        subq_0.c1 AS c8,
        subq_0.c0 AS c9,
        subq_1.c0 AS c10,
        subq_0.c0 AS c11,
        subq_1.c0 AS c12,
        subq_0.c2 AS c13,
        subq_1.c0 AS c14,
        subq_1.c0 AS c15,
        subq_1.c0 AS c16,
        subq_1.c0 AS c17,
        subq_0.c2 AS c18,
        subq_0.c0 AS c19,
        subq_0.c1 AS c20,
        subq_0.c0 AS c21,
        subq_0.c3 AS c22
    FROM (
        SELECT
            ref_0.s_address AS c0,
            ref_0.s_acctbal AS c1,
            ref_0.s_name AS c2,
            ref_0.s_address AS c3
        FROM
            main.supplier AS ref_0
        WHERE (ref_0.s_suppkey IS NOT NULL)
        AND (ref_0.s_acctbal IS NOT NULL)
    LIMIT 149) AS subq_0
    INNER JOIN (
        SELECT
            ref_1.n_name AS c0
        FROM
            main.nation AS ref_1
            INNER JOIN main.orders AS ref_2 ON (ref_2.o_orderstatus IS NOT NULL)
        WHERE
            ref_1.n_nationkey IS NOT NULL
        LIMIT 91) AS subq_1 ON (subq_0.c0 = subq_1.c0)
WHERE
    85 IS NOT NULL
LIMIT 165
)
SELECT
    subq_2.c0 AS c0,
    (
        SELECT
            c_phone
        FROM
            main.customer
        LIMIT 1 offset 5) AS c1,
    subq_2.c0 AS c2,
    subq_2.c0 AS c3,
    subq_2.c0 AS c4,
    subq_2.c0 AS c5
FROM (
    SELECT
        CASE WHEN ((0)
                OR (1))
            OR (EXISTS (
                    SELECT
                        ref_5.c_nationkey AS c0,
                        ref_5.c_comment AS c1,
                        ref_5.c_phone AS c2,
                        ref_5.c_name AS c3,
                        ref_6.c15 AS c4,
                        ref_5.c_acctbal AS c5,
                        ref_6.c0 AS c6,
                        ref_6.c5 AS c7,
                        ref_6.c4 AS c8
                    FROM
                        jennifer_0 AS ref_6
                    WHERE
                        ref_5.c_nationkey IS NOT NULL)) THEN
                ref_5.c_phone
            ELSE
                ref_5.c_phone
            END AS c0
        FROM
            main.customer AS ref_5
        WHERE
            1) AS subq_2
WHERE (0)
OR (0)
