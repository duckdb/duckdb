SELECT
    subq_1.c11 AS c0,
    subq_1.c10 AS c1
FROM (
    SELECT
        subq_0.c2 AS c0,
        subq_0.c1 AS c1,
        subq_0.c1 AS c2,
        subq_0.c2 AS c3,
        subq_0.c0 AS c4,
        (
            SELECT
                c_acctbal
            FROM
                main.customer
            LIMIT 1 offset 2) AS c5,
        subq_0.c1 AS c6,
        subq_0.c1 AS c7,
        92 AS c8,
        (
            SELECT
                l_partkey
            FROM
                main.lineitem
            LIMIT 1 offset 23) AS c9,
        subq_0.c2 AS c10,
        subq_0.c0 AS c11,
        CASE WHEN (subq_0.c2 IS NULL)
            OR ((subq_0.c2 IS NULL)
                OR (0)) THEN
            subq_0.c1
        ELSE
            subq_0.c1
        END AS c12,
        subq_0.c1 AS c13
    FROM (
        SELECT
            ref_0.s_address AS c0,
            ref_2.n_name AS c1,
            ref_1.l_linenumber AS c2
        FROM
            main.supplier AS ref_0
        LEFT JOIN main.lineitem AS ref_1
        RIGHT JOIN main.nation AS ref_2 ON (ref_2.n_regionkey IS NOT NULL) ON (ref_0.s_address = ref_1.l_returnflag)
    WHERE
        ref_1.l_quantity IS NOT NULL
    LIMIT 108) AS subq_0
WHERE ((((EXISTS (
                    SELECT
                        subq_0.c2 AS c0, (
                            SELECT
                                c_address
                            FROM
                                main.customer
                            LIMIT 1 offset 5) AS c1
                    FROM
                        main.lineitem AS ref_3
                    WHERE
                        ref_3.l_linenumber IS NULL
                    LIMIT 84))
            OR ((subq_0.c2 IS NOT NULL)
                AND (subq_0.c1 IS NOT NULL)))
        OR (0))
    OR ((
            SELECT
                o_shippriority
            FROM
                main.orders
            LIMIT 1 offset 3)
        IS NOT NULL))
OR ((((0)
            OR (1))
        AND ((subq_0.c0 IS NULL)
            OR (subq_0.c2 IS NOT NULL)))
    OR (subq_0.c1 IS NULL))
LIMIT 62) AS subq_1
WHERE (EXISTS (
        SELECT
            ref_4.ps_availqty AS c0, ref_4.ps_availqty AS c1, subq_1.c6 AS c2
        FROM
            main.partsupp AS ref_4
        WHERE
            CASE WHEN (0)
                AND (subq_1.c3 IS NULL) THEN
                ref_4.ps_supplycost
            ELSE
                ref_4.ps_supplycost
            END IS NOT NULL
        LIMIT 97))
OR ((subq_1.c11 IS NOT NULL)
    AND (CAST(coalesce(subq_1.c4, subq_1.c7) AS VARCHAR)
        IS NOT NULL))
LIMIT 137
