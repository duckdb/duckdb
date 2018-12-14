SELECT
    subq_0.c0 AS c0
FROM (
    SELECT
        ref_1.n_regionkey AS c0,
        ref_1.n_regionkey AS c1
    FROM
        main.supplier AS ref_0
    RIGHT JOIN main.nation AS ref_1
    INNER JOIN main.lineitem AS ref_2 ON ((1)
            OR (1)) ON (ref_0.s_address = ref_1.n_name)
WHERE ((((EXISTS (
                    SELECT
                        79 AS c0, ref_1.n_comment AS c1, ref_0.s_address AS c2, ref_2.l_extendedprice AS c3
                    FROM
                        main.lineitem AS ref_3
                    WHERE
                        ref_1.n_comment IS NOT NULL
                    LIMIT 167))
            AND ((0)
                OR (((ref_1.n_regionkey IS NULL)
                        OR (((((1)
                                        AND (0))
                                    AND (ref_0.s_suppkey IS NULL))
                                AND ((ref_1.n_name IS NULL)
                                    OR (1)))
                            AND (ref_0.s_phone IS NULL)))
                    OR ((
                            SELECT
                                n_regionkey
                            FROM
                                main.nation
                            LIMIT 1 offset 5)
                        IS NULL))))
        AND (8 IS NULL))
    OR (ref_2.l_discount IS NULL))
OR ((EXISTS (
            SELECT
                28 AS c0,
                ref_1.n_comment AS c1,
                (
                    SELECT
                        o_orderstatus
                    FROM
                        main.orders
                    LIMIT 1 offset 1) AS c2,
                ref_1.n_comment AS c3,
                28 AS c4,
                ref_2.l_linestatus AS c5,
                ref_0.s_acctbal AS c6,
                ref_1.n_regionkey AS c7,
                ref_1.n_regionkey AS c8
            FROM
                main.nation AS ref_4
            WHERE
                ref_2.l_receiptdate IS NULL
            LIMIT 78))
    OR (1))
LIMIT 73) AS subq_0
WHERE
    40 IS NOT NULL
