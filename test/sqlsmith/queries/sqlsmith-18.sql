INSERT INTO main.customer
    VALUES (14, DEFAULT, DEFAULT, 76, CAST(coalesce(CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)) AS VARCHAR), DEFAULT, CASE WHEN EXISTS (
            SELECT
                ref_0.c_phone AS c0,
                ref_0.c_address AS c1,
                CASE WHEN ref_0.c_address IS NOT NULL THEN
                    ref_0.c_phone
                ELSE
                    ref_0.c_phone
                END AS c2,
                ref_0.c_comment AS c3
            FROM
                main.customer AS ref_0
            WHERE ((((ref_0.c_address IS NOT NULL)
                        AND (((EXISTS (
                                        SELECT
                                            ref_0.c_custkey AS c0, ref_0.c_custkey AS c1
                                        FROM
                                            main.nation AS ref_1
                                        WHERE
                                            0
                                        LIMIT 117))
                                AND (21 IS NULL))
                            OR (((
                                        SELECT
                                            o_orderkey
                                        FROM
                                            main.orders
                                        LIMIT 1 offset 6) IS NOT NULL)
                                OR (1))))
                    AND (1))
                OR (ref_0.c_phone IS NULL))
            OR ((EXISTS (
                        SELECT
                            ref_2.n_nationkey AS c0,
                            ref_0.c_custkey AS c1,
                            ref_0.c_nationkey AS c2
                        FROM
                            main.nation AS ref_2
                        WHERE ((0)
                            AND ((1)
                                AND (1)))
                        OR ((ref_2.n_regionkey IS NULL)
                            AND (((1)
                                    OR (1))
                                OR (EXISTS (
                                        SELECT
                                            ref_3.l_receiptdate AS c0
                                        FROM
                                            main.lineitem AS ref_3
                                        WHERE
                                            0))))
                        LIMIT 118))
                AND (((0)
                        OR ((ref_0.c_mktsegment IS NULL)
                            OR ((0)
                                OR (EXISTS (
                                        SELECT
                                            ref_0.c_address AS c0
                                        FROM
                                            main.nation AS ref_4
                                        WHERE ((0)
                                            AND (ref_0.c_comment IS NOT NULL))
                                        OR (ref_0.c_phone IS NULL))))))
                    OR (ref_0.c_phone IS NULL)))
        LIMIT 148) THEN
            CAST(NULL AS VARCHAR)
        ELSE
            CAST(NULL AS VARCHAR)
        END,
        CAST(NULL AS VARCHAR))
