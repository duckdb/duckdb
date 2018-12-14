SELECT
    subq_0.c3 AS c0
FROM (
    SELECT
        ref_2.l_linestatus AS c0,
        ref_0.c_comment AS c1,
        ref_1.n_name AS c2,
        ref_1.n_name AS c3,
        ref_2.l_shipdate AS c4,
        ref_2.l_commitdate AS c5,
        ref_2.l_extendedprice AS c6,
        ref_2.l_receiptdate AS c7,
        CASE WHEN ((EXISTS (
                        SELECT
                            ref_0.c_comment AS c0
                        FROM
                            main.part AS ref_3
                        WHERE
                            EXISTS (
                                SELECT
                                    (
                                        SELECT
                                            n_comment
                                        FROM
                                            main.nation
                                        LIMIT 1 offset 5) AS c0,
                                    ref_0.c_nationkey AS c1,
                                    ref_3.p_size AS c2,
                                    ref_3.p_comment AS c3,
                                    ref_4.n_regionkey AS c4,
                                    ref_0.c_nationkey AS c5,
                                    ref_1.n_name AS c6,
                                    ref_3.p_container AS c7,
                                    (
                                        SELECT
                                            s_phone
                                        FROM
                                            main.supplier
                                        LIMIT 1 offset 5) AS c8
                                FROM
                                    main.nation AS ref_4
                                WHERE
                                    1)))
                        OR (1))
                    AND ((EXISTS (
                                SELECT
                                    ref_1.n_regionkey AS c0, ref_1.n_regionkey AS c1
                                FROM
                                    main.partsupp AS ref_5
                                WHERE
                                    ref_2.l_quantity IS NOT NULL))
                            OR ((0)
                                OR (EXISTS (
                                        SELECT
                                            ref_6.s_suppkey AS c0
                                        FROM
                                            main.supplier AS ref_6
                                        WHERE ((EXISTS (
                                                    SELECT
                                                        (
                                                            SELECT
                                                                ps_supplycost
                                                            FROM
                                                                main.partsupp
                                                            LIMIT 1 offset 1) AS c0,
                                                        ref_2.l_extendedprice AS c1,
                                                        ref_1.n_regionkey AS c2,
                                                        ref_0.c_mktsegment AS c3,
                                                        ref_1.n_nationkey AS c4
                                                    FROM
                                                        main.customer AS ref_7
                                                    WHERE
                                                        EXISTS (
                                                            SELECT
                                                                ref_1.n_name AS c0, ref_2.l_shipinstruct AS c1, ref_6.s_name AS c2
                                                            FROM
                                                                main.lineitem AS ref_8
                                                            WHERE
                                                                ref_1.n_regionkey IS NULL
                                                            LIMIT 95)))
                                                AND (0))
                                            AND (ref_6.s_nationkey IS NULL))))) THEN
                            ref_1.n_name
                        ELSE
                            ref_1.n_name
                        END AS c8
                    FROM
                        main.customer AS ref_0
                    LEFT JOIN main.nation AS ref_1
                    INNER JOIN main.lineitem AS ref_2 ON (ref_1.n_nationkey IS NOT NULL) ON (ref_0.c_address = ref_1.n_name)
                WHERE (EXISTS (
                        SELECT
                            ref_0.c_nationkey AS c0
                        FROM
                            main.lineitem AS ref_9
                        WHERE (EXISTS (
                                SELECT
                                    ref_9.l_tax AS c0, ref_10.n_nationkey AS c1, ref_10.n_comment AS c2, 6 AS c3, ref_9.l_shipinstruct AS c4, 84 AS c5, ref_0.c_mktsegment AS c6, ref_2.l_shipdate AS c7, ref_0.c_acctbal AS c8, ref_1.n_name AS c9, ref_1.n_name AS c10, (
                                        SELECT
                                            ps_comment
                                        FROM
                                            main.partsupp
                                        LIMIT 1 offset 6) AS c11,
                                    (
                                        SELECT
                                            r_name
                                        FROM
                                            main.region
                                        LIMIT 1 offset 1) AS c12
                                FROM
                                    main.nation AS ref_10
                                WHERE ((ref_1.n_regionkey IS NULL)
                                    AND (1))
                                OR (0)
                            LIMIT 24))
                    OR (0)
                LIMIT 169))
        OR (ref_0.c_name IS NULL)
    LIMIT 157) AS subq_0
    RIGHT JOIN main.orders AS ref_11 ON (subq_0.c1 = ref_11.o_orderstatus)
WHERE
    CAST(nullif (ref_11.o_orderdate, ref_11.o_orderdate) AS DATE)
    IS NULL
LIMIT 133
