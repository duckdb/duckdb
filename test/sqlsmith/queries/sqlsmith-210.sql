SELECT
    ref_0.c_name AS c0,
    subq_0.c1 AS c1,
    80 AS c2,
    CAST(nullif (ref_0.c_nationkey, subq_0.c3) AS INTEGER) AS c3,
    subq_0.c0 AS c4
FROM
    main.customer AS ref_0
    LEFT JOIN (
        SELECT
            ref_3.l_commitdate AS c0,
            ref_3.l_shipdate AS c1,
            ref_1.n_name AS c2,
            ref_1.n_nationkey AS c3,
            ref_2.c_name AS c4
        FROM
            main.nation AS ref_1
            LEFT JOIN main.customer AS ref_2
            INNER JOIN main.lineitem AS ref_3 ON (ref_2.c_acctbal IS NOT NULL) ON (ref_1.n_name IS NOT NULL)
        WHERE ((EXISTS (
                    SELECT
                        ref_3.l_commitdate AS c0, ref_3.l_quantity AS c1, ref_1.n_regionkey AS c2, ref_2.c_nationkey AS c3, ref_2.c_address AS c4, ref_1.n_comment AS c5, ref_2.c_phone AS c6, ref_1.n_regionkey AS c7
                    FROM
                        main.nation AS ref_4
                    WHERE (1)
                    AND ((1)
                        OR (1))))
            AND (ref_2.c_acctbal IS NULL))
        OR (ref_3.l_orderkey IS NOT NULL)
    LIMIT 71) AS subq_0 ON (ref_0.c_comment = subq_0.c2)
WHERE
    1
