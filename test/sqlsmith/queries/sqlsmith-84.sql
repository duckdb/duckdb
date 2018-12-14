SELECT
    subq_0.c1 AS c0
FROM
    main.customer AS ref_0
    INNER JOIN main.orders AS ref_1
    INNER JOIN (
        SELECT
            ref_2.n_comment AS c0,
            ref_2.n_comment AS c1,
            ref_2.n_regionkey AS c2,
            ref_2.n_regionkey AS c3,
            ref_2.n_nationkey AS c4,
            ref_2.n_regionkey AS c5
        FROM
            main.nation AS ref_2
        WHERE
            EXISTS (
                SELECT
                    ref_2.n_nationkey AS c0
                FROM
                    main.nation AS ref_3
                WHERE
                    ref_3.n_name IS NOT NULL
                LIMIT 63)
        LIMIT 84) AS subq_0 ON (ref_1.o_orderpriority IS NOT NULL) ON (ref_0.c_address = ref_1.o_orderstatus)
WHERE
    ref_0.c_mktsegment IS NOT NULL
LIMIT 7
