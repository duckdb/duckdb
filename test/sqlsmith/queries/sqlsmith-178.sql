SELECT
    ref_0.n_name AS c0
FROM
    main.nation AS ref_0
WHERE (EXISTS (
        SELECT
            ref_2.o_custkey AS c0, ref_0.n_comment AS c1, CAST(coalesce(ref_4.l_quantity, ref_0.n_nationkey) AS INTEGER) AS c2, ref_1.s_address AS c3
        FROM
            main.supplier AS ref_1
            INNER JOIN main.orders AS ref_2 ON (ref_1.s_suppkey = ref_2.o_orderkey)
            INNER JOIN main.region AS ref_3
            RIGHT JOIN main.lineitem AS ref_4 ON (ref_4.l_extendedprice IS NOT NULL) ON (ref_1.s_comment = ref_3.r_name)
        WHERE (EXISTS (
                SELECT
                    ref_6.n_name AS c0, ref_6.n_comment AS c1, ref_4.l_linenumber AS c2, ref_4.l_shipmode AS c3
                FROM
                    main.region AS ref_5
                RIGHT JOIN main.nation AS ref_6 ON (ref_6.n_comment IS NOT NULL)
            WHERE ((
                    SELECT
                        s_comment
                    FROM
                        main.supplier
                    LIMIT 1 offset 3)
                IS NOT NULL)
            OR (ref_2.o_orderdate IS NULL)
        LIMIT 85))
AND (ref_3.r_regionkey IS NOT NULL)
LIMIT 81))
OR (0)
LIMIT 125
