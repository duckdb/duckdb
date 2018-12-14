WITH jennifer_0 AS (
    SELECT
        ref_0.n_regionkey AS c0,
        ref_0.n_name AS c1,
        ref_0.n_comment AS c2,
        ref_0.n_comment AS c3,
        ref_0.n_comment AS c4,
        ref_0.n_regionkey AS c5,
        (
            SELECT
                p_name
            FROM
                main.part
            LIMIT 1 offset 5) AS c6
    FROM
        main.nation AS ref_0
    WHERE
        1
)
SELECT
    subq_0.c0 AS c0
FROM (
    SELECT
        (
            SELECT
                l_commitdate
            FROM
                main.lineitem
            LIMIT 1 offset 5) AS c0
    FROM
        main.supplier AS ref_1
        INNER JOIN jennifer_0 AS ref_2 ON (ref_1.s_name = ref_2.c1)
    WHERE
        ref_1.s_address IS NULL) AS subq_0
WHERE (((
            SELECT
                o_orderdate
            FROM
                main.orders
            LIMIT 1 offset 2)
        IS NOT NULL)
    OR ((EXISTS (
                SELECT
                    ref_3.r_regionkey AS c0,
                    ref_3.r_name AS c1,
                    subq_0.c0 AS c2,
                    ref_3.r_name AS c3
                FROM
                    main.region AS ref_3
                LEFT JOIN main.lineitem AS ref_4 ON (ref_3.r_regionkey = ref_4.l_orderkey)
            WHERE ((EXISTS (
                        SELECT
                            subq_0.c0 AS c0, ref_4.l_returnflag AS c1, subq_0.c0 AS c2, ref_3.r_comment AS c3
                        FROM
                            main.nation AS ref_5
                        WHERE
                            1))
                    OR (((ref_4.l_quantity IS NULL)
                            OR (1))
                        OR (subq_0.c0 IS NULL)))
                AND (1)))
        OR (((EXISTS (
                        SELECT
                            ref_6.c_address AS c0, subq_0.c0 AS c1
                        FROM
                            main.customer AS ref_6
                        WHERE ((0)
                            AND (ref_6.c_acctbal IS NOT NULL))
                        OR (1)
                    LIMIT 45))
            OR (subq_0.c0 IS NULL))
        AND ((((EXISTS (
                            SELECT
                                ref_7.c_mktsegment AS c0,
                                ref_7.c_comment AS c1,
                                ref_7.c_phone AS c2,
                                subq_0.c0 AS c3,
                                98 AS c4,
                                subq_0.c0 AS c5,
                                97 AS c6,
                                subq_0.c0 AS c7,
                                ref_7.c_comment AS c8,
                                (
                                    SELECT
                                        r_comment
                                    FROM
                                        main.region
                                    LIMIT 1 offset 5) AS c9,
                                ref_7.c_name AS c10,
                                subq_0.c0 AS c11,
                                subq_0.c0 AS c12,
                                subq_0.c0 AS c13,
                                ref_7.c_mktsegment AS c14,
                                ref_7.c_comment AS c15,
                                subq_0.c0 AS c16,
                                ref_7.c_mktsegment AS c17,
                                ref_7.c_phone AS c18,
                                ref_7.c_name AS c19,
                                subq_0.c0 AS c20,
                                (
                                    SELECT
                                        r_comment
                                    FROM
                                        main.region
                                    LIMIT 1 offset 3) AS c21,
                                ref_7.c_address AS c22,
                                subq_0.c0 AS c23
                            FROM
                                main.customer AS ref_7
                            WHERE
                                0
                            LIMIT 67))
                    AND (1))
                AND ((subq_0.c0 IS NULL)
                    OR (subq_0.c0 IS NULL)))
            OR ((EXISTS (
                        SELECT
                            ref_8.l_extendedprice AS c0,
                            (
                                SELECT
                                    l_linenumber
                                FROM
                                    main.lineitem
                                LIMIT 1 offset 1) AS c1,
                            ref_8.l_discount AS c2,
                            ref_8.l_discount AS c3,
                            ref_8.l_shipinstruct AS c4
                        FROM
                            main.lineitem AS ref_8
                        WHERE
                            subq_0.c0 IS NULL
                        LIMIT 78))
                OR (subq_0.c0 IS NULL))))))
OR (((((0)
                OR ((((EXISTS (
                                    SELECT
                                        subq_0.c0 AS c0,
                                        (
                                            SELECT
                                                l_extendedprice
                                            FROM
                                                main.lineitem
                                            LIMIT 1 offset 6) AS c1,
                                        subq_0.c0 AS c2,
                                        subq_0.c0 AS c3
                                    FROM
                                        main.partsupp AS ref_9
                                    WHERE (0)
                                    AND (0)))
                            AND (0))
                        OR (((EXISTS (
                                        SELECT
                                            ref_10.l_returnflag AS c0, ref_10.l_quantity AS c1, ref_10.l_shipdate AS c2, subq_0.c0 AS c3, ref_10.l_quantity AS c4, subq_0.c0 AS c5, subq_0.c0 AS c6, ref_10.l_shipdate AS c7, subq_0.c0 AS c8
                                        FROM
                                            main.lineitem AS ref_10
                                        WHERE (EXISTS (
                                                SELECT
                                                    35 AS c0, ref_10.l_partkey AS c1, ref_11.s_phone AS c2
                                                FROM
                                                    main.supplier AS ref_11
                                                WHERE
                                                    ref_10.l_commitdate IS NOT NULL
                                                LIMIT 19))
                                        AND (EXISTS (
                                                SELECT
                                                    subq_0.c0 AS c0
                                                FROM
                                                    main.orders AS ref_12
                                                WHERE
                                                    0
                                                LIMIT 84))
                                    LIMIT 56))
                            AND (EXISTS (
                                    SELECT
                                        subq_0.c0 AS c0,
                                        ref_13.s_nationkey AS c1,
                                        ref_13.s_acctbal AS c2,
                                        ref_13.s_nationkey AS c3
                                    FROM
                                        main.supplier AS ref_13
                                    WHERE
                                        1
                                    LIMIT 73)))
                        AND (EXISTS (
                                SELECT
                                    subq_0.c0 AS c0,
                                    ref_14.n_nationkey AS c1,
                                    ref_14.n_nationkey AS c2,
                                    ref_14.n_nationkey AS c3,
                                    55 AS c4,
                                    subq_0.c0 AS c5
                                FROM
                                    main.nation AS ref_14
                                WHERE
                                    ref_14.n_comment IS NOT NULL))))
                    OR ((0)
                        OR ((subq_0.c0 IS NOT NULL)
                            AND (1)))))
            AND (subq_0.c0 IS NOT NULL))
        AND (0))
    AND ((0)
        OR ((subq_0.c0 IS NULL)
            AND ((subq_0.c0 IS NOT NULL)
                OR ((((EXISTS (
                                    SELECT
                                        ref_15.p_type AS c0, ref_15.p_mfgr AS c1, ref_15.p_brand AS c2, (
                                            SELECT
                                                n_name
                                            FROM
                                                main.nation
                                            LIMIT 1 offset 36) AS c3,
                                        subq_0.c0 AS c4,
                                        ref_15.p_retailprice AS c5,
                                        ref_15.p_mfgr AS c6,
                                        subq_0.c0 AS c7,
                                        subq_0.c0 AS c8,
                                        ref_15.p_brand AS c9,
                                        subq_0.c0 AS c10,
                                        subq_0.c0 AS c11,
                                        ref_15.p_type AS c12,
                                        ref_15.p_partkey AS c13
                                    FROM
                                        main.part AS ref_15
                                    WHERE
                                        1
                                    LIMIT 106))
                            AND (0))
                        OR (0))
                    AND (1))))))
LIMIT 75
