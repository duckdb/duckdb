SELECT
    subq_0.c0 AS c0,
    subq_0.c0 AS c1
FROM (
    SELECT
        ref_4.ps_availqty AS c0
    FROM
        main.region AS ref_0
        INNER JOIN main.part AS ref_1 ON (ref_0.r_regionkey IS NULL)
        LEFT JOIN main.customer AS ref_2 ON (ref_0.r_comment = ref_2.c_name)
        INNER JOIN main.part AS ref_3
        RIGHT JOIN main.partsupp AS ref_4
        INNER JOIN main.supplier AS ref_5 ON (ref_4.ps_suppkey = ref_5.s_suppkey) ON (ref_4.ps_supplycost IS NOT NULL) ON (ref_2.c_comment = ref_3.p_name)
    WHERE
        ref_0.r_comment IS NULL) AS subq_0
WHERE ((EXISTS (
            SELECT
                ref_6.n_nationkey AS c0, ref_6.n_nationkey AS c1
            FROM
                main.nation AS ref_6
            WHERE ((EXISTS (
                        SELECT
                            ref_7.s_nationkey AS c0
                        FROM
                            main.supplier AS ref_7
                        WHERE
                            subq_0.c0 IS NULL))
                    OR ((subq_0.c0 IS NOT NULL)
                        AND (((subq_0.c0 IS NULL)
                                AND (ref_6.n_comment IS NULL))
                            OR (1))))
                AND (((0)
                        AND (((subq_0.c0 IS NULL)
                                OR (0))
                            AND ((((0)
                                        OR (EXISTS (
                                                SELECT
                                                    ref_6.n_name AS c0, subq_0.c0 AS c1, ref_8.l_shipdate AS c2, ref_8.l_linenumber AS c3, ref_6.n_name AS c4, ref_6.n_name AS c5, subq_0.c0 AS c6, ref_6.n_nationkey AS c7
                                                FROM
                                                    main.lineitem AS ref_8
                                                WHERE (EXISTS (
                                                        SELECT
                                                            subq_0.c0 AS c0, ref_8.l_linenumber AS c1, ref_9.p_mfgr AS c2, subq_0.c0 AS c3, ref_6.n_name AS c4, subq_0.c0 AS c5, ref_6.n_comment AS c6, ref_6.n_name AS c7, ref_9.p_container AS c8, ref_9.p_partkey AS c9, ref_9.p_brand AS c10, subq_0.c0 AS c11, ref_8.l_extendedprice AS c12, ref_9.p_size AS c13, ref_8.l_linenumber AS c14
                                                        FROM
                                                            main.part AS ref_9
                                                        WHERE (1)
                                                        OR (EXISTS (
                                                                SELECT
                                                                    ref_9.p_container AS c0, subq_0.c0 AS c1, ref_10.o_totalprice AS c2, ref_10.o_custkey AS c3, ref_10.o_comment AS c4, ref_8.l_tax AS c5, subq_0.c0 AS c6, ref_10.o_orderstatus AS c7, ref_10.o_shippriority AS c8, ref_6.n_nationkey AS c9, ref_6.n_nationkey AS c10, ref_10.o_shippriority AS c11, ref_6.n_name AS c12, subq_0.c0 AS c13, ref_6.n_regionkey AS c14
                                                                FROM
                                                                    main.orders AS ref_10
                                                                WHERE
                                                                    0
                                                                LIMIT 24))
                                                    LIMIT 63))
                                            AND (96 IS NOT NULL))))
                                AND ((1)
                                    AND (EXISTS (
                                            SELECT
                                                ref_11.c_custkey AS c0,
                                                ref_6.n_nationkey AS c1,
                                                11 AS c2,
                                                subq_0.c0 AS c3
                                            FROM
                                                main.customer AS ref_11
                                            WHERE
                                                ref_6.n_regionkey IS NULL
                                            LIMIT 168))))
                            OR (EXISTS (
                                    SELECT
                                        ref_12.l_extendedprice AS c0
                                    FROM
                                        main.lineitem AS ref_12
                                    WHERE
                                        1
                                    LIMIT 19)))))
                AND ((subq_0.c0 IS NULL)
                    AND (ref_6.n_regionkey IS NULL)))
        LIMIT 82))
OR (subq_0.c0 IS NOT NULL))
OR (((subq_0.c0 IS NULL)
        OR (0))
    AND (subq_0.c0 IS NOT NULL))
LIMIT 125
