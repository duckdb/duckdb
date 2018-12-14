SELECT
    subq_0.c19 AS c0,
    subq_0.c14 AS c1,
    subq_0.c11 AS c2,
    subq_0.c19 AS c3,
    subq_0.c18 AS c4,
    subq_0.c9 AS c5,
    subq_0.c18 AS c6,
    CAST(nullif (subq_0.c12, CAST(nullif (
                    CASE WHEN EXISTS (
                            SELECT
                                ref_3.n_name AS c0, subq_0.c3 AS c1, ref_1.o_totalprice AS c2, 27 AS c3, ref_2.c_nationkey AS c4, subq_0.c0 AS c5, ref_3.n_comment AS c6, ref_3.n_regionkey AS c7, ref_2.c_phone AS c8 FROM main.orders AS ref_1
                                INNER JOIN main.customer AS ref_2
                                INNER JOIN main.nation AS ref_3 ON (80 IS NOT NULL) ON (ref_1.o_orderpriority = ref_2.c_name)
                            WHERE ((subq_0.c22 IS NULL)
                                OR (0))
                            OR ((((((EXISTS (
                                                        SELECT
                                                            ref_2.c_mktsegment AS c0, ref_4.n_regionkey AS c1, ref_1.o_orderkey AS c2 FROM main.nation AS ref_4
                                                        WHERE (1)
                                                        AND ((0)
                                                            AND ((subq_0.c21 IS NOT NULL)
                                                                OR ((ref_1.o_shippriority IS NULL)
                                                                    AND (0))))
                                                    LIMIT 153))
                                            OR ((EXISTS (
                                                        SELECT
                                                            ref_5.n_regionkey AS c0 FROM main.nation AS ref_5
                                                        WHERE (0)
                                                        AND ((1)
                                                            AND (1))
                                                    LIMIT 82))
                                            OR ((ref_3.n_comment IS NULL)
                                                AND (((1)
                                                        OR ((1)
                                                            OR ((((1)
                                                                        AND ((subq_0.c18 IS NULL)
                                                                            OR (ref_2.c_nationkey IS NULL)))
                                                                    OR (ref_1.o_clerk IS NOT NULL))
                                                                OR (((EXISTS (
                                                                                SELECT
                                                                                    ref_1.o_orderdate AS c0, ref_3.n_name AS c1, ref_6.ps_availqty AS c2, ref_2.c_address AS c3, ref_2.c_nationkey AS c4, ref_3.n_regionkey AS c5, ref_6.ps_availqty AS c6, ref_6.ps_availqty AS c7, ref_2.c_custkey AS c8, subq_0.c6 AS c9, subq_0.c9 AS c10, subq_0.c7 AS c11, ref_1.o_orderkey AS c12 FROM main.partsupp AS ref_6
                                                                                WHERE
                                                                                    ref_3.n_name IS NOT NULL))
                                                                            OR (1))
                                                                        AND (EXISTS (
                                                                                SELECT
                                                                                    ref_7.p_type AS c0, ref_2.c_address AS c1, subq_0.c12 AS c2 FROM main.part AS ref_7
                                                                                WHERE (1)
                                                                                AND (1)))))))
                                                        AND (EXISTS (
                                                                SELECT
                                                                    ref_1.o_orderkey AS c0, ref_1.o_shippriority AS c1, ref_1.o_orderkey AS c2 FROM main.lineitem AS ref_8
                                                                WHERE
                                                                    ref_1.o_shippriority IS NOT NULL
                                                                LIMIT 139))))))
                                        OR (1))
                                    OR (0))
                                OR (subq_0.c20 IS NULL))
                            OR (((0)
                                    OR (EXISTS (
                                            SELECT
                                                subq_0.c11 AS c0, ref_2.c_name AS c1, ref_9.p_mfgr AS c2, 68 AS c3, subq_0.c4 AS c4, ref_1.o_orderstatus AS c5, ref_2.c_address AS c6, ref_2.c_mktsegment AS c7, subq_0.c21 AS c8, ref_9.p_partkey AS c9 FROM main.part AS ref_9
                                            WHERE (1)
                                            OR ((1)
                                                AND ((0)
                                                    AND (1)))
                                        LIMIT 52)))
                            AND (EXISTS (
                                    SELECT
                                        ref_1.o_orderkey AS c0, subq_0.c10 AS c1 FROM main.lineitem AS ref_10
                                    WHERE
                                        subq_0.c17 IS NULL
                                    LIMIT 97))))) THEN
                subq_0.c0
            ELSE
                subq_0.c0
            END, subq_0.c12) AS INTEGER)) AS INTEGER) AS c7, subq_0.c5 AS c8
FROM (
    SELECT
        ref_0.o_custkey AS c0, ref_0.o_comment AS c1, ref_0.o_totalprice AS c2, ref_0.o_orderpriority AS c3, ref_0.o_totalprice AS c4, ref_0.o_orderkey AS c5, ref_0.o_comment AS c6, ref_0.o_custkey AS c7, ref_0.o_comment AS c8, ref_0.o_totalprice AS c9, CAST(coalesce(ref_0.o_shippriority, ref_0.o_orderkey) AS INTEGER) AS c10, ref_0.o_orderstatus AS c11, ref_0.o_custkey AS c12, ref_0.o_clerk AS c13, ref_0.o_comment AS c14, ref_0.o_totalprice AS c15, ref_0.o_comment AS c16, ref_0.o_orderpriority AS c17, ref_0.o_clerk AS c18, ref_0.o_comment AS c19, ref_0.o_orderpriority AS c20, ref_0.o_orderpriority AS c21, ref_0.o_orderdate AS c22
    FROM
        main.orders AS ref_0
    WHERE (0)
    OR (ref_0.o_orderpriority IS NOT NULL)) AS subq_0
WHERE
    subq_0.c15 IS NOT NULL
LIMIT 51
