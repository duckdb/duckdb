SELECT
    subq_0.c0 AS c0,
    ref_1.p_partkey AS c1,
    ref_1.p_size AS c2,
    subq_1.c0 AS c3
FROM (
    SELECT
        ref_0.o_orderdate AS c0,
        ref_0.o_orderstatus AS c1,
        ref_0.o_shippriority AS c2,
        ref_0.o_orderdate AS c3,
        88 AS c4,
        ref_0.o_orderdate AS c5,
        ref_0.o_totalprice AS c6,
        ref_0.o_comment AS c7,
        ref_0.o_totalprice AS c8,
        ref_0.o_clerk AS c9,
        ref_0.o_comment AS c10,
        ref_0.o_shippriority AS c11,
        ref_0.o_orderdate AS c12,
        ref_0.o_orderdate AS c13,
        ref_0.o_shippriority AS c14
    FROM
        main.orders AS ref_0
    WHERE (ref_0.o_clerk IS NOT NULL)
    AND (ref_0.o_comment IS NOT NULL)
LIMIT 138) AS subq_0
    RIGHT JOIN main.part AS ref_1
    RIGHT JOIN (
        SELECT
            ref_2.l_quantity AS c0
        FROM
            main.lineitem AS ref_2
        WHERE ((0)
            AND ((EXISTS (
                        SELECT
                            ref_3.r_name AS c0
                        FROM
                            main.region AS ref_3
                        WHERE (0)
                        OR ((EXISTS (
                                    SELECT
                                        ref_3.r_regionkey AS c0, ref_4.r_name AS c1, ref_4.r_comment AS c2
                                    FROM
                                        main.region AS ref_4
                                    WHERE
                                        1
                                    LIMIT 79))
                            AND ((ref_2.l_extendedprice IS NULL)
                                AND (ref_2.l_linestatus IS NOT NULL)))
                    LIMIT 78))
            OR (0)))
    OR (1)
LIMIT 89) AS subq_1 ON ((ref_1.p_brand IS NULL)
        OR (1)) ON (subq_0.c7 = ref_1.p_name)
WHERE
    ref_1.p_name IS NOT NULL
LIMIT 96
