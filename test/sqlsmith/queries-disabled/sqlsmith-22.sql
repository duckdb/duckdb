SELECT
    subq_0.c3 AS c0,
    (
        SELECT
            p_retailprice
        FROM
            main.part
        LIMIT 1 offset 78) AS c1,
    subq_0.c0 AS c2,
    subq_0.c2 AS c3,
    80 AS c4,
    subq_0.c5 AS c5,
    subq_0.c4 AS c6,
    subq_0.c4 AS c7,
    subq_0.c4 AS c8,
    subq_0.c0 AS c9,
    subq_0.c5 AS c10,
    subq_0.c1 AS c11
FROM (
    SELECT
        ref_0.l_shipmode AS c0,
        43 AS c1,
        ref_1.s_nationkey AS c2,
        ref_1.s_name AS c3,
        ref_0.l_comment AS c4,
        ref_0.l_linenumber AS c5
    FROM
        main.lineitem AS ref_0
    LEFT JOIN main.supplier AS ref_1 ON (ref_0.l_comment = ref_1.s_name)
WHERE ((1)
    OR (0))
AND (((ref_0.l_returnflag IS NULL)
        OR (1))
    AND (EXISTS (
            SELECT
                ref_2.o_orderpriority AS c0, ref_1.s_address AS c1, ref_1.s_name AS c2
            FROM
                main.orders AS ref_2
            WHERE (EXISTS (
                    SELECT
                        ref_0.l_orderkey AS c0, ref_0.l_shipinstruct AS c1, ref_0.l_tax AS c2, ref_3.s_suppkey AS c3
                    FROM
                        main.supplier AS ref_3
                    WHERE (EXISTS (
                            SELECT
                                ref_3.s_comment AS c0, ref_2.o_clerk AS c1, (
                                    SELECT
                                        r_regionkey
                                    FROM
                                        main.region
                                    LIMIT 1 offset 6) AS c2,
                                ref_1.s_address AS c3,
                                ref_4.l_extendedprice AS c4,
                                ref_2.o_totalprice AS c5,
                                ref_0.l_suppkey AS c6,
                                ref_0.l_shipdate AS c7
                            FROM
                                main.lineitem AS ref_4
                            WHERE
                                1
                            LIMIT 91))
                    OR (EXISTS (
                            SELECT
                                ref_2.o_clerk AS c0,
                                ref_5.p_retailprice AS c1,
                                ref_1.s_suppkey AS c2
                            FROM
                                main.part AS ref_5
                            WHERE
                                ref_0.l_shipinstruct IS NOT NULL
                            LIMIT 6))
                LIMIT 20))
        AND (ref_0.l_shipinstruct IS NULL)
    LIMIT 115)))
LIMIT 82) AS subq_0
WHERE ((((
                CASE WHEN subq_0.c5 IS NULL THEN
                    subq_0.c3
                ELSE
                    subq_0.c3
                END IS NOT NULL)
            AND (0))
        OR (subq_0.c0 IS NOT NULL))
    AND (0))
AND (subq_0.c1 IS NULL)
LIMIT 7
