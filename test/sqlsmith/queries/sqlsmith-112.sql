SELECT
    subq_0.c6 AS c0
FROM (
    SELECT
        ref_2.ps_partkey AS c0,
        ref_2.ps_suppkey AS c1,
        ref_2.ps_supplycost AS c2,
        ref_0.s_phone AS c3,
        ref_1.s_comment AS c4,
        ref_2.ps_availqty AS c5,
        ref_0.s_name AS c6,
        ref_2.ps_supplycost AS c7,
        ref_1.s_acctbal AS c8
    FROM
        main.supplier AS ref_0
        INNER JOIN main.supplier AS ref_1
        RIGHT JOIN main.partsupp AS ref_2 ON ((1)
                AND (ref_1.s_comment IS NULL)) ON (ref_0.s_comment = ref_1.s_name)
    WHERE (ref_1.s_nationkey IS NOT NULL)
    AND ((((EXISTS (
                        SELECT
                            ref_3.ps_suppkey AS c0, ref_1.s_comment AS c1, ref_2.ps_partkey AS c2
                        FROM
                            main.partsupp AS ref_3
                        WHERE (1)
                        OR (1)))
                AND (ref_0.s_comment IS NULL))
            AND (EXISTS (
                    SELECT
                        ref_0.s_phone AS c0, ref_2.ps_suppkey AS c1, ref_1.s_nationkey AS c2, ref_4.c_mktsegment AS c3, ref_0.s_phone AS c4
                    FROM
                        main.customer AS ref_4
                    WHERE (((EXISTS (
                                    SELECT
                                        63 AS c0
                                    FROM
                                        main.customer AS ref_5
                                    WHERE
                                        EXISTS (
                                            SELECT
                                                ref_2.ps_supplycost AS c0, (
                                                    SELECT
                                                        c_custkey
                                                    FROM
                                                        main.customer
                                                    LIMIT 1 offset 1) AS c1,
                                                30 AS c2,
                                                ref_4.c_name AS c3,
                                                ref_1.s_name AS c4,
                                                ref_5.c_acctbal AS c5,
                                                43 AS c6,
                                                13 AS c7
                                            FROM
                                                main.nation AS ref_6
                                            WHERE
                                                ref_1.s_suppkey IS NOT NULL
                                            LIMIT 91)
                                    LIMIT 104))
                            OR (ref_4.c_comment IS NOT NULL))
                        AND (ref_1.s_acctbal IS NOT NULL))
                    OR ((1)
                        AND (0)))))
        OR ((EXISTS (
                    SELECT
                        ref_1.s_acctbal AS c0
                    FROM
                        main.lineitem AS ref_7
                    WHERE
                        ref_7.l_commitdate IS NULL
                    LIMIT 118))
            OR (ref_2.ps_partkey IS NOT NULL)))
LIMIT 188) AS subq_0
WHERE
    subq_0.c2 IS NOT NULL
