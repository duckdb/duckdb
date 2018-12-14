SELECT
    subq_0.c0 AS c0,
    subq_0.c0 AS c1,
    subq_0.c2 AS c2,
    subq_0.c2 AS c3,
    subq_0.c1 AS c4,
    subq_0.c1 AS c5,
    subq_0.c3 AS c6,
    subq_0.c0 AS c7,
    subq_0.c3 AS c8,
    subq_0.c1 AS c9
FROM (
    SELECT
        ref_0.ps_comment AS c0,
        CASE WHEN ((0)
                AND (((0)
                        AND ((ref_0.ps_suppkey IS NULL)
                            OR ((ref_0.ps_comment IS NULL)
                                AND ((((0)
                                            AND (0))
                                        AND (0))
                                    OR (ref_0.ps_comment IS NULL)))))
                    OR ((ref_0.ps_supplycost IS NULL)
                        AND (((ref_0.ps_supplycost IS NULL)
                                AND ((EXISTS (
                                            SELECT
                                                ref_0.ps_supplycost AS c0,
                                                77 AS c1,
                                                ref_0.ps_suppkey AS c2,
                                                ref_0.ps_availqty AS c3,
                                                91 AS c4,
                                                ref_0.ps_partkey AS c5,
                                                ref_1.r_regionkey AS c6,
                                                ref_0.ps_suppkey AS c7,
                                                ref_1.r_name AS c8,
                                                ref_0.ps_comment AS c9
                                            FROM
                                                main.region AS ref_1
                                            WHERE ((ref_0.ps_availqty IS NULL)
                                                AND (ref_1.r_name IS NOT NULL))
                                            AND (0)
                                        LIMIT 91))
                                AND (EXISTS (
                                        SELECT
                                            ref_0.ps_comment AS c0
                                        FROM
                                            main.partsupp AS ref_2
                                        WHERE
                                            0
                                        LIMIT 49))))
                        AND ((EXISTS (
                                    SELECT
                                        ref_3.ps_supplycost AS c0,
                                        ref_0.ps_supplycost AS c1,
                                        ref_0.ps_suppkey AS c2,
                                        ref_0.ps_suppkey AS c3,
                                        ref_0.ps_partkey AS c4,
                                        ref_0.ps_suppkey AS c5,
                                        ref_0.ps_availqty AS c6,
                                        ref_3.ps_supplycost AS c7
                                    FROM
                                        main.partsupp AS ref_3
                                    WHERE
                                        ref_3.ps_supplycost IS NOT NULL
                                    LIMIT 148))
                            OR (1))))))
        OR ((EXISTS (
                    SELECT
                        ref_0.ps_partkey AS c0,
                        ref_0.ps_suppkey AS c1,
                        76 AS c2,
                        ref_0.ps_partkey AS c3,
                        ref_0.ps_partkey AS c4,
                        ref_4.p_retailprice AS c5,
                        ref_0.ps_suppkey AS c6,
                        ref_0.ps_availqty AS c7
                    FROM
                        main.part AS ref_4
                    WHERE (1)
                    OR (EXISTS (
                            SELECT
                                ref_5.s_address AS c0, 73 AS c1
                            FROM
                                main.supplier AS ref_5
                            WHERE
                                EXISTS (
                                    SELECT
                                        ref_6.p_container AS c0, ref_4.p_container AS c1, ref_6.p_container AS c2, ref_5.s_address AS c3, ref_5.s_name AS c4
                                    FROM
                                        main.part AS ref_6
                                    WHERE (1)
                                    OR (0))))))
                OR ((0)
                    OR (0))) THEN
            ref_0.ps_partkey
        ELSE
            ref_0.ps_partkey
        END AS c1, ref_0.ps_supplycost AS c2, CASE WHEN ref_0.ps_availqty IS NULL THEN
            CASE WHEN 1 THEN
                ref_0.ps_suppkey
            ELSE
                ref_0.ps_suppkey
            END
        ELSE
            CASE WHEN 1 THEN
                ref_0.ps_suppkey
            ELSE
                ref_0.ps_suppkey
            END
        END AS c3, ref_0.ps_partkey AS c4
    FROM
        main.partsupp AS ref_0
    WHERE
        ref_0.ps_supplycost IS NOT NULL) AS subq_0
WHERE
    subq_0.c1 IS NULL
