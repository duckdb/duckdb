INSERT INTO main.customer
    VALUES (
        CASE WHEN EXISTS (
            SELECT
                subq_0.c7 AS c0,
                subq_0.c4 AS c1,
                subq_0.c5 AS c2,
                subq_0.c3 AS c3
            FROM (
                SELECT
                    ref_0.l_discount AS c0,
                    ref_0.l_orderkey AS c1,
                    ref_0.l_partkey AS c2,
                    (
                        SELECT
                            r_comment
                        FROM
                            main.region
                        LIMIT 1 offset 3) AS c3,
                    ref_0.l_discount AS c4,
                    ref_0.l_linenumber AS c5,
                    ref_0.l_quantity AS c6,
                    ref_0.l_shipinstruct AS c7,
                    ref_0.l_tax AS c8
                FROM
                    main.lineitem AS ref_0
                WHERE (((ref_0.l_linestatus IS NULL)
                        AND (1))
                    AND (ref_0.l_commitdate IS NOT NULL))
                AND ((1)
                    OR (EXISTS (
                            SELECT
                                ref_0.l_suppkey AS c0, ref_1.r_comment AS c1, ref_0.l_returnflag AS c2
                            FROM
                                main.region AS ref_1
                            WHERE (EXISTS (
                                    SELECT
                                        ref_2.o_custkey AS c0, ref_2.o_orderpriority AS c1, ref_1.r_regionkey AS c2, 7 AS c3
                                    FROM
                                        main.orders AS ref_2
                                    WHERE
                                        0
                                    LIMIT 145))
                            OR (1)
                        LIMIT 140)))
        LIMIT 170) AS subq_0
WHERE
    subq_0.c8 IS NULL
LIMIT 144) THEN
            10
        ELSE
            10
        END,
        CAST(coalesce(CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)) AS VARCHAR),
        CAST(NULL AS VARCHAR),
        CAST(coalesce(50, 95) AS INTEGER),
        CAST(NULL AS VARCHAR),
        CAST(NULL AS DECIMAL),
        CAST(NULL AS VARCHAR),
        DEFAULT)
