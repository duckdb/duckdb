SELECT
    subq_0.c0 AS c0,
    subq_0.c0 AS c1
FROM (
    SELECT
        ref_3.c_comment AS c0
    FROM
        main.orders AS ref_0
        INNER JOIN main.part AS ref_1 ON (ref_0.o_orderpriority = ref_1.p_name)
        RIGHT JOIN main.partsupp AS ref_2
        INNER JOIN main.customer AS ref_3 ON (ref_3.c_custkey IS NOT NULL) ON (ref_0.o_orderstatus = ref_2.ps_comment)
    WHERE
        EXISTS (
            SELECT
                ref_2.ps_supplycost AS c0, 47 AS c1
            FROM
                main.partsupp AS ref_4
            WHERE
                1
            LIMIT 10)) AS subq_0
WHERE (EXISTS (
        SELECT
            ref_5.s_address AS c0, ref_5.s_phone AS c1, 45 AS c2, subq_0.c0 AS c3, (
                SELECT
                    s_acctbal
                FROM
                    main.supplier
                LIMIT 1 offset 5) AS c4,
            ref_5.s_phone AS c5,
            97 AS c6
        FROM
            main.supplier AS ref_5
        WHERE ((ref_5.s_address IS NULL)
            AND ((subq_0.c0 IS NOT NULL)
                AND (subq_0.c0 IS NULL)))
        AND (ref_5.s_comment IS NULL)))
OR ((EXISTS (
            SELECT
                subq_0.c0 AS c0, subq_0.c0 AS c1
            FROM
                main.supplier AS ref_6
            WHERE
                subq_0.c0 IS NULL))
        AND (1))
LIMIT 89
