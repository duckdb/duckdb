SELECT
    subq_0.c0 AS c0,
    subq_0.c0 AS c1,
    subq_0.c0 AS c2
FROM (
    SELECT
        ref_1.ps_suppkey AS c0
    FROM
        main.region AS ref_0
        INNER JOIN main.partsupp AS ref_1
        RIGHT JOIN main.part AS ref_2 ON (ref_2.p_mfgr IS NOT NULL) ON (ref_0.r_comment = ref_1.ps_comment)
    WHERE
        CASE WHEN EXISTS (
                SELECT
                    ref_1.ps_comment AS c0, ref_1.ps_comment AS c1, (
                        SELECT
                            s_comment
                        FROM
                            main.supplier
                        LIMIT 1 offset 2) AS c2,
                    ref_2.p_comment AS c3,
                    ref_1.ps_supplycost AS c4,
                    ref_0.r_regionkey AS c5
                FROM
                    main.orders AS ref_3
                WHERE (ref_1.ps_comment IS NULL)
                AND (0)) THEN
            ref_0.r_comment
        ELSE
            ref_0.r_comment
        END IS NOT NULL
    LIMIT 106) AS subq_0
WHERE (subq_0.c0 IS NOT NULL)
OR ((0)
    AND (subq_0.c0 IS NOT NULL))
LIMIT 120
