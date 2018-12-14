SELECT
    CASE WHEN ((1)
            OR (1))
        AND ((((1)
                    OR ((1)
                        OR ((((((0)
                                            OR (1))
                                        AND (0))
                                    OR (subq_0.c0 IS NULL))
                                OR ((((0)
                                            AND (subq_0.c0 IS NOT NULL))
                                        AND (subq_0.c0 IS NULL))
                                    OR ((0)
                                        AND (subq_0.c0 IS NULL))))
                            AND (0))))
                AND (0))
            AND (EXISTS (
                    SELECT
                        ref_5.ps_supplycost AS c0,
                        ref_5.ps_supplycost AS c1,
                        subq_0.c0 AS c2,
                        ref_5.ps_suppkey AS c3
                    FROM
                        main.partsupp AS ref_5
                    WHERE (subq_0.c0 IS NULL)
                    OR (1)
                LIMIT 111))) THEN
    21
ELSE
    21
END AS c0,
subq_0.c0 AS c1,
subq_0.c0 AS c2
FROM (
    SELECT
        ref_2.p_brand AS c0
    FROM
        main.customer AS ref_0
    RIGHT JOIN main.lineitem AS ref_1
    INNER JOIN main.part AS ref_2 ON (ref_1.l_shipdate IS NOT NULL) ON (ref_0.c_comment = ref_1.l_returnflag)
WHERE
    EXISTS (
        SELECT
            ref_4.ps_suppkey AS c0
        FROM
            main.customer AS ref_3
        LEFT JOIN main.partsupp AS ref_4 ON (ref_3.c_name = ref_4.ps_comment)
    WHERE (1)
    AND (1))
LIMIT 152) AS subq_0
WHERE
    1
LIMIT 125
