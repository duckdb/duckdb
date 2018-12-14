SELECT
    subq_0.c3 AS c0,
    subq_0.c3 AS c1,
    96 AS c2,
    subq_0.c2 AS c3
FROM (
    SELECT
        ref_2.s_suppkey AS c0,
        ref_1.l_tax AS c1,
        ref_0.p_comment AS c2,
        ref_1.l_linenumber AS c3,
        CASE WHEN (0)
            OR (((((ref_0.p_container IS NOT NULL)
                            AND (0))
                        AND (ref_1.l_comment IS NULL))
                    OR ((
                            SELECT
                                n_regionkey
                            FROM
                                main.nation
                            LIMIT 1 offset 2)
                        IS NULL))
                OR (ref_1.l_partkey IS NULL)) THEN
            ref_0.p_retailprice
        ELSE
            ref_0.p_retailprice
        END AS c4
    FROM
        main.part AS ref_0
    RIGHT JOIN main.lineitem AS ref_1
    INNER JOIN main.supplier AS ref_2 ON ((1)
            OR ((1)
                OR (38 IS NOT NULL))) ON (ref_0.p_name = ref_1.l_returnflag)
WHERE ((1)
    AND (1))
AND (((ref_0.p_container IS NOT NULL)
        OR ((ref_1.l_quantity IS NOT NULL)
            AND (ref_0.p_type IS NOT NULL)))
    OR (((((ref_0.p_partkey IS NOT NULL)
                    OR (1))
                OR ((1)
                    OR (ref_2.s_nationkey IS NULL)))
            OR ((ref_2.s_name IS NULL)
                OR (0)))
        OR (0)))
LIMIT 78) AS subq_0
WHERE
    subq_0.c2 IS NULL
LIMIT 149
