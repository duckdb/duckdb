SELECT
    subq_0.c6 AS c0,
    ref_2.l_comment AS c1,
    17 AS c2,
    ref_2.l_discount AS c3,
    subq_0.c3 AS c4
FROM (
    SELECT
        ref_0.r_name AS c0,
        ref_0.r_comment AS c1,
        ref_0.r_name AS c2,
        ref_0.r_regionkey AS c3,
        ref_0.r_regionkey AS c4,
        (
            SELECT
                l_comment
            FROM
                main.lineitem
            LIMIT 1 offset 5) AS c5,
        ref_0.r_comment AS c6,
        ref_0.r_name AS c7,
        ref_0.r_name AS c8,
        ref_0.r_name AS c9,
        CASE WHEN 1 THEN
            ref_0.r_regionkey
        ELSE
            ref_0.r_regionkey
        END AS c10,
        ref_0.r_regionkey AS c11
    FROM
        main.region AS ref_0
    WHERE
        ref_0.r_name IS NOT NULL) AS subq_0
    RIGHT JOIN main.lineitem AS ref_1
    INNER JOIN main.lineitem AS ref_2 ON (ref_2.l_commitdate IS NOT NULL) ON (subq_0.c6 = ref_1.l_returnflag)
WHERE (ref_2.l_shipdate IS NOT NULL)
OR ((((EXISTS (
                    SELECT
                        18 AS c0, ref_2.l_comment AS c1
                    FROM
                        main.supplier AS ref_3
                        INNER JOIN main.region AS ref_4 ON (ref_3.s_nationkey = ref_4.r_regionkey)
                    WHERE
                        0
                    LIMIT 114))
            AND (0))
        AND (ref_1.l_linestatus IS NOT NULL))
    OR (((subq_0.c9 IS NULL)
            AND (subq_0.c11 IS NULL))
        AND (ref_1.l_returnflag IS NULL)))
LIMIT 134
