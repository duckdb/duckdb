SELECT
    subq_0.c0 AS c0
FROM (
    SELECT
        ref_6.r_name AS c0
    FROM
        main.nation AS ref_0
        INNER JOIN main.customer AS ref_1
        RIGHT JOIN main.lineitem AS ref_2
        LEFT JOIN main.nation AS ref_3 ON ((1)
                OR (ref_2.l_partkey IS NULL)) ON (ref_1.c_phone = ref_2.l_returnflag) ON (ref_0.n_nationkey IS NOT NULL)
        LEFT JOIN main.customer AS ref_4
        LEFT JOIN main.region AS ref_5
        INNER JOIN main.region AS ref_6 ON (ref_5.r_comment = ref_6.r_name) ON (ref_6.r_name IS NULL) ON (ref_0.n_name IS NULL)
    WHERE
        ref_0.n_regionkey IS NOT NULL
    LIMIT 67) AS subq_0
WHERE
    24 IS NULL
LIMIT 85
