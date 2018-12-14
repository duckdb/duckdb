SELECT
    subq_0.c0 AS c0
FROM (
    SELECT
        ref_2.ps_availqty AS c0
    FROM
        main.lineitem AS ref_0
    RIGHT JOIN main.customer AS ref_1
    INNER JOIN main.partsupp AS ref_2 ON (ref_1.c_mktsegment IS NOT NULL) ON (ref_0.l_shipmode = ref_1.c_name)
WHERE
    1) AS subq_0
WHERE
    subq_0.c0 IS NULL
LIMIT 157
