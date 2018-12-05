SELECT
    subq_0.c4 AS c0,
    subq_0.c0 AS c1,
    subq_0.c3 AS c2,
    subq_0.c2 AS c3,
    subq_0.c5 AS c4,
    subq_0.c1 AS c5
FROM (
    SELECT
        ref_0.s_nationkey AS c0,
        CAST(nullif (ref_0.s_address, ref_1.n_name) AS VARCHAR) AS c1,
        ref_1.n_name AS c2,
        (
            SELECT
                o_custkey
            FROM
                main.orders
            LIMIT 1 offset 5) AS c3,
        ref_0.s_nationkey AS c4,
        ref_1.n_name AS c5
    FROM
        main.supplier AS ref_0
    RIGHT JOIN main.nation AS ref_1 ON ((0)
            AND (ref_1.n_comment IS NOT NULL))
WHERE
    ref_1.n_name IS NOT NULL
LIMIT 35) AS subq_0
WHERE
    1
