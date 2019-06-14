SELECT
    subq_1.c1 AS c0,
    subq_1.c1 AS c1,
    subq_1.c1 AS c2
FROM (
    SELECT
        subq_0.c5 AS c0,
        subq_0.c7 AS c1,
        subq_0.c4 AS c2
    FROM (
        SELECT
            80 AS c0,
            ref_2.c_custkey AS c1,
            ref_1.n_nationkey AS c2,
            ref_2.c_acctbal AS c3,
            30 AS c4,
            ref_2.c_nationkey AS c5,
            ref_2.c_phone AS c6,
            ref_0.s_comment AS c7
        FROM
            main.supplier AS ref_0
        RIGHT JOIN main.nation AS ref_1 ON (ref_0.s_phone = ref_1.n_name)
        INNER JOIN main.customer AS ref_2 ON ((ref_0.s_name IS NOT NULL)
                OR (0))
    WHERE ((EXISTS (
                SELECT
                    ref_2.c_acctbal AS c0, ref_3.c_address AS c1, ref_0.s_phone AS c2
                FROM
                    main.customer AS ref_3
                WHERE
                    ref_1.n_nationkey IS NULL
                LIMIT 115))
        AND ((
                SELECT
                    ps_availqty
                FROM
                    main.partsupp
                LIMIT 1 offset 5) IS NOT NULL))
    OR (ref_1.n_regionkey IS NOT NULL)
LIMIT 67) AS subq_0
WHERE
    subq_0.c7 IS NOT NULL) AS subq_1
WHERE ((subq_1.c0 IS NULL)
    OR (((EXISTS (
                    SELECT
                        ref_4.p_container AS c0
                    FROM
                        main.part AS ref_4
                    WHERE
                        subq_1.c1 IS NULL
                    LIMIT 109))
            OR (63 IS NULL))
        OR (1)))
AND ((1)
    AND (subq_1.c1 IS NULL))
LIMIT 166
