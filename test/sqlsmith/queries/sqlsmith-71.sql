SELECT
    CASE WHEN 1 THEN
        subq_1.c8
    ELSE
        subq_1.c8
    END AS c0,
    4 AS c1
FROM (
    SELECT
        subq_0.c0 AS c0,
        ref_4.s_acctbal AS c1,
        ref_3.s_address AS c2,
        (
            SELECT
                ps_suppkey
            FROM
                main.partsupp
            LIMIT 1 offset 3) AS c3,
        ref_3.s_address AS c4,
        55 AS c5,
        subq_0.c0 AS c6,
        subq_0.c1 AS c7,
        ref_4.s_phone AS c8,
        (
            SELECT
                c_phone
            FROM
                main.customer
            LIMIT 1 offset 5) AS c9,
        subq_0.c1 AS c10,
        subq_0.c1 AS c11,
        (
            SELECT
                l_shipmode
            FROM
                main.lineitem
            LIMIT 1 offset 4) AS c12,
        ref_3.s_nationkey AS c13
    FROM (
        SELECT
            ref_0.r_name AS c0,
            ref_0.r_name AS c1
        FROM
            main.region AS ref_0
        WHERE
            EXISTS (
                SELECT
                    ref_1.s_comment AS c0, ref_0.r_name AS c1, ref_0.r_comment AS c2, ref_0.r_name AS c3, ref_1.s_name AS c4, 66 AS c5
                FROM
                    main.supplier AS ref_1
                WHERE (((0)
                        OR (0))
                    AND (((1)
                            AND ((ref_0.r_name IS NULL)
                                AND (EXISTS (
                                        SELECT
                                            (
                                                SELECT
                                                    r_name
                                                FROM
                                                    main.region
                                                LIMIT 1 offset 5) AS c0,
                                            ref_1.s_name AS c1
                                        FROM
                                            main.nation AS ref_2
                                        WHERE
                                            0
                                        LIMIT 91))))
                        OR ((1)
                            OR (0))))
                OR (0)
            LIMIT 154)
    LIMIT 79) AS subq_0
    INNER JOIN main.supplier AS ref_3
    RIGHT JOIN main.supplier AS ref_4 ON (((1)
                AND (ref_3.s_phone IS NOT NULL))
            OR (1)) ON (subq_0.c0 = ref_3.s_name)
WHERE (ref_4.s_comment IS NOT NULL)
OR (0)
LIMIT 97) AS subq_1
WHERE
    0
