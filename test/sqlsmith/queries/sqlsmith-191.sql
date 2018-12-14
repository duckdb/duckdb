SELECT
    subq_1.c3 AS c0,
    subq_1.c3 AS c1,
    subq_1.c3 AS c2,
    subq_1.c0 AS c3
FROM (
    SELECT
        80 AS c0,
        subq_0.c6 AS c1,
        subq_0.c3 AS c2,
        CAST(coalesce(subq_0.c4, subq_0.c7) AS VARCHAR) AS c3
    FROM (
        SELECT
            ref_0.c_phone AS c0,
            ref_2.s_address AS c1,
            ref_0.c_phone AS c2,
            ref_2.s_acctbal AS c3,
            ref_0.c_name AS c4,
            93 AS c5,
            (
                SELECT
                    r_regionkey
                FROM
                    main.region
                LIMIT 1 offset 5) AS c6,
            ref_0.c_comment AS c7,
            ref_2.s_acctbal AS c8
        FROM
            main.customer AS ref_0
        RIGHT JOIN main.part AS ref_1
        INNER JOIN main.supplier AS ref_2 ON (ref_1.p_retailprice IS NOT NULL) ON (ref_0.c_name = ref_1.p_name)
    WHERE ((0)
        OR ((
                SELECT
                    n_name
                FROM
                    main.nation
                LIMIT 1 offset 3)
            IS NOT NULL))
    AND (ref_0.c_acctbal IS NOT NULL)) AS subq_0
WHERE
    CASE WHEN EXISTS (
            SELECT
                subq_0.c0 AS c0, subq_0.c2 AS c1, (
                    SELECT
                        l_shipdate
                    FROM
                        main.lineitem
                    LIMIT 1 offset 5) AS c2,
                ref_3.ps_comment AS c3,
                ref_3.ps_availqty AS c4
            FROM
                main.partsupp AS ref_3
            WHERE
                EXISTS (
                    SELECT
                        subq_0.c8 AS c0
                    FROM
                        main.orders AS ref_4
                    WHERE (ref_3.ps_partkey IS NOT NULL)
                    OR (ref_4.o_orderkey IS NOT NULL)
                LIMIT 102)
        LIMIT 71) THEN
    (
        SELECT
            o_custkey
        FROM
            main.orders
        LIMIT 1 offset 2)
ELSE
    (
        SELECT
            o_custkey
        FROM
            main.orders
        LIMIT 1 offset 2)
END IS NULL
LIMIT 18) AS subq_1
WHERE (((subq_1.c0 IS NULL)
        OR (0))
    AND (subq_1.c0 IS NULL))
OR ((
        SELECT
            n_name
        FROM
            main.nation
        LIMIT 1 offset 53)
    IS NULL)
LIMIT 109
