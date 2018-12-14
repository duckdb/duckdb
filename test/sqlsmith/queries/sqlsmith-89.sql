SELECT
    subq_1.c13 AS c0,
    CAST(coalesce(subq_1.c7, subq_0.c9) AS INTEGER) AS c1
FROM (
    SELECT
        ref_0.n_regionkey AS c0,
        ref_0.n_nationkey AS c1,
        ref_0.n_regionkey AS c2,
        ref_0.n_name AS c3,
        (
            SELECT
                l_linestatus
            FROM
                main.lineitem
            LIMIT 1 offset 5) AS c4,
        ref_0.n_regionkey AS c5,
        ref_0.n_name AS c6,
        ref_0.n_name AS c7,
        (
            SELECT
                ps_comment
            FROM
                main.partsupp
            LIMIT 1 offset 6) AS c8,
        ref_0.n_nationkey AS c9
    FROM
        main.nation AS ref_0
    WHERE
        ref_0.n_regionkey IS NOT NULL
    LIMIT 119) AS subq_0
    INNER JOIN (
        SELECT
            ref_1.s_nationkey AS c0,
            ref_1.s_suppkey AS c1,
            ref_1.s_name AS c2,
            ref_1.s_address AS c3,
            ref_1.s_name AS c4,
            (
                SELECT
                    ps_suppkey
                FROM
                    main.partsupp
                LIMIT 1 offset 5) AS c5,
            ref_1.s_comment AS c6,
            76 AS c7,
            ref_1.s_address AS c8,
            ref_1.s_suppkey AS c9,
            53 AS c10,
            CAST(coalesce(ref_1.s_address, ref_1.s_name) AS VARCHAR) AS c11,
            (
                SELECT
                    p_container
                FROM
                    main.part
                LIMIT 1 offset 5) AS c12,
            ref_1.s_comment AS c13
        FROM
            main.supplier AS ref_1
        WHERE (ref_1.s_suppkey IS NOT NULL)
        OR ((ref_1.s_suppkey IS NOT NULL)
            OR (1))) AS subq_1 ON (subq_0.c1 = subq_1.c0)
WHERE
    subq_0.c7 IS NOT NULL
LIMIT 89
