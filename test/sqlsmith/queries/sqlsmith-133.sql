SELECT
    subq_0.c10 AS c0
FROM
    main.orders AS ref_0
    LEFT JOIN (
        SELECT
            ref_1.p_container AS c0,
            ref_1.p_name AS c1,
            ref_1.p_partkey AS c2,
            ref_1.p_size AS c3,
            (
                SELECT
                    n_name
                FROM
                    main.nation
                LIMIT 1 offset 3) AS c4,
            ref_1.p_name AS c5,
            ref_1.p_mfgr AS c6,
            ref_1.p_size AS c7,
            ref_1.p_type AS c8,
            ref_1.p_size AS c9,
            ref_1.p_container AS c10,
            ref_1.p_size AS c11,
            (
                SELECT
                    s_acctbal
                FROM
                    main.supplier
                LIMIT 1 offset 5) AS c12,
            ref_1.p_size AS c13,
            74 AS c14,
            ref_1.p_brand AS c15
        FROM
            main.part AS ref_1
        WHERE
            ref_1.p_mfgr IS NULL
        LIMIT 46) AS subq_0 ON (ref_0.o_clerk = subq_0.c0)
WHERE
    ref_0.o_comment IS NOT NULL
LIMIT 120
