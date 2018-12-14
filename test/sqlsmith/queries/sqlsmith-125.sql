SELECT
    subq_1.c2 AS c0,
    subq_1.c1 AS c1,
    subq_1.c3 AS c2,
    subq_1.c0 AS c3,
    16 AS c4,
    subq_1.c0 AS c5,
    (
        SELECT
            l_discount
        FROM
            main.lineitem
        LIMIT 1 offset 44) AS c6,
    subq_1.c5 AS c7,
    subq_1.c4 AS c8,
    subq_1.c0 AS c9,
    subq_1.c5 AS c10,
    subq_1.c2 AS c11,
    34 AS c12,
    subq_1.c5 AS c13,
    subq_1.c5 AS c14,
    (
        SELECT
            ps_comment
        FROM
            main.partsupp
        LIMIT 1 offset 1) AS c15,
    subq_1.c2 AS c16,
    CASE WHEN 86 IS NOT NULL THEN
        subq_1.c0
    ELSE
        subq_1.c0
    END AS c17,
    CAST(nullif (subq_1.c3, CAST(NULL AS VARCHAR)) AS VARCHAR) AS c18,
    subq_1.c3 AS c19,
    subq_1.c5 AS c20,
    subq_1.c0 AS c21,
    subq_1.c4 AS c22
FROM (
    SELECT
        ref_1.p_retailprice AS c0,
        subq_0.c8 AS c1,
        subq_0.c10 AS c2,
        subq_0.c1 AS c3,
        subq_0.c5 AS c4,
        ref_2.ps_partkey AS c5
    FROM (
        SELECT
            ref_0.l_orderkey AS c0,
            ref_0.l_linestatus AS c1,
            ref_0.l_suppkey AS c2,
            ref_0.l_linenumber AS c3,
            (
                SELECT
                    o_shippriority
                FROM
                    main.orders
                LIMIT 1 offset 72) AS c4,
            ref_0.l_partkey AS c5,
            ref_0.l_tax AS c6,
            (
                SELECT
                    c_name
                FROM
                    main.customer
                LIMIT 1 offset 6) AS c7,
            ref_0.l_commitdate AS c8,
            (
                SELECT
                    c_acctbal
                FROM
                    main.customer
                LIMIT 1 offset 6) AS c9,
            ref_0.l_receiptdate AS c10,
            ref_0.l_quantity AS c11,
            ref_0.l_quantity AS c12,
            ref_0.l_discount AS c13,
            ref_0.l_partkey AS c14
        FROM
            main.lineitem AS ref_0
        WHERE
            ref_0.l_linenumber IS NULL
        LIMIT 66) AS subq_0
    LEFT JOIN main.part AS ref_1
    RIGHT JOIN main.partsupp AS ref_2 ON (ref_2.ps_partkey IS NOT NULL) ON (subq_0.c1 = ref_1.p_name)
WHERE
    subq_0.c11 IS NULL
LIMIT 110) AS subq_1
WHERE (CAST(coalesce(subq_1.c0, subq_1.c0) AS DECIMAL)
    IS NOT NULL)
AND (32 IS NULL)
LIMIT 67
