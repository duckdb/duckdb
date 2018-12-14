SELECT
    subq_0.c0 AS c0,
    subq_0.c0 AS c1,
    subq_0.c0 AS c2,
    subq_0.c0 AS c3,
    (
        SELECT
            o_shippriority
        FROM
            main.orders
        LIMIT 1 offset 3) AS c4,
    subq_0.c0 AS c5,
    CAST(coalesce(subq_0.c0, subq_0.c0) AS VARCHAR) AS c6,
    CASE WHEN subq_0.c0 IS NOT NULL THEN
        subq_0.c0
    ELSE
        subq_0.c0
    END AS c7,
    subq_0.c0 AS c8,
    CAST(nullif (subq_0.c0, subq_0.c0) AS VARCHAR) AS c9
FROM (
    SELECT
        ref_0.p_name AS c0
    FROM
        main.part AS ref_0
    WHERE
        ref_0.p_mfgr IS NULL) AS subq_0
WHERE
    CASE WHEN ((EXISTS (
                    SELECT
                        ref_1.ps_suppkey AS c0, ref_1.ps_partkey AS c1, ref_1.ps_suppkey AS c2, subq_0.c0 AS c3, ref_1.ps_supplycost AS c4, ref_1.ps_availqty AS c5, subq_0.c0 AS c6, subq_0.c0 AS c7, subq_0.c0 AS c8, (
                            SELECT
                                s_nationkey
                            FROM
                                main.supplier
                            LIMIT 1 offset 1) AS c9,
                        ref_1.ps_comment AS c10,
                        (
                            SELECT
                                o_totalprice
                            FROM
                                main.orders
                            LIMIT 1 offset 4) AS c11,
                        subq_0.c0 AS c12,
                        subq_0.c0 AS c13,
                        ref_1.ps_comment AS c14,
                        ref_1.ps_availqty AS c15,
                        ref_1.ps_availqty AS c16,
                        subq_0.c0 AS c17
                    FROM
                        main.partsupp AS ref_1
                    WHERE
                        0))
                OR (0))
            AND (subq_0.c0 IS NOT NULL) THEN
            CASE WHEN 0 THEN
                CAST(coalesce(subq_0.c0, subq_0.c0) AS VARCHAR)
    ELSE
        CAST(coalesce(subq_0.c0, subq_0.c0) AS VARCHAR)
END
ELSE
    CASE WHEN 0 THEN
        CAST(coalesce(subq_0.c0, subq_0.c0) AS VARCHAR)
ELSE
    CAST(coalesce(subq_0.c0, subq_0.c0) AS VARCHAR)
END
END IS NOT NULL
LIMIT 29
