SELECT
    27 AS c0,
    ref_0.ps_availqty AS c1,
    ref_0.ps_supplycost AS c2,
    ref_0.ps_partkey AS c3,
    CAST(coalesce(ref_0.ps_supplycost, ref_0.ps_supplycost) AS DECIMAL) AS c4,
    ref_0.ps_supplycost AS c5,
    ref_0.ps_partkey AS c6,
    (
        SELECT
            l_linestatus
        FROM
            main.lineitem
        LIMIT 1 offset 2) AS c7,
    CAST(nullif (ref_0.ps_comment, ref_0.ps_comment) AS VARCHAR) AS c8,
    ref_0.ps_supplycost AS c9,
    ref_0.ps_supplycost AS c10,
    ref_0.ps_comment AS c11,
    85 AS c12,
    ref_0.ps_comment AS c13,
    ref_0.ps_supplycost AS c14,
    ref_0.ps_availqty AS c15,
    ref_0.ps_partkey AS c16,
    14 AS c17,
    (
        SELECT
            c_name
        FROM
            main.customer
        LIMIT 1 offset 2) AS c18
FROM
    main.partsupp AS ref_0
WHERE (ref_0.ps_suppkey IS NULL)
    OR (CAST(nullif (ref_0.ps_suppkey, 76) AS INTEGER) IS NOT NULL)
LIMIT 121
