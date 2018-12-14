SELECT
    ref_0.r_regionkey AS c0,
    ref_0.r_comment AS c1,
    CAST(coalesce(ref_0.r_name, ref_0.r_comment) AS VARCHAR) AS c2,
    ref_0.r_name AS c3,
    ref_0.r_comment AS c4,
    ref_0.r_comment AS c5,
    ref_0.r_regionkey AS c6,
    ref_0.r_regionkey AS c7,
    ref_0.r_name AS c8,
    ref_0.r_name AS c9,
    (
        SELECT
            o_orderdate
        FROM
            main.orders
        LIMIT 1 offset 4) AS c10,
    ref_0.r_regionkey AS c11,
    ref_0.r_comment AS c12,
    37 AS c13,
    ref_0.r_comment AS c14,
    ref_0.r_regionkey AS c15,
    CAST(coalesce(ref_0.r_regionkey, ref_0.r_regionkey) AS INTEGER) AS c16,
    ref_0.r_comment AS c17,
    ref_0.r_comment AS c18,
    CAST(nullif (ref_0.r_comment, CAST(coalesce(ref_0.r_comment, CASE WHEN (1)
                        AND (1) THEN
                        ref_0.r_name
                    ELSE
                        ref_0.r_name
                    END) AS VARCHAR)) AS VARCHAR) AS c19,
    ref_0.r_regionkey AS c20,
    ref_0.r_name AS c21
FROM
    main.region AS ref_0
WHERE (ref_0.r_name IS NOT NULL)
OR ((((ref_0.r_name IS NOT NULL)
            OR (EXISTS (
                    SELECT
                        ref_0.r_name AS c0, ref_0.r_regionkey AS c1, 34 AS c2, ref_0.r_regionkey AS c3, (
                            SELECT
                                l_commitdate
                            FROM
                                main.lineitem
                            LIMIT 1 offset 57) AS c4,
                        ref_0.r_regionkey AS c5,
                        ref_1.ps_supplycost AS c6
                    FROM
                        main.partsupp AS ref_1
                    WHERE (EXISTS (
                            SELECT
                                (
                                    SELECT
                                        l_partkey
                                    FROM
                                        main.lineitem
                                    LIMIT 1 offset 5) AS c0,
                                ref_0.r_regionkey AS c1,
                                ref_1.ps_suppkey AS c2
                            FROM
                                main.nation AS ref_2
                            WHERE
                                ref_1.ps_suppkey IS NULL))
                        AND (ref_0.r_name IS NULL))))
            OR ((1)
                AND (ref_0.r_comment IS NOT NULL)))
        OR (ref_0.r_name IS NOT NULL))
LIMIT 136
