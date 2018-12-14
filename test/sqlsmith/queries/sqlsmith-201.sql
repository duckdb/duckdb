SELECT
    ref_0.l_shipinstruct AS c0,
    ref_1.s_suppkey AS c1,
    (
        SELECT
            s_phone
        FROM
            main.supplier
        LIMIT 1 offset 5) AS c2,
    ref_0.l_linestatus AS c3,
    CASE WHEN ref_2.ps_supplycost IS NOT NULL THEN
        ref_2.ps_availqty
    ELSE
        ref_2.ps_availqty
    END AS c4
FROM
    main.lineitem AS ref_0
    INNER JOIN main.supplier AS ref_1
    RIGHT JOIN main.partsupp AS ref_2 ON (ref_1.s_address IS NOT NULL) ON (ref_0.l_partkey = ref_2.ps_partkey)
WHERE ((EXISTS (
            SELECT
                ref_1.s_acctbal AS c0, ref_0.l_linenumber AS c1, ref_2.ps_comment AS c2, ref_3.c_comment AS c3, ref_2.ps_partkey AS c4, ref_2.ps_comment AS c5, ref_1.s_name AS c6, CASE WHEN ref_0.l_shipmode IS NULL THEN
                    ref_3.c_phone
                ELSE
                    ref_3.c_phone
                END AS c7, ref_0.l_shipmode AS c8, ref_2.ps_partkey AS c9, ref_2.ps_supplycost AS c10, (
                    SELECT
                        n_regionkey
                    FROM
                        main.nation
                    LIMIT 1 offset 1) AS c11,
                (
                    SELECT
                        ps_availqty
                    FROM
                        main.partsupp
                    LIMIT 1 offset 3) AS c12,
                ref_1.s_phone AS c13,
                ref_0.l_returnflag AS c14,
                ref_0.l_comment AS c15,
                ref_1.s_address AS c16,
                ref_2.ps_availqty AS c17,
                CAST(coalesce(ref_0.l_quantity, ref_1.s_suppkey) AS INTEGER) AS c18,
                ref_3.c_comment AS c19,
                ref_2.ps_partkey AS c20,
                ref_1.s_phone AS c21,
                ref_1.s_suppkey AS c22
            FROM
                main.customer AS ref_3
            WHERE (ref_2.ps_comment IS NULL)
            AND ((1)
                OR ((ref_2.ps_supplycost IS NULL)
                    OR (((1)
                            AND ((0)
                                OR ((((ref_3.c_mktsegment IS NULL)
                                            OR (ref_3.c_address IS NULL))
                                        OR ((ref_0.l_comment IS NOT NULL)
                                            OR (0)))
                                    OR ((1)
                                        OR (1)))))
                        OR (((ref_0.l_commitdate IS NULL)
                                OR ((0)
                                    AND ((1)
                                        AND (0))))
                            AND (1)))))
        LIMIT 72))
AND (1))
AND ((1)
    AND (((ref_0.l_quantity IS NULL)
            AND (1))
        OR ((
                SELECT
                    l_comment
                FROM
                    main.lineitem
                LIMIT 1 offset 1)
            IS NOT NULL)))
LIMIT 65
