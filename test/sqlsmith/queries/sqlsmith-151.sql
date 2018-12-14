SELECT
    ref_0.l_shipinstruct AS c0,
    ref_0.l_shipinstruct AS c1,
    ref_0.l_receiptdate AS c2,
    ref_0.l_commitdate AS c3,
    ref_0.l_returnflag AS c4,
    ref_0.l_orderkey AS c5,
    CAST(nullif ((
                SELECT
                    s_acctbal FROM main.supplier
                LIMIT 1 offset 3), CAST(coalesce(ref_0.l_extendedprice, CAST(coalesce(ref_0.l_discount, ref_0.l_extendedprice) AS DECIMAL)) AS DECIMAL)) AS DECIMAL) AS c6
FROM
    main.lineitem AS ref_0
WHERE (ref_0.l_linestatus IS NOT NULL)
AND (((
            SELECT
                c_nationkey
            FROM
                main.customer
            LIMIT 1 offset 42)
        IS NULL)
    AND (EXISTS (
            SELECT
                (
                    SELECT
                        c_phone
                    FROM
                        main.customer
                    LIMIT 1 offset 6) AS c0,
                (
                    SELECT
                        p_retailprice
                    FROM
                        main.part
                    LIMIT 1 offset 1) AS c1,
                ref_0.l_linestatus AS c2,
                ref_0.l_comment AS c3,
                49 AS c4
            FROM
                main.partsupp AS ref_1
            WHERE ((1)
                AND (EXISTS (
                        SELECT
                            DISTINCT ref_0.l_commitdate AS c0, ref_0.l_receiptdate AS c1, ref_0.l_comment AS c2
                        FROM
                            main.customer AS ref_2
                        WHERE (EXISTS (
                                SELECT
                                    ref_2.c_address AS c0, ref_1.ps_supplycost AS c1, ref_0.l_discount AS c2
                                FROM
                                    main.supplier AS ref_3
                                WHERE
                                    ref_3.s_address IS NOT NULL
                                LIMIT 57))
                        OR ((1)
                            OR ((ref_1.ps_partkey IS NOT NULL)
                                AND ((1)
                                    AND (EXISTS (
                                            SELECT
                                                (
                                                    SELECT
                                                        l_commitdate
                                                    FROM
                                                        main.lineitem
                                                    LIMIT 1 offset 1) AS c0,
                                                ref_1.ps_suppkey AS c1,
                                                ref_4.p_size AS c2,
                                                ref_4.p_mfgr AS c3,
                                                ref_4.p_comment AS c4
                                            FROM
                                                main.part AS ref_4
                                            WHERE
                                                0)))))
                        LIMIT 88)))
            AND (ref_0.l_quantity IS NOT NULL)
        LIMIT 167)))
LIMIT 69
