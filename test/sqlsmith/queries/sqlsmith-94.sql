SELECT
    ref_0.o_orderdate AS c0,
    CAST(coalesce(
            CASE WHEN 0 THEN
                ref_0.o_clerk
            ELSE
                ref_0.o_clerk
            END, ref_0.o_orderstatus) AS VARCHAR) AS c1,
    ref_0.o_orderdate AS c2,
    ref_0.o_clerk AS c3,
    ref_0.o_totalprice AS c4,
    CASE WHEN ((1)
            AND (1))
        OR (((((ref_0.o_orderstatus IS NOT NULL)
                        OR (0))
                    AND (ref_0.o_orderdate IS NULL))
                OR (EXISTS (
                        SELECT
                            ref_0.o_totalprice AS c0,
                            ref_1.l_tax AS c1,
                            ref_1.l_shipinstruct AS c2,
                            ref_0.o_orderstatus AS c3,
                            ref_0.o_orderdate AS c4,
                            ref_0.o_clerk AS c5,
                            ref_1.l_tax AS c6,
                            ref_1.l_comment AS c7
                        FROM
                            main.lineitem AS ref_1
                        WHERE ((ref_1.l_tax IS NOT NULL)
                            OR (0))
                        AND ((EXISTS (
                                    SELECT
                                        (
                                            SELECT
                                                s_address
                                            FROM
                                                main.supplier
                                            LIMIT 1 offset 5) AS c0,
                                        ref_0.o_clerk AS c1,
                                        ref_0.o_custkey AS c2,
                                        ref_1.l_receiptdate AS c3,
                                        ref_1.l_extendedprice AS c4,
                                        ref_2.ps_suppkey AS c5,
                                        ref_0.o_shippriority AS c6,
                                        ref_0.o_shippriority AS c7,
                                        ref_2.ps_comment AS c8,
                                        ref_1.l_receiptdate AS c9,
                                        ref_0.o_clerk AS c10,
                                        ref_0.o_orderdate AS c11
                                    FROM
                                        main.partsupp AS ref_2
                                    WHERE
                                        ref_0.o_totalprice IS NOT NULL
                                    LIMIT 66))
                            AND (((0)
                                    AND (1))
                                OR (1))))))
            AND (ref_0.o_totalprice IS NOT NULL)) THEN
        ref_0.o_orderkey
    ELSE
        ref_0.o_orderkey
    END AS c5,
    ref_0.o_orderpriority AS c6,
    ref_0.o_orderkey AS c7,
    ref_0.o_orderstatus AS c8,
    ref_0.o_comment AS c9
FROM
    main.orders AS ref_0
WHERE (ref_0.o_orderpriority IS NULL)
OR ((0)
    OR (ref_0.o_orderkey IS NULL))
LIMIT 164
