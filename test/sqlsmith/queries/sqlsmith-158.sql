SELECT
    CAST(coalesce(ref_0.l_orderkey, CAST(coalesce(6, ref_0.l_quantity) AS INTEGER)) AS INTEGER) AS c0,
    (
        SELECT
            r_comment
        FROM
            main.region
        LIMIT 1 offset 5) AS c1,
    ref_0.l_tax AS c2,
    (
        SELECT
            c_phone
        FROM
            main.customer
        LIMIT 1 offset 11) AS c3,
    ref_0.l_extendedprice AS c4,
    ref_0.l_comment AS c5,
    CASE WHEN (((0)
                OR (0))
            OR (((((ref_0.l_returnflag IS NULL)
                            AND ((
                                    SELECT
                                        l_receiptdate
                                    FROM
                                        main.lineitem
                                    LIMIT 1 offset 1)
                                IS NULL))
                        AND (ref_0.l_linestatus IS NULL))
                    OR ((1)
                        OR ((
                                SELECT
                                    r_regionkey
                                FROM
                                    main.region
                                LIMIT 1 offset 86)
                            IS NOT NULL)))
                OR ((ref_0.l_linestatus IS NULL)
                    AND (((((1)
                                    AND (((EXISTS (
                                                    SELECT
                                                        1 AS c0
                                                    FROM
                                                        main.orders AS ref_1
                                                    WHERE
                                                        0
                                                    LIMIT 74))
                                            OR ((((1)
                                                        OR ((1)
                                                            AND (0)))
                                                    OR (1))
                                                OR (1)))
                                        AND (EXISTS (
                                                SELECT
                                                    ref_2.p_partkey AS c0
                                                FROM
                                                    main.part AS ref_2
                                                WHERE
                                                    EXISTS (
                                                        SELECT
                                                            ref_3.o_clerk AS c0, ref_2.p_container AS c1, ref_0.l_returnflag AS c2, ref_3.o_clerk AS c3, ref_2.p_type AS c4
                                                        FROM
                                                            main.orders AS ref_3
                                                        WHERE
                                                            ref_0.l_returnflag IS NULL
                                                        LIMIT 134)
                                                LIMIT 47))))
                                OR (ref_0.l_extendedprice IS NULL))
                            AND (((((ref_0.l_receiptdate IS NOT NULL)
                                            AND (ref_0.l_comment IS NULL))
                                        AND (((0)
                                                OR (0))
                                            AND ((0)
                                                AND (0))))
                                    AND (1))
                                OR (1)))
                        AND (ref_0.l_receiptdate IS NOT NULL)))))
        AND ((((((((1)
                                    OR (ref_0.l_receiptdate IS NULL))
                                AND ((1)
                                    OR (1)))
                            AND (1))
                        AND (((1)
                                AND (ref_0.l_shipmode IS NOT NULL))
                            OR (((1)
                                    OR (0))
                                AND ((ref_0.l_tax IS NOT NULL)
                                    OR (1)))))
                    OR ((ref_0.l_commitdate IS NULL)
                        OR ((ref_0.l_shipinstruct IS NULL)
                            OR ((0)
                                AND (1)))))
                AND (0))
            AND (EXISTS (
                    SELECT
                        (
                            SELECT
                                s_name
                            FROM
                                main.supplier
                            LIMIT 1 offset 4) AS c0
                    FROM
                        main.lineitem AS ref_4
                    WHERE
                        1
                    LIMIT 142))) THEN
        ref_0.l_linestatus
    ELSE
        ref_0.l_linestatus
    END AS c6,
    ref_0.l_linestatus AS c7,
    ref_0.l_discount AS c8,
    ref_0.l_comment AS c9,
    ref_0.l_receiptdate AS c10,
    ref_0.l_tax AS c11,
    CASE WHEN CASE WHEN EXISTS (
                SELECT
                    61 AS c0,
                    ref_0.l_discount AS c1
                FROM
                    main.region AS ref_5
                WHERE ((1)
                    AND (1))
                AND ((0)
                    OR ((ref_5.r_comment IS NULL)
                        OR (((ref_5.r_comment IS NULL)
                                AND (1))
                            OR (1))))) THEN
            ref_0.l_tax
        ELSE
            ref_0.l_tax
        END IS NULL THEN
        ref_0.l_shipdate
    ELSE
        ref_0.l_shipdate
    END AS c12, (
        SELECT
            l_orderkey
        FROM
            main.lineitem
        LIMIT 1 offset 2) AS c13,
    ref_0.l_orderkey AS c14,
    ref_0.l_returnflag AS c15,
    ref_0.l_extendedprice AS c16,
    ref_0.l_discount AS c17,
    ref_0.l_receiptdate AS c18
FROM
    main.lineitem AS ref_0
WHERE
    ref_0.l_linestatus IS NULL
LIMIT 179
