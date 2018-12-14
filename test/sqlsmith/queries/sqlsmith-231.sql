SELECT
    ref_0.o_orderpriority AS c0,
    ref_0.o_totalprice AS c1,
    CAST(coalesce(
            CASE WHEN 0 THEN
                ref_0.o_custkey
            ELSE
                ref_0.o_custkey
            END, ref_0.o_shippriority) AS INTEGER) AS c2,
    (
        SELECT
            c_acctbal
        FROM
            main.customer
        LIMIT 1 offset 6) AS c3,
    CASE WHEN EXISTS (
            SELECT
                ref_1.n_comment AS c0,
                ref_1.n_name AS c1,
                (
                    SELECT
                        c_comment
                    FROM
                        main.customer
                    LIMIT 1 offset 3) AS c2,
                ref_0.o_orderpriority AS c3,
                ref_1.n_regionkey AS c4,
                ref_1.n_comment AS c5
            FROM
                main.nation AS ref_1
            WHERE (ref_1.n_comment IS NOT NULL)
            AND ((ref_0.o_comment IS NULL)
                OR (EXISTS (
                        SELECT
                            ref_2.c_name AS c0, ref_2.c_acctbal AS c1, ref_1.n_nationkey AS c2
                        FROM
                            main.customer AS ref_2
                        WHERE (1)
                        AND (0))))) THEN
        (
            SELECT
                n_name
            FROM
                main.nation
            LIMIT 1 offset 4)
    ELSE
        (
            SELECT
                n_name
            FROM
                main.nation
            LIMIT 1 offset 4)
    END AS c4,
    ref_0.o_shippriority AS c5,
    ref_0.o_clerk AS c6,
    ref_0.o_totalprice AS c7,
    ref_0.o_orderkey AS c8
FROM
    main.orders AS ref_0
WHERE
    ref_0.o_orderstatus IS NULL
