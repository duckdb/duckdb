SELECT
    CASE WHEN ref_0.o_totalprice IS NULL THEN
        ref_0.o_comment
    ELSE
        ref_0.o_comment
    END AS c0,
    CASE WHEN ref_0.o_custkey IS NOT NULL THEN
        CASE WHEN 0 THEN
            ref_0.o_custkey
        ELSE
            ref_0.o_custkey
        END
    ELSE
        CASE WHEN 0 THEN
            ref_0.o_custkey
        ELSE
            ref_0.o_custkey
        END
    END AS c1,
    (
        SELECT
            r_name
        FROM
            main.region
        LIMIT 1 offset 1) AS c2,
    ref_0.o_orderpriority AS c3,
    (
        SELECT
            c_phone
        FROM
            main.customer
        LIMIT 1 offset 4) AS c4
FROM
    main.orders AS ref_0
WHERE
    ref_0.o_totalprice IS NULL
