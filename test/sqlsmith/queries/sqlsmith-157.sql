SELECT
    ref_0.n_comment AS c0,
    ref_0.n_comment AS c1,
    (
        SELECT
            l_discount
        FROM
            main.lineitem
        LIMIT 1 offset 6) AS c2
FROM
    main.nation AS ref_0
WHERE
    EXISTS (
        SELECT
            CAST(coalesce(CAST(coalesce(ref_1.c_phone, ref_1.c_address) AS VARCHAR), CASE WHEN 0 THEN
                        ref_1.c_mktsegment
                    ELSE
                        ref_1.c_mktsegment
                    END) AS VARCHAR) AS c0, ref_0.n_name AS c1, ref_1.c_nationkey AS c2, ref_1.c_phone AS c3, ref_1.c_nationkey AS c4, ref_0.n_name AS c5
        FROM
            main.customer AS ref_1
        WHERE (1)
        AND (ref_1.c_address IS NULL)
    LIMIT 170)
