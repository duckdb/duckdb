SELECT
    CAST(coalesce(ref_0.p_size, CASE WHEN 1 THEN
                ref_0.p_size
            ELSE
                ref_0.p_size
            END) AS INTEGER) AS c0,
    ref_0.p_retailprice AS c1,
    ref_0.p_name AS c2,
    ref_0.p_retailprice AS c3,
    ref_0.p_container AS c4
FROM
    main.part AS ref_0
WHERE (ref_0.p_comment IS NULL)
AND (ref_0.p_mfgr IS NULL)
