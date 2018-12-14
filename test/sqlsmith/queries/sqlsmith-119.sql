SELECT
    ref_0.n_name AS c0,
    ref_0.n_nationkey AS c1,
    ref_0.n_comment AS c2,
    (
        SELECT
            s_nationkey
        FROM
            main.supplier
        LIMIT 1 offset 32) AS c3,
    ref_0.n_nationkey AS c4,
    ref_0.n_regionkey AS c5,
    (
        SELECT
            n_name
        FROM
            main.nation
        LIMIT 1 offset 6) AS c6,
    (
        SELECT
            c_phone
        FROM
            main.customer
        LIMIT 1 offset 6) AS c7,
    (
        SELECT
            p_mfgr
        FROM
            main.part
        LIMIT 1 offset 1) AS c8,
    CASE WHEN ref_0.n_name IS NOT NULL THEN
        ref_0.n_name
    ELSE
        ref_0.n_name
    END AS c9,
    ref_0.n_name AS c10,
    CASE WHEN CAST(coalesce(84, ref_0.n_nationkey) AS INTEGER)
    IS NOT NULL THEN
    ref_0.n_nationkey
ELSE
    ref_0.n_nationkey
END AS c11,
ref_0.n_nationkey AS c12,
CAST(coalesce(ref_0.n_regionkey, ref_0.n_nationkey) AS INTEGER) AS c13,
    ref_0.n_name AS c14,
    CASE WHEN ref_0.n_regionkey IS NULL THEN
        ref_0.n_comment
    ELSE
        ref_0.n_comment
    END AS c15,
    ref_0.n_comment AS c16,
    ref_0.n_nationkey AS c17,
    CAST(coalesce(50, ref_0.n_nationkey) AS INTEGER) AS c18,
    CAST(nullif (9, ref_0.n_regionkey) AS INTEGER) AS c19
FROM
    main.nation AS ref_0
WHERE
    ref_0.n_name IS NULL
LIMIT 84
