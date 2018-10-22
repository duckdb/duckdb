SELECT
    ref_0.l_shipmode AS c0,
    CAST(nullif (ref_0.l_linestatus, CAST(nullif (ref_0.l_shipmode, ref_0.l_shipmode) AS VARCHAR)) AS VARCHAR) AS c1
FROM
    main.lineitem AS ref_0
WHERE
    ref_0.l_tax IS NOT NULL
LIMIT 100
