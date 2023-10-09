copy (SELECT
  s_name,
  count(*) AS numwait
FROM
    supplier,
    lineitem l1,
    orders,
    nation
WHERE
  o_orderstatus = 'F' AND
--   n_name = 'SAUDI ARABIA' AND
--   l1.l_receiptdate > l1.l_commitdate AND
      s_suppkey = l1.l_suppkey AND
      o_orderkey = l1.l_orderkey AND
      s_suppkey = l1.l_suppkey
        AND EXISTS (
            SELECT
            *
            FROM
            lineitem l2
            WHERE
            l2.l_orderkey = l1.l_orderkey
              AND l2.l_suppkey <> l1.l_suppkey
        )
      AND s_nationkey = n_nationkey
GROUP BY
  s_name
ORDER BY
  numwait DESC,
  s_name
  LIMIT 100) to 'no_anti_orig.csv' (FORMAT CSV);
