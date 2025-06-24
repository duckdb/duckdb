SELECT
  FLOOR((
    CASE
      WHEN j.pt < 15 THEN 14.99
      WHEN j.pt > 60 THEN 60.01
      ELSE j.pt
    END - 0.15) / 0.45) * 0.45 + 0.375 AS x,
  COUNT(*) AS y
FROM hep_singleMu
CROSS JOIN UNNEST(Jet) AS _j(j)
GROUP BY FLOOR((
    CASE
      WHEN j.pt < 15 THEN 14.99
      WHEN j.pt > 60 THEN 60.01
      ELSE j.pt
    END - 0.15) / 0.45) * 0.45 + 0.375
ORDER BY x;
