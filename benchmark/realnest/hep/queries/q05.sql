WITH temp AS (
  WITH m as (select unnest(Muon) as m from hep_singleMu )
  SELECT rowid, MET.pt, COUNT(*)
  FROM hep_singleMu
  CROSS JOIN (
    SELECT row_number() OVER () idx1, m.charge as c1, m.eta as e1, m.phi as ph1, m.pt as p1
    FROM m
  ) AS _m1
  CROSS JOIN (
    SELECT row_number() OVER () idx2, m.charge as c2, m.eta as e2, m.phi as ph2, m.pt as p2
    FROM m
  ) AS _m2
  WHERE
    len(Muon) > 1 AND
    idx1 < idx2 AND
    c1 <> c2 AND
    SQRT(2 * p1 * p2 * (COSH(e1 - e2) - COS(ph1 - ph2))) BETWEEN 60 AND 120
  GROUP BY rowid, MET.pt
  HAVING COUNT(*) > 0
)
SELECT
  FLOOR((
    CASE
      WHEN pt < 0 THEN -1
      WHEN pt > 2000 THEN 2001
      ELSE pt
    END) / 20) * 20 + 10 AS x,
  COUNT(*) AS y
FROM temp
GROUP BY FLOOR((
    CASE
      WHEN pt < 0 THEN -1
      WHEN pt > 2000 THEN 2001
      ELSE pt
    END) / 20) * 20 + 10
ORDER BY x;