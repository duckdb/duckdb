WITH temp AS (
  SELECT event, MET.pt, COUNT(*)
  FROM run2012B_singleMu
  CROSS JOIN UNNEST(Muon) WITH ORDINALITY AS _m1(m1, idx1)
  CROSS JOIN UNNEST(Muon) WITH ORDINALITY AS _m2(m2, idx2)
  WHERE
    len(Muon) > 1 AND
    idx1 < idx2
    AND m1.charge <> m2.charge AND
    SQRT(2 * m1.pt * m2.pt * (COSH(m1.eta - m2.eta) - COS(m1.phi - m2.phi))) BETWEEN 60 AND 120
  GROUP BY event, MET.pt
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


WITH temp AS (
      SELECT
        event, MET.pt AS pt,
        COUNT(*) AS mass_count
      FROM hep_singleMu
      CROSS JOIN (
        SELECT m1, m1.charge as charge1, rowid AS idx1
        FROM (SELECT rowid, UNNEST(Muon) AS m1 from hep_singleMu)
      ) AS _m1
      CROSS JOIN (
        SELECT m2, m2.charge as charge2, rowid AS idx2
        FROM (SELECT rowid, UNNEST(Muon) AS m2 from hep_singleMu)
      ) AS _m2
      WHERE
        len(Muon) >= 2 AND
        idx1 < idx2 AND
        charge1 <> charge2 AND
        SQRT(2 * _m1.m1.pt * _m2.m2.pt
          * (COSH(_m1.m1.eta - _m2.m2.eta)
            - COS(_m1.m1.phi - _m2.m2.phi))) BETWEEN 60 AND 120
      GROUP BY event, MET.pt
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
    
