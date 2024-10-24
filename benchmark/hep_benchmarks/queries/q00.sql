--SNOWFLAKE
SELECT
  HistogramBin(pt, 0, 2000, 100) AS x,
  COUNT(*) AS y
FROM (
  SELECT ANY_VALUE(MET:pt) AS pt
  FROM 
    {input_table}
    , lateral flatten(input => Muon::array) AS m1
    , lateral flatten(input => Muon::array) AS m2
  WHERE 
    m1.index < m2.index 
    AND m1.value:charge != m2.value:charge
    AND SQRT(2 * m1.value:pt * m2.value:pt 
      * (COSH(m1.value:eta - m2.value:eta) 
        - COS(m1.value:phi - m2.value:phi))) BETWEEN 60 AND 120
  GROUP BY EVENT
)
GROUP BY x
ORDER BY x;

--BIGQUERY
SELECT
  HistogramBin(MET.pt, 0, 2000, 100) AS x,
  COUNT(*) AS y
FROM `{bigquery_dataset}.{input_table}`
WHERE ARRAY_LENGTH(Muon) >= 2 AND
  (SELECT COUNT(*) AS mass
   FROM UNNEST(Muon) m1 WITH OFFSET i
   CROSS JOIN UNNEST(Muon) m2 WITH OFFSET j
   WHERE
     m1.charge <> m2.charge AND i < j AND
     SQRT(2*m1.pt*m2.pt*(COSH(m1.eta-m2.eta)-COS(m1.phi-m2.phi))) BETWEEN 60 AND 120) > 0
GROUP BY x
ORDER BY x

--PRESTO
WITH temp AS (
  SELECT event, MET.pt, COUNT(*)
  FROM {input_table}
  CROSS JOIN UNNEST(Muon) WITH ORDINALITY
    AS m1 (pt, eta, phi, mass, charge, pfRelIso03_all, pfRelIso04_all, tightId,
           softId, dxy, dxyErr, dz, dzErr, jetIdx, genPartIdx, idx)
  CROSS JOIN UNNEST(Muon) WITH ORDINALITY
    AS m2 (pt, eta, phi, mass, charge, pfRelIso03_all, pfRelIso04_all, tightId,
           softId, dxy, dxyErr, dz, dzErr, jetIdx, genPartIdx, idx)
  WHERE
    cardinality(Muon) > 1 AND
    m1.idx < m2.idx AND
    m1.charge <> m2.charge AND
    SQRT(2 * m1.pt * m2.pt * (COSH(m1.eta - m2.eta) - COS(m1.phi - m2.phi))) BETWEEN 60 AND 120
  GROUP BY event, MET.pt
  HAVING COUNT(*) > 0
)
SELECT
  mysql.default.HistogramBin(pt, 0, 2000, 100) AS x,
  COUNT(*) AS y
FROM temp
GROUP BY mysql.default.HistogramBin(pt, 0, 2000, 100)
ORDER BY x;

--ATHENA
WITH temp AS (
  SELECT event, MET.pt, COUNT(*)
  FROM {input_table}
  CROSS JOIN UNNEST(Muon) WITH ORDINALITY AS _m1(m1, idx1)
  CROSS JOIN UNNEST(Muon) WITH ORDINALITY AS _m2(m2, idx2)
  WHERE
    cardinality(Muon) > 1 AND
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

--SQLPP
histogram((
    FROM %(input_table)s AS e
    WHERE EXISTS (
      FROM e.Muon AS m1 AT idx1,
           e.Muon AS m2 AT idx2
      WHERE
        idx1 < idx2 AND
        m1.charge != m2.charge AND
        computeInvariantMass(m1, m2) BETWEEN 60 AND 120
      SELECT *)
    SELECT VALUE MET.pt),
  0, 2000, 100);

  --