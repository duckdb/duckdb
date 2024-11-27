WITH xyze_jets AS (
  SELECT
    rowid,
    list_transform(JET,
              j -> CAST(ROW(j.btag,
                            j.pt * cos(j.phi),
                            j.pt * sin(j.phi),
                            j.pt * ( ( exp(j.eta) - exp(-j.eta) ) / 2.0 ),
                            sqrt(j.pt * cosh(j.eta) * j.pt * cosh(j.eta) + j.mASs * j.mASs)) AS
                        ROW (btag REAL, x REAL, y REAL, z REAL, e REAL))) AS Jet
  FROM hep_singleMu
),
tri_jets AS (
  WITH m AS (SELECT unnest(Jet) AS m FROM hep_singleMu)
  SELECT m1, m2, m3, idx1, idx2, idx3
  FROM (
    SELECT row_number() OVER (Partition BY rowid) idx1, m1
    FROM (SELECT rowid, unnest(jet) AS m1 FROM xyze_jets)
  ) AS _m1
  CROSS JOIN (
    SELECT row_number() OVER (Partition BY rowid) idx2, m2
    FROM (SELECT rowid, unnest(jet) AS m2 FROM xyze_jets)
  ) AS _m2
  CROSS JOIN (
    SELECT row_number() OVER (Partition BY rowid) idx3, m3
    FROM (SELECT rowid, unnest(jet) AS m3 FROM xyze_jets)
  ) AS _m3
  WHERE idx1 < idx2 AND idx2 < idx3
),
condensed_tri_jet AS (
  SELECT
    idx1, idx2, idx3, m1, m2, m3,
    m1.x + m2.x + m3.x AS x,
    m1.y + m2.y + m3.y AS y,
    m1.z + m2.z + m3.z AS z,
    m1.e + m2.e + m3.e AS e,
    (m1.x + m2.x + m3.x) * (m1.x + m2.x + m3.x) AS x2,
    (m1.y + m2.y + m3.y) * (m1.y + m2.y + m3.y) AS y2,
    (m1.z + m2.z + m3.z) * (m1.z + m2.z + m3.z) AS z2,
    (m1.e + m2.e + m3.e) * (m1.e + m2.e + m3.e) AS e2
  FROM tri_jets
),
singular_system AS (
  SELECT
    idx1, idx2, idx3,
    min_by(
        list_max([m1.btag, m2.btag, m3.btag]),
        abs(172.5 - sqrt(e2 - x2 - y2 - z2))
    ) AS btag
  FROM condensed_tri_jet
  GROUP BY idx1, idx2, idx3
)
SELECT
    CAST((
     CASE
         WHEN btag < 0 THEN -0.005
         WHEN btag > 1 THEN 1.005
         ELSE btag
         END - 0.005) / 0.01 AS BIGINT) * 0.01 + 0.005 AS x,
    COUNT(*) AS y
FROM singular_system
GROUP BY CAST((
  CASE
      WHEN btag < 0 THEN -0.005
      WHEN btag > 1 THEN 1.005
      ELSE btag
      END - 0.005) / 0.01 AS BIGINT) * 0.01 + 0.005
ORDER BY x;