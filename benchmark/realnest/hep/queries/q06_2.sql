WITH xyze_jets AS (
  SELECT
    rowid,
    list_transform(JET,
              j -> CAST(ROW(j.btag,
                            j.btag * cos(j.phi),
                            j.btag * sin(j.phi),
                            j.btag * ( ( exp(j.eta) - exp(-j.eta) ) / 2.0 ),
                            sqrt(j.btag * cosh(j.eta) * j.btag * cosh(j.eta) + j.mass * j.mass)) AS
                        ROW (btag REAL, x REAL, y REAL, z REAL, e REAL))) AS Jet
  FROM hep_singleMu
),
tri_jets AS (
  WITH m as (select unnest(Jet) as m from hep_singleMu)
    SELECT rowid, m1, m2, m3
    FROM xyze_jets
    CROSS JOIN (
      SELECT row_number() OVER () idx1, unnest(jet) as m1
      FROM xyze_jets
    ) AS _m1
    CROSS JOIN (
      SELECT row_number() OVER () idx2, unnest(jet) as m2
      FROM xyze_jets
    ) AS _m2
    CROSS JOIN (
      SELECT row_number() OVER () idx3, unnest(jet) as m3 
      FROM xyze_jets
    ) AS _m3
    WHERE idx1 < idx2 AND idx2 < idx3
),
condensed_tri_jet AS (
  SELECT
    rowid, m1, m2, m3,
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
    rowid,
    min_by(
      sqrt(x2 + y2),
      abs(172.5 - sqrt(e2 - x2 - y2 - z2))
    ) AS btag
  FROM condensed_tri_jet
  GROUP BY rowid
)
SELECT
  FLOOR((
    CASE
      WHEN btag < 15 THEN 14.99
      WHEN btag > 40 THEN 40.01
      ELSE btag
    END) / 0.25) * 0.25 + 0.125 AS x,
  COUNT(*) AS y
FROM singular_system
GROUP BY FLOOR((
    CASE
      WHEN btag < 15 THEN 14.99
      WHEN btag > 40 THEN 40.01
      ELSE btag
    END) / 0.25) * 0.25 + 0.125
ORDER BY x;