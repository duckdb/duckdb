WITH uniform_structure_leptons AS (
  SELECT
    event,
    MET,
    array_union(
      transform(
        COALESCE(Muon, ARRAY []),
        x -> CAST( ROW(x.pt, x.eta, x.phi, x.mass, x.charge, 'm') AS ROW( pt REAL, eta REAL, phi REAL, mass REAL, charge INTEGER, type CHAR ) )
      ),
      transform(
        COALESCE(Electron, ARRAY []),
        x -> CAST( ROW(x.pt, x.eta, x.phi, x.mass, x.charge, 'e') AS ROW( pt REAL, eta REAL, phi REAL, mass REAL, charge INTEGER, type CHAR ) )
      )
    ) AS Leptons
  FROM hep_singleMu
  WHERE len(Muon) + len(Electron) > 2
),
lepton_pairs AS (
  SELECT
    *,
    CAST(
      ROW(
        l1.pt * cos(l1.phi) + l2.pt * cos(l2.phi),
        l1.pt * sin(l1.phi) + l2.pt * sin(l2.phi),
        l1.pt * ( ( exp(l1.eta) - exp(-l1.eta) ) / 2.0 ) + l2.pt * ( ( exp(l2.eta) - exp(-l2.eta) ) / 2.0 ),
        sqrt(l1.pt * cosh(l1.eta) * l1.pt * cosh(l1.eta) + l1.mass * l1.mass) + sqrt(l2.pt * cosh(l2.eta) * l2.pt * cosh(l2.eta) + l2.mass * l2.mass)
      ) AS
      ROW (x REAL, y REAL, z REAL, e REAL)
    ) AS l,
    idx1 AS l1_idx,
    idx2 AS l2_idx
  FROM uniform_structure_leptons
  CROSS JOIN UNNEST(Leptons) WITH ORDINALITY AS _l1(l1, idx1)
  CROSS JOIN UNNEST(Leptons) WITH ORDINALITY AS _l2(l2, idx2)
  WHERE idx1 < idx2 AND l1.type = l2.type AND l1.charge != l2.charge
),
processed_pairs AS (
  SELECT
    event,
    min_by(
      ROW(
        l1_idx,
        l2_idx,
        Leptons,
        MET.pt,
        MET.phi
      ),
      abs(91.2 - sqrt(l.e * l.e - l.x * l.x - l.y * l.y - l.z * l.z))
    ) AS system
  FROM lepton_pairs
  GROUP BY event
),
other_max_pt AS (
  SELECT event, CAST(max_by(sqrt(2 * system.field3 * l.pt * (1.0 - cos((system.field4- l.phi + pi()) % (2 * pi()) - pi()))), l.pt) AS REAL) AS pt
  FROM processed_pairs
  CROSS JOIN UNNEST(system.field2) WITH ORDINALITY AS _l(l, idx)
  WHERE idx != system.field0 AND idx != system.field1
  GROUP BY event
)
SELECT
  FLOOR((
    CASE
      WHEN pt < 15 THEN 14.99
      WHEN pt > 250 THEN 250.1
      ELSE pt
    END - 0.9) / 2.35) * 2.35 + 2.075 AS x,
  COUNT(*) AS y
FROM other_max_pt
GROUP BY FLOOR((
    CASE
      WHEN pt < 15 THEN 14.99
      WHEN pt > 250 THEN 250.1
      ELSE pt
    END - 0.9) / 2.35) * 2.35 + 2.075
ORDER BY x;
