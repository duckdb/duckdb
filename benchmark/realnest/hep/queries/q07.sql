WITH matching_jets AS (
  SELECT rowid, SUM(j.pt) AS pt_sum
  FROM hep_singleMu
  CROSS JOIN UNNEST(Jet) AS _j(j)
  WHERE
    j.pt > 30 AND
    len(
        filter(
            Electron,
            x -> x.pt > 10 AND
                 sqrt( (j.eta - x.eta) * (j.eta - x.eta) +
                       pow( (j.phi - x.phi + pi()) % (2 * pi()) - pi(), 2) ) < 0.4)) = 0 AND
    len(
        filter(
            Muon,
            x -> x.pt > 10 AND
                 sqrt( (j.eta - x.eta) * (j.eta - x.eta) +
                       pow( (j.phi - x.phi + pi()) % (2 * pi()) - pi(), 2) ) < 0.4)) = 0
  GROUP BY rowid
)
SELECT
  FLOOR((
    CASE
      WHEN pt_sum < 15 THEN 14.9
      WHEN pt_sum > 200 THEN 200.1
      ELSE pt_sum
    END - 0.2) / 1.85) * 1.85 + 1.125 AS x,
  COUNT(*) AS y
FROM matching_jets
GROUP BY FLOOR((
    CASE
      WHEN pt_sum < 15 THEN 14.9
      WHEN pt_sum > 200 THEN 200.1
      ELSE pt_sum
    END - 0.2) / 1.85) * 1.85 + 1.125
ORDER BY x;
