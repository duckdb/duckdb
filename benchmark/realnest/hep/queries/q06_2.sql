WITH trijets_b_tag_dval AS materialized (SELECT
    row_id_i,
    list_max(list_transform([j1, j2, j3], lambda x: x.btag)) AS b_tag_dval,
    AddPtEtaPhiM3({'pt':j1.pt, 'eta':j1.eta, 'phi':j1.phi, 'mass':j1.mass},
        {'pt':j2.pt, 'eta':j2.eta, 'phi':j2.phi, 'mass':j2.mass},
        {'pt':j3.pt, 'eta':j3.eta, 'phi':j3.phi, 'mass':j3.mass}) AS triJet,
    abs(triJet['mass'] - 172.5) AS invariant_mass
FROM (SELECT j1, row_number() OVER(Partition BY row_id) i, row_id row_id_i FROM (SELECT UNNEST(Jet) j1, rowid row_id FROM hep_singleMu WHERE array_length(jet) >= 3)),
   (SELECT j2, row_number() OVER(Partition BY row_id) j, row_id row_id_j FROM (SELECT UNNEST(Jet) j2, rowid row_id FROM hep_singleMu WHERE array_length(jet) >= 3)),
   (SELECT j3, row_number() OVER(Partition BY row_id) k, row_id row_id_k FROM (SELECT UNNEST(Jet) j3, rowid row_id FROM hep_singleMu WHERE array_length(jet) >= 3))
WHERE i < j AND j < k AND row_id_i = row_id_j AND row_id_j = row_id_k
ORDER BY invariant_mass asc)
SELECT 
    round(HistogramBin(trijets_b_tag_dval.b_tag_dval, 0, 1, 100),3) AS x, 
    count(*) AS y
FROM trijets_b_tag_dval
WHERE 
    invariant_mass in (SELECT min(tim_2.invariant_mass) FROM trijets_b_tag_dval tim_2 GROUP BY tim_2.row_id_i)
GROUP BY x
ORDER BY x;