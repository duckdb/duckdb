ATTACH 'https://duckdb-blobs.s3.amazonaws.com/data/realnest/cord_10k.duckdb' AS cord (READ_ONLY);
CREATE TABLE cord AS SELECT * FROM cord.cord;
ATTACH 'https://duckdb-blobs.s3.amazonaws.com/data/realnest/open_street_map_524k.duckdb' AS osm (READ_ONLY);
CREATE TABLE open_street_map AS SELECT * FROM osm.open_street_map;
ATTACH 'https://duckdb-blobs.s3.amazonaws.com/data/realnest/pull_131k.duckdb' AS gh_pull (READ_ONLY);
CREATE TABLE gh_pull AS SELECT * FROM gh_pull.gh_pull;
ATTACH 'https://duckdb-blobs.s3.amazonaws.com/data/realnest/issue_131k.duckdb' AS gh_issue (READ_ONLY);
CREATE TABLE gh_issue AS SELECT * FROM gh_issue.gh_issue;
ATTACH 'https://duckdb-blobs.s3.amazonaws.com/data/realnest/twitter_131k.duckdb' AS tw (READ_ONLY);
CREATE TABLE twitter AS SELECT * FROM tw.twitter;
ATTACH 'https://duckdb-blobs.s3.amazonaws.com/data/realnest/singleMu_524k.duckdb' AS rn_singleMu (READ_ONLY);
CREATE TABLE run2012B_singleMu AS SELECT * FROM rn_singleMu.run2012B_singleMu;
CREATE TABLE single_mu_lists AS SELECT * REPLACE(
    list_resize(Jet, 10, NULL) AS Jet, list_resize(Muon, 10, NULL) AS Muon, 
    list_resize(Photon, 10, NULL) AS Photon, list_resize(Tau, 10, NULL) AS Tau) 
FROM rn_singleMu.run2012B_singleMu;
CREATE OR REPLACE TABLE singleMu AS 
SELECT 
    list_distinct(list_transform("Tau", x -> x.pt)) AS tau_pt, list_distinct(list_transform("Tau", x -> x.eta)) AS tau_eta,
    list_distinct(list_transform("Jet", x -> x.pt)) AS jet_pt, list_distinct(list_transform("Jet", x -> x.eta)) AS jet_eta, 
    list_distinct(list_transform("Muon", x -> x.pt)) AS muon_pt, list_distinct(list_transform("Muon", x -> x.eta)) AS muon_eta, 
    list_distinct(list_transform("Photon", x -> x.pt)) AS ph_pt, list_distinct(list_transform("Photon", x -> x.eta)) AS ph_eta
FROM rn_singleMu.run2012B_singleMu ORDER BY all DESC;
UPDATE singleMu SET jet_eta = list_resize(jet_eta, len(jet_pt));
UPDATE singleMu SET muon_eta = list_resize(muon_eta, len(muon_pt));
UPDATE singleMu SET ph_eta = list_resize(ph_eta, len(ph_pt));
UPDATE singleMu SET tau_eta = list_resize(tau_eta, len(tau_pt));