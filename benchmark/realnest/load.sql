attach 'https://duckdb-blobs.s3.amazonaws.com/data/realnest/cord_10k.duckdb' as cord (READ_ONLY);
create table cord as select * from cord.cord;
attach 'https://duckdb-blobs.s3.amazonaws.com/data/realnest/open_street_map_524k.duckdb' as osm (READ_ONLY);
create table open_street_map as select * from osm.open_street_map;
attach 'https://duckdb-blobs.s3.amazonaws.com/data/realnest/pull_131k.duckdb' as gh_pull (READ_ONLY);
create table gh_pull as select * from gh_pull.gh_pull;
attach 'https://duckdb-blobs.s3.amazonaws.com/data/realnest/issue_131k.duckdb' as gh_issue (READ_ONLY);
create table gh_issue as select * from gh_issue.gh_issue;
attach 'https://duckdb-blobs.s3.amazonaws.com/data/realnest/twitter_131k.duckdb' as tw (READ_ONLY);
create table twitter as select * from tw.twitter;
attach 'https://duckdb-blobs.s3.amazonaws.com/data/realnest/singleMu_524k.duckdb' as rn_singleMu (READ_ONLY);
create table run2012B_singleMu as select * from rn_singleMu.run2012B_singleMu;
CREATE TABLE single_mu_lists AS SELECT * REPLACE(
    list_resize(Jet, 10, NULL) as Jet, list_resize(Muon, 10, NULL) as Muon, 
    list_resize(Photon, 10, NULL) as Photon, list_resize(Tau, 10, NULL) as Tau) 
FROM rn_singleMu.run2012B_singleMu;
CREATE or replace TABLE singleMu as 
SELECT 
    list_distinct(list_transform("Tau", x -> x.pt)) AS tau_pt, list_distinct(list_transform("Tau", x -> x.eta)) AS tau_eta,
    list_distinct(list_transform("Jet", x -> x.pt)) AS jet_pt, list_distinct(list_transform("Jet", x -> x.eta)) AS jet_eta, 
    list_distinct(list_transform("Muon", x -> x.pt)) AS muon_pt, list_distinct(list_transform("Muon", x -> x.eta)) AS muon_eta, 
    list_distinct(list_transform("Photon", x -> x.pt)) AS ph_pt, list_distinct(list_transform("Photon", x -> x.eta)) AS ph_eta
FROM rn_singleMu.run2012B_singleMu order by all desc;
UPDATE singleMu set jet_eta = list_resize(jet_eta, len(jet_pt));
UPDATE singleMu set muon_eta = list_resize(muon_eta, len(muon_pt));
UPDATE singleMu set ph_eta = list_resize(ph_eta, len(ph_pt));
UPDATE singleMu set tau_eta = list_resize(tau_eta, len(tau_pt));