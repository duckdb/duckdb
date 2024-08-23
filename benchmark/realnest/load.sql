create table twitter as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/twitter-stream-2023-01/data.jsonl');
create table cord as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/cord-19-document_parses/data.jsonl');
create table open_street_map as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/daylight-openstreetmap-osm_features/data.jsonl');
create table run2012B_singleMu as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/hep-adl-ethz-Run2012B_SingleMu/data.jsonl');
create table gh_issue as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/gharchive-IssuesEvent/data.jsonl');
create table gh_pull as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/gharchive-PullRequestEvent/data.jsonl');

CREATE VIEW singleMu as SELECT list_transform("Tau", x -> x.pt) AS tau_pt, list_transform("Tau", x -> x.eta) AS tau_eta,
    list_transform("Jet", x -> x.pt) AS jet_pt, list_transform("Jet", x -> x.eta) AS jet_eta, 
    list_transform("Muon", x -> x.pt) AS muon_pt, list_transform("Muon", x -> x.eta) AS muon_eta, 
    list_transform("Electron", x -> x.pt) AS el_pt, list_transform("Electron", x -> x.eta) AS el_eta, 
    list_transform("Photon", x -> x.pt) AS ph_pt, list_transform("Photon", x -> x.eta) AS ph_eta
FROM run2012B_singleMu; 

CREATE VIEW unnested_hlt AS (SELECT rowid, UNNEST(HLT) AS hlt FROM run2012B_singleMu);
CREATE VIEW unnested_pv AS (SELECT rowid, UNNEST(PV) AS pv FROM run2012B_singleMu);
CREATE VIEW unnested_met AS (SELECT rowid, UNNEST(MET) AS met FROM run2012B_singleMu);
CREATE VIEW unnested_muon AS (SELECT rowid, UNNEST(Muon, recursive:=true) AS muon FROM run2012B_singleMu);
CREATE VIEW unnested_electron AS (SELECT rowid, UNNEST(Electron, recursive:=true) AS electron FROM run2012B_singleMu);
CREATE VIEW unnested_tau AS (SELECT rowid, UNNEST(Tau, recursive:=true) AS tau FROM run2012B_singleMu);
CREATE VIEW unnested_photon AS (SELECT rowid, UNNEST(Photon, recursive:=true) AS photon FROM run2012B_singleMu);
CREATE VIEW unnested_jet AS (SELECT rowid, UNNEST(Jet, recursive:=true) AS jet FROM run2012B_singleMu);