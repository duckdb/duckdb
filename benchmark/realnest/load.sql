create table twitter as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/twitter-stream-2023-01/data.jsonl');
create table cord as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/cord-19-document_parses/data.jsonl');
create table open_street_map as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/daylight-openstreetmap-osm_features/data.jsonl');
create table run2012B_singleMu as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/hep-adl-ethz-Run2012B_SingleMu/data.jsonl');
create table gh_issue as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/gharchive-IssuesEvent/data.jsonl');
create table gh_pull as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/gharchive-PullRequestEvent/data.jsonl');

CREATE TABLE single_mu_lists AS SELECT * REPLACE(list_resize(Jet, 10, NULL) as Jet, list_resize(Muon, 10, NULL) as Muon, list_resize(Photon, 10, NULL) as Photon, list_resize(Tau, 10, NULL) as Tau) 
FROM run2012B_singleMu;
CREATE TABLE singleMu as SELECT list_transform("Tau", x -> x.pt) AS tau_pt, list_transform("Tau", x -> x.eta) AS tau_eta,
    list_transform("Jet", x -> x.pt) AS jet_pt, list_transform("Jet", x -> x.eta) AS jet_eta, 
    list_transform("Muon", x -> x.pt) AS muon_pt, list_transform("Muon", x -> x.eta) AS muon_eta, 
    list_transform("Electron", x -> x.pt) AS el_pt, list_transform("Electron", x -> x.eta) AS el_eta, 
    list_transform("Photon", x -> x.pt) AS ph_pt, list_transform("Photon", x -> x.eta) AS ph_eta
FROM run2012B_singleMu; 
