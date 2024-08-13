create table twitter as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/twitter-stream-2023-01/data.jsonl');
create table cord as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/cord-19-document_parses/data.jsonl');
create table open_street_map as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/daylight-openstreetmap-osm_features/data.jsonl');
create table run2012B_singleMu as select * from read_json('https://github.com/cwida/RealNest/raw/cdaa85652bf187226706b321c29597673b2b7d86/sample-data/100mib/hep-adl-ethz-Run2012B_SingleMu/data.jsonl')

CREATE VIEW unnested_hlt AS (SELECT rowid, UNNEST(HLT) AS hlt FROM Run2012B_SingleMu);
CREATE VIEW unnested_pv AS (SELECT rowid, UNNEST(PV) AS pv FROM Run2012B_SingleMu);
CREATE VIEW unnested_met AS (SELECT rowid, UNNEST(MET) AS met FROM Run2012B_SingleMu);
CREATE VIEW unnested_muon AS (SELECT rowid, UNNEST(Muon, recursive:=true) AS muon FROM Run2012B_SingleMu);
CREATE VIEW unnested_electron AS (SELECT rowid, UNNEST(Electron, recursive:=true) AS electron FROM Run2012B_SingleMu);
CREATE VIEW unnested_tau AS (SELECT rowid, UNNEST(Tau, recursive:=true) AS tau FROM Run2012B_SingleMu);
CREATE VIEW unnested_photon AS (SELECT rowid, UNNEST(Photon, recursive:=true) AS photon FROM Run2012B_SingleMu);
CREATE VIEW unnested_jet AS (SELECT rowid, UNNEST(Jet, recursive:=true) AS jet FROM Run2012B_SingleMu);