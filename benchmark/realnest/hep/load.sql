CREATE TABLE hep_singleMu AS SELECT * FROM READ_PARQUET('s3://duckdb-blobs/data/realnest/Run2012B_SingleMu_restructured_1000.parquet');
-- ATTACH 'https://duckdb-blobs.s3.amazonaws.com/data/realnest/singleMu_524k.duckdb' AS rn_singleMu (READ_ONLY);
-- CREATE TABLE hep_singleMu AS SELECT * FROM rn_singleMu.run2012B_singleMu;