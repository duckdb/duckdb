CREATE TABLE hep_singleMu AS SELECT * FROM READ_PARQUET('https://raw.githubusercontent.com/RumbleDB/iris-hep-benchmark-athena/refs/heads/master/data/Run2012B_SingleMu-restructured-1000.parquet');
