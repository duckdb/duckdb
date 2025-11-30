PRAGMA memory_limit='4GB';
SET threads = 4;

DROP TABLE IF EXISTS bench_int128;
DROP TABLE IF EXISTS bench_uint128;
DROP TABLE IF EXISTS bench_float;
DROP TABLE IF EXISTS bench_double;

CREATE TABLE bench_int128 AS
SELECT (i::HUGEINT) AS v
FROM range(1000000) t(i);

CREATE TABLE bench_uint128 AS
SELECT (i::UHUGEINT) AS v
FROM range(1000000) t(i);

CREATE TABLE bench_float AS
SELECT (i::FLOAT * 0.1) AS v
FROM range(1000000) t(i);

CREATE TABLE bench_double AS
SELECT (i::DOUBLE * 0.1) AS v
FROM range(1000000) t(i);

SELECT 'bench_int128', COUNT(*) FROM bench_int128;
SELECT 'bench_uint128', COUNT(*) FROM bench_uint128;
SELECT 'bench_float',   COUNT(*) FROM bench_float;
SELECT 'bench_double',  COUNT(*) FROM bench_double;
