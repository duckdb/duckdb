PRAGMA threads=1;

DROP TABLE IF EXISTS fact_seq;
DROP TABLE IF EXISTS fact_sorted;
DROP TABLE IF EXISTS fact_random;

CREATE TABLE fact_seq AS
SELECT i::BIGINT AS value
FROM range(10000000) t(i);

CREATE TABLE fact_sorted AS
SELECT * FROM fact_seq
ORDER BY value;

CREATE TABLE fact_random AS
SELECT * FROM fact_seq
ORDER BY random();
