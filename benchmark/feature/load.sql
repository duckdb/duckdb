-- Feature benchmark source data: a lightweight projection of the ClickBench
-- (Yandex Metrica) "hits" dataset. Only the columns needed for the feature
-- definitions are read from the Parquet files, so the download stays small
-- thanks to Parquet projection pushdown.
--
--   Entity:    UserID    (high cardinality, millions of distinct users)
--   Timestamp: EventTime
--
-- Reduce the range(0, 100) below to download fewer Parquet files for a
-- smaller, faster benchmark.
CREATE TABLE hits AS
SELECT
    CAST(UserID AS BIGINT)     AS UserID,
    epoch_ms(EventTime * 1000) AS EventTime,
    CAST(RegionID AS INTEGER)  AS RegionID,
    CAST(CounterID AS INTEGER) AS CounterID
FROM read_parquet(
    [format('https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{}.parquet', x)
     for x in range(0, 100)],
    binary_as_string = True
);

-- FULL-refresh feature: hourly windowed aggregation per user over a 24h window.
-- Exercises the CREATE FEATURE / REFRESH FULL group-by aggregation path.
CREATE FEATURE user_activity_full ON hits
    ENTITY UserID
    TIMESTAMP EventTime
    GRANULARITY HOUR
    WINDOW 24
    REFRESH FULL
    RETAIN 1
    AS (SELECT UserID, COUNT(*) AS event_count, AVG(RegionID) AS avg_region);

-- INCREMENTAL-refresh feature: same shape, watermark-based incremental refresh.
-- A refresh always recomputes the tail from the last floor bucket onward, so it
-- exercises the REFRESH INCREMENTAL path even without new source rows.
CREATE FEATURE user_activity_incr ON hits
    ENTITY UserID
    TIMESTAMP EventTime
    GRANULARITY HOUR
    WINDOW 24
    REFRESH INCREMENTAL
    RETAIN 5
    AS (SELECT UserID, COUNT(*) AS event_count);

-- Serving spine: a sample of entities with a serving timestamp after all events.
-- Column names match the feature's ENTITY/TIMESTAMP, so SERVE needs no
-- ENTITY/ASOF overrides. Exercises the point-in-time LEFT ASOF JOIN serving path.
CREATE TABLE serve_requests AS
SELECT DISTINCT
    UserID,
    TIMESTAMP '2015-01-01 00:00:00' AS EventTime
FROM hits
USING SAMPLE 100000 ROWS;
