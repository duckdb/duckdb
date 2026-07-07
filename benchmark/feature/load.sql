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
CREATE TABLE hits_raw AS
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

-- Entity table: the distinct users. CREATE FEATURE requires the event table's
-- entity columns to form a FOREIGN KEY into the declared entity table, so we
-- populate the entities first and give hits a foreign key into them.
CREATE TABLE users (UserID BIGINT PRIMARY KEY);

INSERT INTO users SELECT DISTINCT UserID FROM hits_raw;

CREATE TABLE hits (
    UserID    BIGINT,
    EventTime TIMESTAMP,
    RegionID  INTEGER,
    CounterID INTEGER,
    FOREIGN KEY (UserID) REFERENCES users(UserID)
);

INSERT INTO hits SELECT UserID, EventTime, RegionID, CounterID FROM hits_raw;

DROP TABLE hits_raw;

-- Windowed aggregation per user. The window is wide enough that a snapshot AT the fixed refresh
-- time (below) captures the whole dataset, so REFRESH exercises the group-by aggregation path.
CREATE FEATURE user_activity ENTITY users TIMESTAMP EventTime
    WINDOW 3650 DAYS
    RETAIN 1
    AS (SELECT UserID, COUNT(*) AS event_count, AVG(RegionID) AS avg_region FROM hits GROUP BY UserID);

-- Same shape but RETAIN 5, used by the interleaved refresh/serve benchmark to exercise multi-version
-- time-travel serving. WATERMARK is accepted but inert.
CREATE FEATURE user_activity_retain5 ENTITY users TIMESTAMP EventTime
    WINDOW 3650 DAYS
    WATERMARK 1 HOUR
    RETAIN 5
    AS (SELECT UserID, COUNT(*) AS event_count FROM hits GROUP BY UserID);

-- CREATE FEATURE only registers metadata; the first REFRESH materializes version 1. Snapshot both
-- features AT a fixed time so the SERVE / REFRESH benchmarks (and the sanity assert in
-- feature.benchmark.in) have a materialized current version to read from.
REFRESH FEATURE user_activity AT '2016-01-01 00:00:00';

REFRESH FEATURE user_activity_retain5 AT '2016-01-01 00:00:00';

-- Serving spine: a sample of entities with a serving timestamp after the snapshot time, so every
-- spine row resolves to the current version. SERVE benchmarks consume it with SERVE FEATURE ... FOR.
-- Column names match the feature key and timestamp, so SERVE needs no overrides.
CREATE TABLE serve_requests AS
SELECT DISTINCT
    UserID,
    TIMESTAMP '2016-01-02 00:00:00' AS EventTime
FROM hits
USING SAMPLE 100000 ROWS;
