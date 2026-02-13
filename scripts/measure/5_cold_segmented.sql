-- 5 cold entry for each hot entry - segmented
-- Hash table size: ~60MiB, of which 14MiB is hot
-- Does not use perfect hashing


-- https://duckdb.org/docs/stable/dev/profiling
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = 'scripts/measure/5_cold_segmented.json';
PRAGMA profiling_coverage = 'SELECT';
-- PRAGMA profiling_mode = 'detailed';


-- https://duckdb.org/docs/stable/configuration/overview#:~:text=max_temp_directory_size
SET max_temp_directory_size='0KiB'; -- Forces no disk spill, I think?
SET threads = 4; 
SET disabled_optimizers = 'compressed_materialization';

-- Clean up
DROP TABLE IF EXISTS a; 
DROP TABLE IF EXISTS b; 

-- Create Fact Table A
-- Hits every hot key 1000 times
CREATE TABLE a AS 
SELECT 
    range AS id, 
    range % 400_000 AS keyB1
FROM range(0, 400_000_000)
UNION ALL
SELECT 999_999_999 AS id, 999_999_999 as keyB1; -- Have large min/max filter and disable perfect hashing

-- Create Dimension Table B
-- 400k hot entries in hashtable followed by 1.6M of cold entries  
-- Since the range is > 1M wide, perfect hashing is disabled
CREATE TABLE b AS
WITH base_data AS (
    SELECT range AS keyB1 FROM range(0, 2_000_000)
    UNION ALL
    SELECT 999_999_999 as keyB1 -- Have large min/max filter and disable perfect hashing
)
SELECT keyB1, (keyB1 < 400_000) as hot FROM base_data
ORDER BY hot, random();

-- Update statistics for the cost-based optimizer
ANALYZE a;
ANALYZE b;

-- EXPLAIN ANALYZE SELECT count(*) 
SELECT count(*) 
FROM a 
JOIN b ON a.keyB1 = b.keyB1;