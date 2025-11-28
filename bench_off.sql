PRAGMA threads=1;

SET enable_column_imprint=false;

SELECT imprint_stats('reset');

-- for equality

SELECT 'OFF / fact_sorted / value = 5000000' AS case, COUNT(*) AS cnt
FROM fact_sorted
WHERE value = 5000000;

SELECT 'OFF / fact_random / value = 5000000' AS case, COUNT(*) AS cnt
FROM fact_random
WHERE value = 5000000;

-- for greater than 

-- SELECT 'OFF / fact_sorted / value > 9999900' AS case, COUNT(*) AS cnt
-- FROM fact_sorted
-- WHERE value > 9999900;

-- SELECT 'OFF / fact_random / value > 9999900' AS case, COUNT(*) AS cnt
-- FROM fact_random
-- WHERE value > 9999900;

-- SELECT 'OFF / fact_sorted / value > 5000000' AS case, COUNT(*) AS cnt
-- FROM fact_sorted
-- WHERE value > 5000000;

-- SELECT 'OFF / fact_random / value > 5000000' AS case, COUNT(*) AS cnt
-- FROM fact_random
-- WHERE value > 5000000;

-- EXPLAIN ANALYZE
-- SELECT COUNT(*) FROM fact_sorted WHERE value > 9999900;

SELECT 'imprint_checks_total' AS metric, imprint_stats('imprint_checks_total');
SELECT 'imprint_pruned_segments' AS metric, imprint_stats('imprint_pruned_segments');
SELECT 'imprint_equality_checks' AS metric, imprint_stats('imprint_equality_checks');
SELECT 'imprint_greater_than_checks' AS metric, imprint_stats('imprint_greater_than_checks');
SELECT 'total_segments_checked' AS metric, imprint_stats('total_segments_checked');
SELECT 'total_segments_skipped' AS metric, imprint_stats('total_segments_skipped');
