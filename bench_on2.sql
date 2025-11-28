PRAGMA threads=1;

PRAGMA build_column_imprints('fact_imprint_demo', 'v');

SET enable_column_imprint=true;

-- equality

-- v = 60000 exists in the table but imprint unable to prune
SELECT imprint_stats('reset');

SELECT 'ON / fact_imprint_demo / v = 60000' AS case,
       COUNT(*) AS cnt
FROM fact_imprint_demo
WHERE v = 60000;

SELECT 'imprint_checks_total'       AS metric, imprint_stats('imprint_checks_total');
SELECT 'imprint_pruned_segments'    AS metric, imprint_stats('imprint_pruned_segments');
SELECT 'imprint_equality_checks'    AS metric, imprint_stats('imprint_equality_checks');
SELECT 'imprint_greater_than_checks' AS metric, imprint_stats('imprint_greater_than_checks');
SELECT 'total_segments_checked'     AS metric, imprint_stats('total_segments_checked');
SELECT 'total_segments_skipped'     AS metric, imprint_stats('total_segments_skipped');

SELECT imprint_stats('reset');


--- v = 100000 doesn't exist in the table and imprint is still unable to prune bc it falls in the first bin

SELECT imprint_stats('reset');

SELECT 'ON / fact_imprint_demo / v = 100000' AS case,
       COUNT(*) AS cnt
FROM fact_imprint_demo
WHERE v = 100000;

SELECT 'imprint_checks_total'       AS metric, imprint_stats('imprint_checks_total');
SELECT 'imprint_pruned_segments'    AS metric, imprint_stats('imprint_pruned_segments');
SELECT 'imprint_equality_checks'    AS metric, imprint_stats('imprint_equality_checks');
SELECT 'imprint_greater_than_checks' AS metric, imprint_stats('imprint_greater_than_checks');
SELECT 'total_segments_checked'     AS metric, imprint_stats('total_segments_checked');
SELECT 'total_segments_skipped'     AS metric, imprint_stats('total_segments_skipped');


--- v = 50000000 doesnt exist in the table and imprint is able to prune

SELECT imprint_stats('reset');

SELECT 'ON / fact_imprint_demo / v = 50000000' AS case,
       COUNT(*) AS cnt
FROM fact_imprint_demo
WHERE v = 50000000;

SELECT 'imprint_checks_total'       AS metric, imprint_stats('imprint_checks_total');
SELECT 'imprint_pruned_segments'    AS metric, imprint_stats('imprint_pruned_segments');
SELECT 'imprint_equality_checks'    AS metric, imprint_stats('imprint_equality_checks');
SELECT 'imprint_greater_than_checks' AS metric, imprint_stats('imprint_greater_than_checks');
SELECT 'total_segments_checked'     AS metric, imprint_stats('total_segments_checked');
SELECT 'total_segments_skipped'     AS metric, imprint_stats('total_segments_skipped');




-- greater than
-- SELECT imprint_stats('reset');

-- SELECT 'ON / fact_imprint_demo / v > 60000000' AS case,
--        COUNT(*) AS cnt
-- FROM fact_imprint_demo
-- WHERE v > 60000000;

-- SELECT 'imprint_checks_total'       AS metric, imprint_stats('imprint_checks_total');
-- SELECT 'imprint_pruned_segments'    AS metric, imprint_stats('imprint_pruned_segments');
-- SELECT 'imprint_equality_checks'    AS metric, imprint_stats('imprint_equality_checks');
-- SELECT 'imprint_greater_than_checks' AS metric, imprint_stats('imprint_greater_than_checks');
-- SELECT 'total_segments_checked'     AS metric, imprint_stats('total_segments_checked');
-- SELECT 'total_segments_skipped'     AS metric, imprint_stats('total_segments_skipped');
