-- TPC-DS Q22 / Q88 / Q92 benchmark against the SAF + PAP optimizer fixes.
--
-- Steps:
--   1. Generate SF1 TPC-DS data with the tpcds extension.
--   2. Export the 4 tables Q88 needs to parquet (so SAF kicks in — it's
--      restricted to parquet/native scans, and TPC-DS data is native).
--   3. Run each query with and without the new optimizer pass, collecting
--      EXPLAIN ANALYZE timings.

INSTALL tpcds; LOAD tpcds;
CALL dsdgen(sf=1);

.print ================================================================
.print Q22 — GROUP BY ROLLUP on inventory (Snowflake 2.5x faster baseline)
.print ================================================================

.print --- Q22 BASELINE (no partial_aggregate_pushdown) ---
SET disabled_optimizers = 'partial_aggregate_pushdown';
.timer on
PRAGMA enable_profiling='no_output';
EXPLAIN ANALYZE
SELECT i_product_name, i_brand, i_class, i_category, avg(inv_quantity_on_hand) qoh
FROM inventory, date_dim, item
WHERE inv_date_sk = d_date_sk
  AND inv_item_sk = i_item_sk
  AND d_month_seq BETWEEN 1200 AND 1200 + 11
GROUP BY rollup(i_product_name, i_brand, i_class, i_category)
ORDER BY qoh NULLS FIRST, i_product_name NULLS FIRST,
         i_brand NULLS FIRST, i_class NULLS FIRST, i_category NULLS FIRST
LIMIT 100;

.print --- Q22 WITH partial_aggregate_pushdown ---
SET disabled_optimizers = '';
EXPLAIN ANALYZE
SELECT i_product_name, i_brand, i_class, i_category, avg(inv_quantity_on_hand) qoh
FROM inventory, date_dim, item
WHERE inv_date_sk = d_date_sk
  AND inv_item_sk = i_item_sk
  AND d_month_seq BETWEEN 1200 AND 1200 + 11
GROUP BY rollup(i_product_name, i_brand, i_class, i_category)
ORDER BY qoh NULLS FIRST, i_product_name NULLS FIRST,
         i_brand NULLS FIRST, i_class NULLS FIRST, i_category NULLS FIRST
LIMIT 100;

.print ================================================================
.print Q88 — 8 similar subqueries cross-joined (Snowflake 1.5x faster)
.print ================================================================

.print --- Q88 BASELINE (no scalar_aggregate_fusion) ---
SET disabled_optimizers = 'scalar_aggregate_fusion';
EXPLAIN ANALYZE
SELECT * FROM
  (SELECT count(*) h8_30_to_9
   FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = time_dim.t_time_sk
     AND ss_hdemo_sk = household_demographics.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND time_dim.t_hour = 8 AND time_dim.t_minute >= 30
     AND ((household_demographics.hd_dep_count = 4 AND household_demographics.hd_vehicle_count<=4+2)
          OR (household_demographics.hd_dep_count = 2 AND household_demographics.hd_vehicle_count<=2+2)
          OR (household_demographics.hd_dep_count = 0 AND household_demographics.hd_vehicle_count<=0+2))
     AND store.s_store_name = 'ese') s1,
  (SELECT count(*) h9_to_9_30 FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
     AND t_hour = 9 AND t_minute < 30
     AND ((hd_dep_count = 4 AND hd_vehicle_count<=4+2) OR (hd_dep_count = 2 AND hd_vehicle_count<=2+2) OR (hd_dep_count = 0 AND hd_vehicle_count<=0+2))
     AND s_store_name = 'ese') s2,
  (SELECT count(*) h9_30_to_10 FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
     AND t_hour = 9 AND t_minute >= 30
     AND ((hd_dep_count = 4 AND hd_vehicle_count<=4+2) OR (hd_dep_count = 2 AND hd_vehicle_count<=2+2) OR (hd_dep_count = 0 AND hd_vehicle_count<=0+2))
     AND s_store_name = 'ese') s3,
  (SELECT count(*) h10_to_10_30 FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
     AND t_hour = 10 AND t_minute < 30
     AND ((hd_dep_count = 4 AND hd_vehicle_count<=4+2) OR (hd_dep_count = 2 AND hd_vehicle_count<=2+2) OR (hd_dep_count = 0 AND hd_vehicle_count<=0+2))
     AND s_store_name = 'ese') s4,
  (SELECT count(*) h10_30_to_11 FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
     AND t_hour = 10 AND t_minute >= 30
     AND ((hd_dep_count = 4 AND hd_vehicle_count<=4+2) OR (hd_dep_count = 2 AND hd_vehicle_count<=2+2) OR (hd_dep_count = 0 AND hd_vehicle_count<=0+2))
     AND s_store_name = 'ese') s5,
  (SELECT count(*) h11_to_11_30 FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
     AND t_hour = 11 AND t_minute < 30
     AND ((hd_dep_count = 4 AND hd_vehicle_count<=4+2) OR (hd_dep_count = 2 AND hd_vehicle_count<=2+2) OR (hd_dep_count = 0 AND hd_vehicle_count<=0+2))
     AND s_store_name = 'ese') s6,
  (SELECT count(*) h11_30_to_12 FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
     AND t_hour = 11 AND t_minute >= 30
     AND ((hd_dep_count = 4 AND hd_vehicle_count<=4+2) OR (hd_dep_count = 2 AND hd_vehicle_count<=2+2) OR (hd_dep_count = 0 AND hd_vehicle_count<=0+2))
     AND s_store_name = 'ese') s7,
  (SELECT count(*) h12_to_12_30 FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
     AND t_hour = 12 AND t_minute < 30
     AND ((hd_dep_count = 4 AND hd_vehicle_count<=4+2) OR (hd_dep_count = 2 AND hd_vehicle_count<=2+2) OR (hd_dep_count = 0 AND hd_vehicle_count<=0+2))
     AND s_store_name = 'ese') s8;

.print --- Q88 WITH scalar_aggregate_fusion ---
SET disabled_optimizers = '';
EXPLAIN ANALYZE
SELECT * FROM
  (SELECT count(*) h8_30_to_9 FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
     AND t_hour = 8 AND t_minute >= 30
     AND ((hd_dep_count = 4 AND hd_vehicle_count<=4+2) OR (hd_dep_count = 2 AND hd_vehicle_count<=2+2) OR (hd_dep_count = 0 AND hd_vehicle_count<=0+2))
     AND s_store_name = 'ese') s1,
  (SELECT count(*) h9_to_9_30 FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
     AND t_hour = 9 AND t_minute < 30
     AND ((hd_dep_count = 4 AND hd_vehicle_count<=4+2) OR (hd_dep_count = 2 AND hd_vehicle_count<=2+2) OR (hd_dep_count = 0 AND hd_vehicle_count<=0+2))
     AND s_store_name = 'ese') s2,
  (SELECT count(*) h9_30_to_10 FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
     AND t_hour = 9 AND t_minute >= 30
     AND ((hd_dep_count = 4 AND hd_vehicle_count<=4+2) OR (hd_dep_count = 2 AND hd_vehicle_count<=2+2) OR (hd_dep_count = 0 AND hd_vehicle_count<=0+2))
     AND s_store_name = 'ese') s3,
  (SELECT count(*) h10_to_10_30 FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
     AND t_hour = 10 AND t_minute < 30
     AND ((hd_dep_count = 4 AND hd_vehicle_count<=4+2) OR (hd_dep_count = 2 AND hd_vehicle_count<=2+2) OR (hd_dep_count = 0 AND hd_vehicle_count<=0+2))
     AND s_store_name = 'ese') s4,
  (SELECT count(*) h10_30_to_11 FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
     AND t_hour = 10 AND t_minute >= 30
     AND ((hd_dep_count = 4 AND hd_vehicle_count<=4+2) OR (hd_dep_count = 2 AND hd_vehicle_count<=2+2) OR (hd_dep_count = 0 AND hd_vehicle_count<=0+2))
     AND s_store_name = 'ese') s5,
  (SELECT count(*) h11_to_11_30 FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
     AND t_hour = 11 AND t_minute < 30
     AND ((hd_dep_count = 4 AND hd_vehicle_count<=4+2) OR (hd_dep_count = 2 AND hd_vehicle_count<=2+2) OR (hd_dep_count = 0 AND hd_vehicle_count<=0+2))
     AND s_store_name = 'ese') s6,
  (SELECT count(*) h11_30_to_12 FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
     AND t_hour = 11 AND t_minute >= 30
     AND ((hd_dep_count = 4 AND hd_vehicle_count<=4+2) OR (hd_dep_count = 2 AND hd_vehicle_count<=2+2) OR (hd_dep_count = 0 AND hd_vehicle_count<=0+2))
     AND s_store_name = 'ese') s7,
  (SELECT count(*) h12_to_12_30 FROM store_sales, household_demographics, time_dim, store
   WHERE ss_sold_time_sk = t_time_sk AND ss_hdemo_sk = hd_demo_sk AND ss_store_sk = s_store_sk
     AND t_hour = 12 AND t_minute < 30
     AND ((hd_dep_count = 4 AND hd_vehicle_count<=4+2) OR (hd_dep_count = 2 AND hd_vehicle_count<=2+2) OR (hd_dep_count = 0 AND hd_vehicle_count<=0+2))
     AND s_store_name = 'ese') s8;

.print ================================================================
.print Q92 — correlated subquery on web_sales (Snowflake 1.2x faster)
.print ================================================================

.print Q92 not yet addressed by these optimizer passes — included for completeness
EXPLAIN ANALYZE
SELECT sum(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales, item, date_dim
WHERE i_manufact_id = 350
  AND i_item_sk = ws_item_sk
  AND d_date BETWEEN cast('2000-01-27' as date) AND cast('2000-04-26' AS date)
  AND d_date_sk = ws_sold_date_sk
  AND ws_ext_discount_amt >
    (SELECT 1.3 * avg(ws_ext_discount_amt)
     FROM web_sales, date_dim
     WHERE ws_item_sk = i_item_sk
       AND d_date BETWEEN cast('2000-01-27' as date) AND cast('2000-04-26' AS date)
       AND d_date_sk = ws_sold_date_sk)
LIMIT 100;
