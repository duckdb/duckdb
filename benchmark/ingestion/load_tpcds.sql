CALL dsdgen(sf = 1); EXPORT DATABASE 'duckdb_benchmark_data/tpcds_parquet'(FORMAT PARQUET); EXPORT DATABASE 'duckdb_benchmark_data/tpcds_csv'(FORMAT CSV);
CREATE VIEW call_center_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/call_center.csv');
CREATE VIEW call_center_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/call_center.parquet');
CREATE VIEW call_center_native AS
SELECT *
FROM call_center;
CREATE VIEW household_demographics_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/household_demographics.csv');
CREATE VIEW household_demographics_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/household_demographics.parquet');
CREATE VIEW household_demographics_native AS
SELECT *
FROM household_demographics;
CREATE VIEW store_returns_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/store_returns.csv');
CREATE VIEW store_returns_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/store_returns.parquet');
CREATE VIEW store_returns_native AS
SELECT *
FROM store_returns;
CREATE VIEW catalog_page_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/catalog_page.csv');
CREATE VIEW catalog_page_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/catalog_page.parquet');
CREATE VIEW catalog_page_native AS
SELECT *
FROM catalog_page;
CREATE VIEW income_band_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/income_band.csv');
CREATE VIEW income_band_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/income_band.parquet');
CREATE VIEW income_band_native AS
SELECT *
FROM income_band;
CREATE VIEW store_sales_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/store_sales.csv');
CREATE VIEW store_sales_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/store_sales.parquet');
CREATE VIEW store_sales_native AS
SELECT *
FROM store_sales;
CREATE VIEW catalog_returns_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/catalog_returns.csv');
CREATE VIEW catalog_returns_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/catalog_returns.parquet');
CREATE VIEW catalog_returns_native AS
SELECT *
FROM catalog_returns;
CREATE VIEW inventory_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/inventory.csv');
CREATE VIEW inventory_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/inventory.parquet');
CREATE VIEW inventory_native AS
SELECT *
FROM inventory;
CREATE VIEW time_dim_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/time_dim.csv');
CREATE VIEW time_dim_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/time_dim.parquet');
CREATE VIEW time_dim_native AS
SELECT *
FROM time_dim;
CREATE VIEW catalog_sales_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/catalog_sales.csv');
CREATE VIEW catalog_sales_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/catalog_sales.parquet');
CREATE VIEW catalog_sales_native AS
SELECT *
FROM catalog_sales;
CREATE VIEW item_csv AS
SELECT *
FROM read_csv('duckdb_benchmark_data/tpcds_csv/item.csv');
CREATE VIEW item_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/item.parquet');
CREATE VIEW item_native AS
SELECT *
FROM item;
CREATE VIEW warehouse_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/warehouse.csv');
CREATE VIEW warehouse_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/warehouse.parquet');
CREATE VIEW warehouse_native AS
SELECT *
FROM warehouse;
CREATE VIEW customer_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/customer.csv');
CREATE VIEW customer_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/customer.parquet');
CREATE VIEW customer_native AS
SELECT *
FROM customer;
CREATE VIEW promotion_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/promotion.csv');
CREATE VIEW promotion_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/promotion.parquet');
CREATE VIEW promotion_native AS
SELECT *
FROM promotion;
CREATE VIEW web_page_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/web_page.csv');
CREATE VIEW web_page_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/web_page.parquet');
CREATE VIEW web_page_native AS
SELECT *
FROM web_page;
CREATE VIEW customer_address_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/customer_address.csv');
CREATE VIEW customer_address_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/customer_address.parquet');
CREATE VIEW customer_address_native AS
SELECT *
FROM customer_address;
CREATE VIEW reason_csv AS
SELECT *
FROM read_csv('duckdb_benchmark_data/tpcds_csv/reason.csv');
CREATE VIEW reason_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/reason.parquet');
CREATE VIEW reason_native AS
SELECT *
FROM reason;
CREATE VIEW web_returns_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/web_returns.csv');
CREATE VIEW web_returns_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/web_returns.parquet');
CREATE VIEW web_returns_native AS
SELECT *
FROM web_returns;
CREATE VIEW customer_demographics_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/customer_demographics.csv');
CREATE VIEW customer_demographics_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/customer_demographics.parquet');
CREATE VIEW customer_demographics_native AS
SELECT *
FROM customer_demographics;
CREATE VIEW ship_mode_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/ship_mode.csv');
CREATE VIEW ship_mode_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/ship_mode.parquet');
CREATE VIEW ship_mode_native AS
SELECT *
FROM ship_mode;
CREATE VIEW web_sales_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/web_sales.csv');
CREATE VIEW web_sales_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/web_sales.parquet');
CREATE VIEW web_sales_native AS
SELECT *
FROM web_sales;
CREATE VIEW date_dim_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/date_dim.csv');
CREATE VIEW date_dim_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/date_dim.parquet');
CREATE VIEW date_dim_native AS
SELECT *
FROM date_dim;
CREATE VIEW store_csv AS
SELECT *
FROM read_csv('duckdb_benchmark_data/tpcds_csv/store.csv');
CREATE VIEW store_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/store.parquet');
CREATE VIEW store_native AS
SELECT *
FROM store;
CREATE VIEW web_site_csv AS
SELECT *
FROM
    read_csv('duckdb_benchmark_data/tpcds_csv/web_site.csv');
CREATE VIEW web_site_parquet AS
SELECT *
FROM
    read_parquet('duckdb_benchmark_data/tpcds_parquet/web_site.parquet');
CREATE VIEW web_site_native AS
SELECT *
FROM web_site;
