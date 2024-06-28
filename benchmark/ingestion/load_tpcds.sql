CALL dsdgen(sf=1);
EXPORT DATABASE 'duckdb_benchmark_data/tpcds_parquet' (FORMAT PARQUET);
EXPORT DATABASE 'duckdb_benchmark_data/tpcds_csv' (FORMAT CSV);

create view call_center_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/call_center.csv');
create view call_center_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/call_center.parquet');
create view call_center_native as select * from call_center;

create view household_demographics_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/household_demographics.csv');
create view household_demographics_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/household_demographics.parquet');
create view household_demographics_native as select * from household_demographics;

create view store_returns_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/store_returns.csv');
create view store_returns_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/store_returns.parquet');
create view store_returns_native as select * from store_returns;

create view catalog_page_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/catalog_page.csv');
create view catalog_page_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/catalog_page.parquet');
create view catalog_page_native as select * from catalog_page;

create view income_band_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/income_band.csv');
create view income_band_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/income_band.parquet');
create view income_band_native as select * from income_band;

create view store_sales_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/store_sales.csv');
create view store_sales_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/store_sales.parquet');
create view store_sales_native as select * from store_sales;

create view catalog_returns_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/catalog_returns.csv');
create view catalog_returns_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/catalog_returns.parquet');
create view catalog_returns_native as select * from catalog_returns;

create view inventory_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/inventory.csv');
create view inventory_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/inventory.parquet');
create view inventory_native as select * from inventory;

create view time_dim_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/time_dim.csv');
create view time_dim_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/time_dim.parquet');
create view time_dim_native as select * from time_dim;

create view catalog_sales_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/catalog_sales.csv');
create view catalog_sales_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/catalog_sales.parquet');
create view catalog_sales_native as select * from catalog_sales;

create view item_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/item.csv');
create view item_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/item.parquet');
create view item_native as select * from item;

create view warehouse_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/warehouse.csv');
create view warehouse_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/warehouse.parquet');
create view warehouse_native as select * from warehouse;

create view customer_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/customer.csv');
create view customer_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/customer.parquet');
create view customer_native as select * from customer;

create view promotion_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/promotion.csv');
create view promotion_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/promotion.parquet');
create view promotion_native as select * from promotion;

create view web_page_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/web_page.csv');
create view web_page_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/web_page.parquet');
create view web_page_native as select * from web_page;

create view customer_address_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/customer_address.csv');
create view customer_address_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/customer_address.parquet');
create view customer_address_native as select * from customer_address;

create view reason_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/reason.csv');
create view reason_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/reason.parquet');
create view reason_native as select * from reason;

create view web_returns_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/web_returns.csv');
create view web_returns_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/web_returns.parquet');
create view web_returns_native as select * from web_returns;

create view customer_demographics_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/customer_demographics.csv');
create view customer_demographics_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/customer_demographics.parquet');
create view customer_demographics_native as select * from customer_demographics;

create view ship_mode_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/ship_mode.csv');
create view ship_mode_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/ship_mode.parquet');
create view ship_mode_native as select * from ship_mode;

create view web_sales_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/web_sales.csv');
create view web_sales_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/web_sales.parquet');
create view web_sales_native as select * from web_sales;

create view date_dim_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/date_dim.csv');
create view date_dim_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/date_dim.parquet');
create view date_dim_native as select * from date_dim;

create view store_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/store.csv');
create view store_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/store.parquet');
create view store_native as select * from store;

create view web_site_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv/web_site.csv');
create view web_site_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet/web_site.parquet');
create view web_site_native as select * from web_site;
