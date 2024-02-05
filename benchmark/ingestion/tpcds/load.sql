CALL dsdgen(sf=5);
EXPORT DATABASE 'duckdb_benchmark_data/tpcds_parquet_sf5' (FORMAT PARQUET);
EXPORT DATABASE 'duckdb_benchmark_data/tpcds_csv_sf5' (FORMAT CSV);

create view call_center_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/call_center.csv');
create view call_center_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/call_center.parquet');
create view call_center_native as select * from call_center;

create view household_demographics_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/household_demographics.csv');
create view household_demographics_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/household_demographics.parquet');
create view household_demographics_native as select * from household_demographics;

create view store_returns_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/store_returns.csv');
create view store_returns_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/store_returns.parquet');
create view store_returns_native as select * from store_returns;

create view catalog_page_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/catalog_page.csv');
create view catalog_page_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/catalog_page.parquet');
create view catalog_page_native as select * from catalog_page;

create view income_band_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/income_band.csv');
create view income_band_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/income_band.parquet');
create view income_band_native as select * from income_band;

create view store_sales_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/store_sales.csv');
create view store_sales_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/store_sales.parquet');
create view store_sales_native as select * from store_sales;

create view catalog_returns_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/catalog_returns.csv');
create view catalog_returns_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/catalog_returns.parquet');
create view catalog_returns_native as select * from catalog_returns;

create view inventory_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/inventory.csv');
create view inventory_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/inventory.parquet');
create view inventory_native as select * from inventory;

create view time_dim_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/time_dim.csv');
create view time_dim_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/time_dim.parquet');
create view time_dim_native as select * from time_dim;

create view catalog_sales_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/catalog_sales.csv');
create view catalog_sales_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/catalog_sales.parquet');
create view catalog_sales_native as select * from catalog_sales;

create view item_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/item.csv');
create view item_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/item.parquet');
create view item_native as select * from item;

create view warehouse_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/warehouse.csv');
create view warehouse_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/warehouse.parquet');
create view warehouse_native as select * from warehouse;

create view customer_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/customer.csv');
create view customer_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/customer.parquet');
create view customer_native as select * from customer;

create view promotion_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/promotion.csv');
create view promotion_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/promotion.parquet');
create view promotion_native as select * from promotion;

create view web_page_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/web_page.csv');
create view web_page_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/web_page.parquet');
create view web_page_native as select * from web_page;

create view customer_address_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/customer_address.csv');
create view customer_address_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/customer_address.parquet');
create view customer_address_native as select * from customer_address;

create view reason_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/reason.csv');
create view reason_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/reason.parquet');
create view reason  _native as select * from reason;

create view web_returns_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/web_returns.csv');
create view web_returns_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/web_returns.parquet');
create view web_returns_native as select * from web_returns;

create view customer_demographics_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/customer_demographics.csv');
create view customer_demographics_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/customer_demographics.parquet');
create view customer_demographics_native as select * from customer_demographics;

create view ship_mode_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/ship_mode.csv');
create view ship_mode_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/ship_mode.parquet');
create view ship_mode_native as select * from ship_mode;

create view web_sales_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/web_sales.csv');
create view web_sales_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/web_sales.parquet');
create view web_sales_native as select * from web_sales;

create view date_dim_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/date_dim.csv');
create view date_dim_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/date_dim.parquet');
create view date_dim_native as select * from date_dim;

create view store_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/store.csv');
create view store_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/store.parquet');
create view store_native as select * from store;

create view web_site_csv as select * from read_csv('duckdb_benchmark_data/tpcds_csv_sf5/web_site.csv');
create view web_site_parquet as select * from read_parquet('duckdb_benchmark_data/tpcds_parquet_sf5/web_site.parquet');
create view web_site_native as select * from web_site;