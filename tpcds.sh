#!/usr/bin/env bash

DB_NAME="tpcds.db"

SF=${SF:-100} # Scale Factor, default to 100 if not set
LOOPS=${LOOPS:-1} # Number of loops, default to 1 if not set

OUTPUT_FILE="query_benchmark_results.txt"
TIMING_FILE="query_timing.csv"
COMPARISON_FILE="performance_comparison.csv"

# Arrays to store timing results for each query
declare -A ON_DURATIONS_ROUND1
declare -A OFF_DURATIONS_ROUND1
declare -A ON_DURATIONS_ROUND2
declare -A OFF_DURATIONS_ROUND2

# Create output directory
RESULTS_DIR="tpcds_benchmark_results"
# if this directory exists, delete it, to avoid appending to the existing file
if [ -d "$RESULTS_DIR" ]; then
    rm -rf "$RESULTS_DIR"
fi
mkdir -p "$RESULTS_DIR"

# Clear output files
> "$RESULTS_DIR/$OUTPUT_FILE"
> "$RESULTS_DIR/$TIMING_FILE"
> "$RESULTS_DIR/$COMPARISON_FILE"

# Write CSV headers
echo "query_file,test_round,column_imprint_setting,start_time,end_time,duration_ns,status" > "$RESULTS_DIR/$TIMING_FILE"
echo "query_file,off_duration_ns,on_duration_ns,performance_improvement_ns,improvement_percent,off_order,on_order" > "$RESULTS_DIR/$COMPARISON_FILE"

echo "Starting TPC-DS Benchmark with Scale Factor: $SF" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

# Clean up table
rm ${DB_NAME}
#./build/debug/duckdb tpch.db "DROP TABLE IF EXISTS customer;
# DROP TABLE IF EXISTS lineitem;
# DROP TABLE IF EXISTS nation;
# DROP TABLE IF EXISTS orders;
# DROP TABLE IF EXISTS part;
# DROP TABLE IF EXISTS partsupp;
# DROP TABLE IF EXISTS region;
# DROP TABLE IF EXISTS supplier;"

# Generate TPC-DS data
echo "Generating TPC-DS data" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
./build/release/duckdb ${DB_NAME} "CALL dsdgen(sf = ${SF});"

# Build column imprint indexes
echo "Building Column Imprint Indexes" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

START_TIME=$(date +%s%N)
START_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')

./build/release/duckdb ${DB_NAME} -c ".timer on" \
    -c "PRAGMA build_column_imprints('call_center', 'cc_call_center_sk');" \
    -c "PRAGMA build_column_imprints('call_center', 'cc_rec_start_date');" \
    -c "PRAGMA build_column_imprints('call_center', 'cc_rec_end_date');" \
    -c "PRAGMA build_column_imprints('call_center', 'cc_closed_date_sk');" \
    -c "PRAGMA build_column_imprints('call_center', 'cc_open_date_sk');" \
    -c "PRAGMA build_column_imprints('call_center', 'cc_employees');" \
    -c "PRAGMA build_column_imprints('call_center', 'cc_sq_ft');" \
    -c "PRAGMA build_column_imprints('call_center', 'cc_mkt_id');" \
    -c "PRAGMA build_column_imprints('call_center', 'cc_division');" \
    -c "PRAGMA build_column_imprints('call_center', 'cc_company');" \
    -c "PRAGMA build_column_imprints('call_center', 'cc_gmt_offset');" \
    -c "PRAGMA build_column_imprints('call_center', 'cc_tax_percentage');" \
    -c "PRAGMA build_column_imprints('catalog_page', 'cp_catalog_page_sk');" \
    -c "PRAGMA build_column_imprints('catalog_page', 'cp_start_date_sk');" \
    -c "PRAGMA build_column_imprints('catalog_page', 'cp_end_date_sk');" \
    -c "PRAGMA build_column_imprints('catalog_page', 'cp_catalog_number');" \
    -c "PRAGMA build_column_imprints('catalog_page', 'cp_catalog_page_number');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_returned_date_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_returned_time_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_item_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_refunded_customer_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_refunded_cdemo_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_refunded_hdemo_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_refunded_addr_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_returning_customer_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_returning_cdemo_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_returning_hdemo_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_returning_addr_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_call_center_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_catalog_page_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_ship_mode_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_warehouse_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_reason_sk');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_order_number');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_return_quantity');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_return_amount');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_return_tax');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_return_amt_inc_tax');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_fee');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_return_ship_cost');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_refunded_cash');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_reversed_charge');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_store_credit');" \
    -c "PRAGMA build_column_imprints('catalog_returns', 'cr_net_loss');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_sold_date_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_sold_time_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_ship_date_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_bill_customer_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_bill_cdemo_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_bill_hdemo_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_bill_addr_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_ship_customer_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_ship_cdemo_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_ship_hdemo_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_ship_addr_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_call_center_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_catalog_page_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_ship_mode_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_warehouse_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_item_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_promo_sk');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_order_number');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_quantity');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_wholesale_cost');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_list_price');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_sales_price');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_ext_discount_amt');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_ext_sales_price');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_ext_wholesale_cost');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_ext_list_price');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_ext_tax');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_coupon_amt');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_ext_ship_cost');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_net_paid');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_net_paid_inc_tax');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_net_paid_inc_ship');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_net_paid_inc_ship_tax');" \
    -c "PRAGMA build_column_imprints('catalog_sales', 'cs_net_profit');" \
    -c "PRAGMA build_column_imprints('customer', 'c_customer_sk');" \
    -c "PRAGMA build_column_imprints('customer', 'c_current_cdemo_sk');" \
    -c "PRAGMA build_column_imprints('customer', 'c_current_hdemo_sk');" \
    -c "PRAGMA build_column_imprints('customer', 'c_current_addr_sk');" \
    -c "PRAGMA build_column_imprints('customer', 'c_first_shipto_date_sk');" \
    -c "PRAGMA build_column_imprints('customer', 'c_first_sales_date_sk');" \
    -c "PRAGMA build_column_imprints('customer', 'c_birth_day');" \
    -c "PRAGMA build_column_imprints('customer', 'c_birth_month');" \
    -c "PRAGMA build_column_imprints('customer', 'c_birth_year');" \
    -c "PRAGMA build_column_imprints('customer', 'c_last_review_date_sk');" \
    -c "PRAGMA build_column_imprints('customer_address', 'ca_address_sk');" \
    -c "PRAGMA build_column_imprints('customer_address', 'ca_gmt_offset');" \
    -c "PRAGMA build_column_imprints('customer_demographics', 'cd_demo_sk');" \
    -c "PRAGMA build_column_imprints('customer_demographics', 'cd_purchase_estimate');" \
    -c "PRAGMA build_column_imprints('customer_demographics', 'cd_dep_count');" \
    -c "PRAGMA build_column_imprints('customer_demographics', 'cd_dep_employed_count');" \
    -c "PRAGMA build_column_imprints('customer_demographics', 'cd_dep_college_count');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_date_sk');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_date');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_month_seq');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_week_seq');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_quarter_seq');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_year');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_dow');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_moy');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_dom');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_qoy');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_fy_year');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_fy_quarter_seq');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_fy_week_seq');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_first_dom');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_last_dom');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_same_day_ly');" \
    -c "PRAGMA build_column_imprints('date_dim', 'd_same_day_lq');" \
    -c "PRAGMA build_column_imprints('household_demographics', 'hd_demo_sk');" \
    -c "PRAGMA build_column_imprints('household_demographics', 'hd_income_band_sk');" \
    -c "PRAGMA build_column_imprints('household_demographics', 'hd_dep_count');" \
    -c "PRAGMA build_column_imprints('household_demographics', 'hd_vehicle_count');" \
    -c "PRAGMA build_column_imprints('income_band', 'ib_income_band_sk');" \
    -c "PRAGMA build_column_imprints('income_band', 'ib_lower_bound');" \
    -c "PRAGMA build_column_imprints('income_band', 'ib_upper_bound');" \
    -c "PRAGMA build_column_imprints('inventory', 'inv_date_sk');" \
    -c "PRAGMA build_column_imprints('inventory', 'inv_item_sk');" \
    -c "PRAGMA build_column_imprints('inventory', 'inv_warehouse_sk');" \
    -c "PRAGMA build_column_imprints('inventory', 'inv_quantity_on_hand');" \
    -c "PRAGMA build_column_imprints('item', 'i_item_sk');" \
    -c "PRAGMA build_column_imprints('item', 'i_rec_start_date');" \
    -c "PRAGMA build_column_imprints('item', 'i_rec_end_date');" \
    -c "PRAGMA build_column_imprints('item', 'i_current_price');" \
    -c "PRAGMA build_column_imprints('item', 'i_wholesale_cost');" \
    -c "PRAGMA build_column_imprints('item', 'i_brand_id');" \
    -c "PRAGMA build_column_imprints('item', 'i_class_id');" \
    -c "PRAGMA build_column_imprints('item', 'i_category_id');" \
    -c "PRAGMA build_column_imprints('item', 'i_manufact_id');" \
    -c "PRAGMA build_column_imprints('item', 'i_manager_id');" \
    -c "PRAGMA build_column_imprints('promotion', 'p_promo_sk');" \
    -c "PRAGMA build_column_imprints('promotion', 'p_start_date_sk');" \
    -c "PRAGMA build_column_imprints('promotion', 'p_end_date_sk');" \
    -c "PRAGMA build_column_imprints('promotion', 'p_item_sk');" \
    -c "PRAGMA build_column_imprints('promotion', 'p_cost');" \
    -c "PRAGMA build_column_imprints('promotion', 'p_response_target');" \
    -c "PRAGMA build_column_imprints('reason', 'r_reason_sk');" \
    -c "PRAGMA build_column_imprints('ship_mode', 'sm_ship_mode_sk');" \
    -c "PRAGMA build_column_imprints('store', 's_store_sk');" \
    -c "PRAGMA build_column_imprints('store', 's_rec_start_date');" \
    -c "PRAGMA build_column_imprints('store', 's_rec_end_date');" \
    -c "PRAGMA build_column_imprints('store', 's_closed_date_sk');" \
    -c "PRAGMA build_column_imprints('store', 's_number_employees');" \
    -c "PRAGMA build_column_imprints('store', 's_floor_space');" \
    -c "PRAGMA build_column_imprints('store', 's_market_id');" \
    -c "PRAGMA build_column_imprints('store', 's_division_id');" \
    -c "PRAGMA build_column_imprints('store', 's_company_id');" \
    -c "PRAGMA build_column_imprints('store', 's_gmt_offset');" \
    -c "PRAGMA build_column_imprints('store', 's_tax_percentage');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_returned_date_sk');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_return_time_sk');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_item_sk');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_customer_sk');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_cdemo_sk');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_hdemo_sk');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_addr_sk');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_store_sk');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_reason_sk');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_ticket_number');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_return_quantity');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_return_amt');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_return_tax');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_return_amt_inc_tax');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_fee');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_return_ship_cost');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_refunded_cash');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_reversed_charge');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_store_credit');" \
    -c "PRAGMA build_column_imprints('store_returns', 'sr_net_loss');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_sold_date_sk');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_sold_time_sk');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_item_sk');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_customer_sk');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_cdemo_sk');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_hdemo_sk');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_addr_sk');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_store_sk');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_promo_sk');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_ticket_number');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_quantity');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_wholesale_cost');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_list_price');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_sales_price');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_ext_discount_amt');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_ext_sales_price');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_ext_wholesale_cost');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_ext_list_price');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_ext_tax');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_coupon_amt');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_net_paid');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_net_paid_inc_tax');" \
    -c "PRAGMA build_column_imprints('store_sales', 'ss_net_profit');" \
    -c "PRAGMA build_column_imprints('time_dim', 't_time_sk');" \
    -c "PRAGMA build_column_imprints('time_dim', 't_time');" \
    -c "PRAGMA build_column_imprints('time_dim', 't_hour');" \
    -c "PRAGMA build_column_imprints('time_dim', 't_minute');" \
    -c "PRAGMA build_column_imprints('time_dim', 't_second');" \
    -c "PRAGMA build_column_imprints('warehouse', 'w_warehouse_sk');" \
    -c "PRAGMA build_column_imprints('warehouse', 'w_warehouse_sq_ft');" \
    -c "PRAGMA build_column_imprints('warehouse', 'w_gmt_offset');" \
    -c "PRAGMA build_column_imprints('web_page', 'wp_web_page_sk');" \
    -c "PRAGMA build_column_imprints('web_page', 'wp_rec_start_date');" \
    -c "PRAGMA build_column_imprints('web_page', 'wp_rec_end_date');" \
    -c "PRAGMA build_column_imprints('web_page', 'wp_creation_date_sk');" \
    -c "PRAGMA build_column_imprints('web_page', 'wp_access_date_sk');" \
    -c "PRAGMA build_column_imprints('web_page', 'wp_customer_sk');" \
    -c "PRAGMA build_column_imprints('web_page', 'wp_char_count');" \
    -c "PRAGMA build_column_imprints('web_page', 'wp_link_count');" \
    -c "PRAGMA build_column_imprints('web_page', 'wp_image_count');" \
    -c "PRAGMA build_column_imprints('web_page', 'wp_max_ad_count');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_returned_date_sk');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_returned_time_sk');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_item_sk');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_refunded_customer_sk');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_refunded_cdemo_sk');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_refunded_hdemo_sk');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_refunded_addr_sk');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_returning_customer_sk');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_returning_cdemo_sk');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_returning_hdemo_sk');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_returning_addr_sk');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_web_page_sk');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_reason_sk');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_order_number');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_return_quantity');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_return_amt');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_return_tax');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_return_amt_inc_tax');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_fee');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_return_ship_cost');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_refunded_cash');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_reversed_charge');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_account_credit');" \
    -c "PRAGMA build_column_imprints('web_returns', 'wr_net_loss');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_sold_date_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_sold_time_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_ship_date_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_item_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_bill_customer_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_bill_cdemo_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_bill_hdemo_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_bill_addr_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_ship_customer_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_ship_cdemo_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_ship_hdemo_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_ship_addr_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_web_page_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_web_site_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_ship_mode_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_warehouse_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_promo_sk');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_order_number');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_quantity');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_wholesale_cost');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_list_price');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_sales_price');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_ext_discount_amt');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_ext_sales_price');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_ext_wholesale_cost');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_ext_list_price');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_ext_tax');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_coupon_amt');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_ext_ship_cost');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_net_paid');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_net_paid_inc_tax');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_net_paid_inc_ship');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_net_paid_inc_ship_tax');" \
    -c "PRAGMA build_column_imprints('web_sales', 'ws_net_profit');" \
    -c "PRAGMA build_column_imprints('web_site', 'web_site_sk');" \
    -c "PRAGMA build_column_imprints('web_site', 'web_rec_start_date');" \
    -c "PRAGMA build_column_imprints('web_site', 'web_rec_end_date');" \
    -c "PRAGMA build_column_imprints('web_site', 'web_open_date_sk');" \
    -c "PRAGMA build_column_imprints('web_site', 'web_close_date_sk');" \
    -c "PRAGMA build_column_imprints('web_site', 'web_mkt_id');" \
    -c "PRAGMA build_column_imprints('web_site', 'web_company_id');" \
    -c "PRAGMA build_column_imprints('web_site', 'web_gmt_offset');" \
    -c "PRAGMA build_column_imprints('web_site', 'web_tax_percentage');" \
    -c "PRAGMA force_checkpoint;" \
    -c "CHECKPOINT;" \
    | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

EXIT_CODE=$?
END_TIME=$(date +%s%N)
END_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')
DURATION=$((END_TIME - START_TIME))

echo "Column Imprints build time: ${DURATION:0:-6}ms" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

# Run TPC-DS benchmark
echo "Running TPC-DS benchmark" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

echo "Loop 1: Column Imprints OFF first, then ON"  | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
for i in $(seq 1 99); do
    for j in $(seq 1 $LOOPS); do
        START_TIME=$(date +%s%N)
        START_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')

        ./build/release/duckdb ${DB_NAME} -c ".timer on" -c "SET enable_column_imprint=false;" -c "PRAGMA tpcds(${i});"

        EXIT_CODE=$?
        END_TIME=$(date +%s%N)
        END_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')
        DURATION=$((END_TIME - START_TIME))
        OFF_DURATIONS_ROUND1[$i]=$DURATION

        echo "Duration: ${DURATION:0:-6}ms" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
        echo "$i,1,off,$START_READABLE,$END_READABLE,$DURATION,$(if [ $EXIT_CODE -eq 0 ]; then echo "SUCCESS"; else echo "FAILED"; fi)" >> "$RESULTS_DIR/$TIMING_FILE"
    done
    for j in $(seq 1 $LOOPS); do
        START_TIME=$(date +%s%N)
        START_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')

        ./build/release/duckdb ${DB_NAME} -c ".timer on" -c "SET enable_column_imprint=true;" -c "PRAGMA tpcds(${i});"

        EXIT_CODE=$?
        END_TIME=$(date +%s%N)
        END_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')
        DURATION=$((END_TIME - START_TIME))
        ON_DURATIONS_ROUND1[$i]=$DURATION

        echo "Duration: ${DURATION:0:-6}ms" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
        echo "$i,1,on,$START_READABLE,$END_READABLE,$DURATION,$(if [ $EXIT_CODE -eq 0 ]; then echo "SUCCESS"; else echo "FAILED"; fi)" >> "$RESULTS_DIR/$TIMING_FILE"
    done

done


echo "Loop 2: Column Imprints ON first, then OFF"
for i in $(seq 1 99); do
    for j in $(seq 1 $LOOPS); do
        START_TIME=$(date +%s%N)
        START_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')

        ./build/release/duckdb ${DB_NAME} -c ".timer on" -c "SET enable_column_imprint=true;" -c "PRAGMA tpcds(${i});"

        EXIT_CODE=$?
        END_TIME=$(date +%s%N)
        END_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')
        DURATION=$((END_TIME - START_TIME))
        ON_DURATIONS_ROUND2[$i]=$DURATION

        echo "Duration: ${DURATION:0:-6}ms" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
        echo "$i,2,on,$START_READABLE,$END_READABLE,$DURATION,$(if [ $EXIT_CODE -eq 0 ]; then echo "SUCCESS"; else echo "FAILED"; fi)" >> "$RESULTS_DIR/$TIMING_FILE"
    done
    for j in $(seq 1 $LOOPS); do
        START_TIME=$(date +%s%N)
        START_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')

        ./build/release/duckdb ${DB_NAME} -c ".timer on" -c "SET enable_column_imprint=false;" -c "PRAGMA tpcds(${i});"

        EXIT_CODE=$?
        END_TIME=$(date +%s%N)
        END_READABLE=$(date '+%Y-%m-%d %H:%M:%S.%N')
        DURATION=$((END_TIME - START_TIME))
        OFF_DURATIONS_ROUND2[$i]=$DURATION

        echo "Duration: ${DURATION:0:-6}ms" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
        echo "$i,2,off,$START_READABLE,$END_READABLE,$DURATION,$(if [ $EXIT_CODE -eq 0 ]; then echo "SUCCESS"; else echo "FAILED"; fi)" >> "$RESULTS_DIR/$TIMING_FILE"
    done
done

echo "=== CALCULATING FINAL RESULTS ===" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

for i in $(seq 1 99); do
    # Calculate averages
    AVG_OFF=$(( (OFF_DURATIONS_ROUND1[$i] + OFF_DURATIONS_ROUND2[$i]) / 2 ))
    AVG_ON=$(( (ON_DURATIONS_ROUND1[$i] + ON_DURATIONS_ROUND2[$i]) / 2 ))

    # Calculate performance improvement (always relative to OFF setting)
    IMPROVEMENT=$((AVG_OFF - AVG_ON))  # Positive if ON is faster, negative if ON is slower
    if [ "$AVG_OFF" -gt 0 ]; then
        IMPROVEMENT_PERCENT=$(echo "scale=2; $IMPROVEMENT * 100 / $AVG_OFF" | bc -l)
    else
        IMPROVEMENT_PERCENT=0
    fi

    IMPROVEMENT_MS=$((IMPROVEMENT / 1000000))

    # Generate descriptive text
    if [ $IMPROVEMENT_MS -gt 0 ]; then
        IMPROVEMENT_TEXT="ON faster by ${IMPROVEMENT_MS}ms (${IMPROVEMENT_PERCENT}%)"
    elif [ $IMPROVEMENT_MS -lt 0 ]; then
        # Convert negative improvement to positive for display
        ABS_IMPROVEMENT=$((-IMPROVEMENT_MS))
        # Handle negative percentage (remove minus sign and convert to positive)
        ABS_PERCENT=$(echo "$IMPROVEMENT_PERCENT" | sed 's/^-//')
        IMPROVEMENT_TEXT="ON slower by ${ABS_IMPROVEMENT}ms (${ABS_PERCENT}%)"
    else
        IMPROVEMENT_TEXT="ON and OFF have same performance (0ms, 0.00%)"
    fi

    echo "Final Results for Query $i:" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
    echo "  enable_column_imprint = OFF : ${AVG_OFF:0:-6}ms (avg: ${OFF_DURATIONS_ROUND1[$i]:0:-6}ms, ${OFF_DURATIONS_ROUND2["$i"]:0:-6}ms)" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
    echo "  enable_column_imprint = ON:  ${AVG_ON:0:-6}ms (avg: ${ON_DURATIONS_ROUND1["$i"]:0:-6}ms, ${ON_DURATIONS_ROUND2["$i"]:0:-6}ms)" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
    echo "  Result: $IMPROVEMENT_TEXT" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
    echo "================================================" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

    # Write to comparison CSV
    echo "$i,$AVG_OFF,$AVG_ON,$IMPROVEMENT,$IMPROVEMENT_PERCENT,loop1,loop2" >> "$RESULTS_DIR/$COMPARISON_FILE"
done

echo "Performance comparison summary:" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
echo "----------------------------------------" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

# Compute stats from comparison file (exclude CSV header)
tail -n +2 "$RESULTS_DIR/$COMPARISON_FILE" | awk -F',' '
    BEGIN {
        count=0;
        off_faster=0;
        on_faster=0;
        on_faster_total_improvement=0;
        off_faster_total_improvement=0;
        max_improvement=0;
    }
    {
        count++
        off_duration=$2
        on_duration=$3
        off_duration = off_duration / 1000000
        on_duration = on_duration / 1000000

        if (off_duration > on_duration) {
            on_faster++
            improvement = off_duration - on_duration
            on_faster_total_improvement += improvement
            if (improvement > max_improvement) max_improvement = improvement
        } else if (on_duration > off_duration) {
            off_faster++
            improvement = on_duration - off_duration
            off_faster_total_improvement += improvement
            if (improvement > max_improvement) max_improvement = improvement
        }
    }
    END {
        if (count > 0) {
            printf "Total queries tested: %d\n", count
            printf "enable_column_imprint = on faster: %d (%.1f%%)\n", on_faster, (on_faster/count)*100
            printf "enable_column_imprint = off faster: %d (%.1f%%)\n", off_faster, (off_faster/count)*100
            printf "Average improvement when on is faster: %.2fms\n", (on_faster > 0) ? on_faster_total_improvement/on_faster : 0
            printf "Average improvement when off is faster: %.2fms\n", (off_faster > 0) ? off_faster_total_improvement/off_faster : 0
            printf "Maximum improvement observed: %.2fms\n", max_improvement
        }
    }' | tee -a "$RESULTS_DIR/$OUTPUT_FILE"

echo "================================================" | tee -a "$RESULTS_DIR/$OUTPUT_FILE"
