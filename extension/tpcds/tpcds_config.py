import os

# list all include directories
include_directories = [
    os.path.sep.join(x.split('/'))
    for x in ['extension/tpcds/include', 'extension/tpcds/dsdgen/include', 'extension/tpcds/dsdgen/include/dsdgen-c']
]
# source files
source_files = [os.path.sep.join(x.split('/')) for x in ['extension/tpcds/tpcds_extension.cpp']]
source_files += [
    os.path.sep.join(x.split('/'))
    for x in [
        'extension/tpcds/dsdgen/dsdgen.cpp',
        'extension/tpcds/dsdgen/append_info-c.cpp',
        'extension/tpcds/dsdgen/dsdgen_helpers.cpp',
    ]
]
source_files += [
    os.path.sep.join(x.split('/'))
    for x in [
        'extension/tpcds/dsdgen/dsdgen-c/skip_days.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/address.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/build_support.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/date.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/dbgen_version.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/decimal.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/dist.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/error_msg.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/genrand.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/join.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/list.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/load.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/misc.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/nulls.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/parallel.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/permute.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/pricing.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/r_params.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/release.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/scaling.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/scd.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/sparse.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/StringBuffer.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/tdef_functions.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/tdefs.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/text.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_call_center.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_catalog_page.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_catalog_returns.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_catalog_sales.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_customer.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_customer_address.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_customer_demographics.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_datetbl.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_household_demographics.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_income_band.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_inventory.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_item.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_promotion.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_reason.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_ship_mode.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_store.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_store_returns.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_store_sales.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_timetbl.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_warehouse.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_web_page.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_web_returns.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_web_sales.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/w_web_site.cpp',
        'extension/tpcds/dsdgen/dsdgen-c/init.cpp',
    ]
]
