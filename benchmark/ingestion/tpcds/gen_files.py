import os


tables = [
"call_center",
"household_demographics",
"store_returns",
"catalog_page",
"income_band",
"store_sales",
"catalog_returns",
"inventory",
"time_dim",
"catalog_sales",
"item",
"warehouse",
"customer",
"promotion",
"web_page",
"customer_address",
"reason",
"web_returns",
"customer_demographics",
"ship_mode",
"web_sales",
"date_dim",
"store",
"web_site"
]



for table_name in tables:
    file_text = f"""# name: benchmark/micro/ingestion/tpcds/ingest_{table_name}.benchmark
# description: benchmark ingestion of {table_name}
# group: [tpcds]

template benchmark/ingestion/tpcds/tpcds_ingestion.benchmark.in
table_name={table_name}
"""
    file_name = f"ingest_{table_name}.benchmark"
    f = open(file_name, "w+")
    f.write(file_text)