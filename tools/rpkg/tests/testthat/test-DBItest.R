DBItest::make_context(duckdb(), list(), tweaks = DBItest::tweaks(), name = "duckdb")
DBItest::test_all(c(
	"package_name", # wontfix
	"constructor", # wontfix
	"send_query_only_one_result_set", # wontfix
	"send_statement_only_one_result_set", # wontfix
	"send_query_stale_warning", # wontfix
	"send_statement_stale_warning", # wontfix

	"fetch_no_return_value",
	"get_query_n_bad",
	"get_query_good_after_bad_n",
	"data_character",
	"has_completed_statement",
	"row_count_query",
	"row_count_statement",
	"get_row_count_error",
	"get_rows_affected_error",

	"data_timestamp",
	"data_time_current",
	"data_time",
	"data_date_current",
	"data_date",
  "data_timestamp_current",
  "data_date_typed",
  "data_date_current_typed",
  "data_timestamp_typed",
  "data_timestamp_current_typed",

  "data_64_bit_numeric_warning",  # not now
  "data_64_bit_lossless"  # not now
))
