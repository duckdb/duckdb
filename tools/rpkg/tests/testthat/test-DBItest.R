DBItest::make_context(duckdb::duckdb(), list(debug=F), tweaks = DBItest::tweaks(omit_blob_tests=TRUE, temporary_tables=FALSE, timestamp_cast = function(x) sprintf("CAST('%s' AS TIMESTAMP)", x), date_cast = function(x) sprintf("CAST('%s' AS DATE)", x), time_cast = function(x) sprintf("CAST('%s' AS TIME)", x), name = "duckdb"))

DBItest::test_all(c(
	"package_name", # wontfix
	"constructor", # wontfix
	"send_query_only_one_result_set", # wontfix
	"send_statement_only_one_result_set", # wontfix
	"send_query_stale_warning", # wontfix
	"send_statement_stale_warning", # wontfix

	"get_query_n_bad", # broken test
	"get_query_good_after_bad_n", # broken test
	"has_completed_statement", # broken test

	"data_logical", # casting NULL issue

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


  "roundtrip_timestamp",
  "roundtrip_date",
  "roundtrip_time",
  "roundtrip_field_types", # strange
    "data_64_bit_numeric_warning",  # not now
  "data_64_bit_lossless",  # not now,
  "roundtrip_64_bit_character",
  "roundtrip_64_bit_numeric",
  "roundtrip_numeric_special",

  "begin_write_commit",
  "remove_table_other_con",
  "table_visible_in_other_connection"
  ))
