DBItest::make_context(duckdb::duckdb(), list(debug=F), tweaks = DBItest::tweaks(omit_blob_tests=TRUE, temporary_tables=FALSE, timestamp_cast = function(x) sprintf("CAST('%s' AS TIMESTAMP)", x), date_cast = function(x) sprintf("CAST('%s' AS DATE)", x), time_cast = function(x) sprintf("CAST('%s' AS TIME)", x)), name = "duckdb")

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
	"roundtrip_timestamp", # broken test
	"data_timestamp", # broken test (fixed in dev)

	"data_logical", # casting NULL issue

	"data_time_current",
	"data_date_current",
	"data_timestamp_current",
	"data_date_current_typed",
	"data_timestamp_current_typed",
	"roundtrip_time",
	"roundtrip_field_types", # strange
	"data_64_bit_numeric_warning",  # not now
	"data_64_bit_lossless",  # not now,
	"roundtrip_64_bit_character",
	"roundtrip_64_bit_numeric",
	"roundtrip_numeric_special",

	"begin_write_commit",
	"remove_table_other_con",
	"table_visible_in_other_connection",
	# new tests skipped after DBI upgrade
	"connect_format",
	"connect_bigint_integer",
	"connect_bigint_character",
	"connect_bigint_integer64",
	"create_table_overwrite",
	"append_table_return",
	"append_table_append_incompatible",
	"append_roundtrip_keywords",
	"append_roundtrip_quotes",
	"append_roundtrip_integer",
	"append_roundtrip_numeric",
	"append_roundtrip_logical",
	"append_roundtrip_null",
	"append_roundtrip_64_bit_numeric",
	"append_roundtrip_64_bit_character",
	"append_roundtrip_64_bit_roundtrip",
	"append_roundtrip_character",
	"append_roundtrip_character_native",
	"append_roundtrip_character_empty",
	"append_roundtrip_factor",
	"append_roundtrip_date",
	"append_roundtrip_time",
	"append_roundtrip_timestamp",
	"append_table_name",
	"append_table_row_names_false",
	"remove_table_missing_succeed",
	"column_info_closed",
	"column_info_consistent",
	"column_info_consistent",
	"reexport",
	"roundtrip_logical",

	"package_dependencies"
	))
