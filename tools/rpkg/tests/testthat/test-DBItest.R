DBItest::test_all(c(
  "package_name", # wontfix
  "package_dependencies", # wontfix
  "reexport", # wontfix

  "constructor", # wontfix
  "send_query_only_one_result_set", # wontfix
  "send_statement_only_one_result_set", # wontfix
  "send_query_stale_warning", # wontfix
  "send_statement_stale_warning", # wontfix

  if (packageVersion("DBItest") < "1.7.0.9004") "roundtrip_timestamp", # broken test

  "bind_multi_row_zero_length", # https://github.com/duckdb/duckdb/issues/2125
  "bind_numeric.*",
  "bind_character.*",
  "bind_factor.*",
  "bind_date_integer.*",
  "bind_timestamp.*",
  "bind_time_.*",


  "data_64_bit_numeric_warning", # 64 bit, not now
  "data_64_bit_lossless",
  "roundtrip_64_bit_character",
  "connect_bigint_integer",
  "connect_bigint_character",
  "connect_bigint_integer64",

  # new tests skipped after DBI upgrade
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
  "append_roundtrip_character_empty.*",
  "append_roundtrip_factor",
  "append_roundtrip_date",
  "append_roundtrip_time",
  "append_roundtrip_timestamp",
  "append_table_row_names_.*",

  "column_info_consistent" # won't fix: https://github.com/r-dbi/DBItest/issues/181
))
