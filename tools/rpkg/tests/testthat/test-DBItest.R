DBItest::test_all(c(
  "package_name", # wontfix
  "package_dependencies", # wontfix
  "reexport", # wontfix

  "constructor", # wontfix
  "send_query_only_one_result_set", # wontfix
  "send_statement_only_one_result_set", # wontfix
  "send_query_stale_warning", # wontfix
  "send_statement_stale_warning", # wontfix

  "roundtrip_timestamp", # broken test

  "data_logical", # casting NULL issue

  "roundtrip_time",
  "roundtrip_mixed",
  "roundtrip_field_types", # strange
  "data_64_bit_numeric_warning", # not now
  "data_64_bit_lossless", # not now,
  "roundtrip_64_bit_character",

  # new tests skipped after DBI upgrade
  "connect_format",
  "connect_bigint_integer",
  "connect_bigint_character",
  "connect_bigint_integer64",
  "create_table_overwrite",
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
  "remove_table_missing_succeed",
  "column_info_closed",
  "column_info_consistent"
))
