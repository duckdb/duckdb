library("testthat")
skip_on_cran()

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

  "data_64_bit_numeric_warning", # 64 bit, not now
  "data_64_bit_lossless",
  "roundtrip_64_bit_character",
  "connect_bigint_integer",
  "connect_bigint_character",
  "connect_bigint_integer64",
  "append_roundtrip_64_bit_numeric",
  "append_roundtrip_64_bit_character",
  "append_roundtrip_64_bit_roundtrip",

  "column_info_consistent" # won't fix: https://github.com/r-dbi/DBItest/issues/181
))
