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

  "send_query_params", # multiple bind parameters
  "get_query_params",
  "bind_return_value",
  "bind_too_many",
  "bind_not_enough",
  "bind_multi_row_unequal_length",
  "bind_unnamed_param_named_placeholders",
  "bind_multi_row",
  "bind_multi_row_zero_length",
  "bind_multi_row_statement",
  "bind_repeated",
  "bind_integer",
  "bind_repeated_untouched",
  "bind_.*",


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