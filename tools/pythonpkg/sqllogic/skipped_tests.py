SKIPPED_TESTS = set(
    [
        'test/sql/timezone/disable_timestamptz_casts.test',  # <-- ICU extension is always loaded
        'test/sql/copy/return_stats_truncate.test',  # <-- <REGEX> handling was changed
        'test/sql/copy/return_stats.test',  # <-- <REGEX> handling was changed
        'test/sql/copy/parquet/writer/skip_empty_write.test',  # <-- <REGEX> handling was changed
        'test/sql/types/map/map_empty.test',
        'test/extension/wrong_function_type.test',  # <-- JSON is always loaded
        'test/sql/insert/test_insert_invalid.test',  # <-- doesn't parse properly
        'test/sql/cast/cast_error_location.test',  # <-- python exception doesn't contain error location yet
        'test/sql/pragma/test_query_log.test',  # <-- query_log gets filled with NULL when con.query(...) is used
        'test/sql/json/table/read_json_objects.test',  # <-- Python client is always loaded with JSON available
        'test/sql/copy/csv/zstd_crash.test',  # <-- Python client is always loaded with Parquet available
        'test/sql/error/extension_function_error.test',  # <-- Python client is always loaded with TPCH available
        'test/optimizer/joins/tpcds_nofail.test',  # <-- Python client is always loaded with TPCDS available
        'test/sql/settings/errors_as_json.test',  # <-- errors_as_json not currently supported in Python
        'test/sql/parallelism/intraquery/depth_first_evaluation_union_and_join.test',  # <-- Python client is always loaded with TPCDS available
        'test/sql/types/timestamp/test_timestamp_tz.test',  # <-- Python client is always loaded wih ICU available - making the TIMESTAMPTZ::DATE cast pass
        'test/sql/parser/invisible_spaces.test',  # <-- Parser is getting tripped up on the invisible spaces
        'test/sql/copy/csv/code_cov/csv_state_machine_invalid_utf.test',  # <-- ConversionException is empty, see Python Mega Issue (duckdb-internal #1488)
        'test/sql/copy/csv/test_csv_timestamp_tz.test',  # <-- ICU is always loaded
        'test/fuzzer/duckfuzz/duck_fuzz_column_binding_tests.test',  # <-- ICU is always loaded
        'test/sql/pragma/test_custom_optimizer_profiling.test',  # Because of logic related to enabling 'restart' statement capabilities, this will not measure the right statement
        'test/sql/pragma/test_custom_profiling_settings.test',  # Because of logic related to enabling 'restart' statement capabilities, this will not measure the right statement
        'test/sql/copy/csv/test_copy.test',  # JSON is always loaded
        'test/sql/copy/csv/test_timestamptz_12926.test',  # ICU is always loaded
        'test/fuzzer/pedro/in_clause_optimization_error.test',  # error message differs due to a different execution path
        'test/sql/order/test_limit_parameter.test',  # error message differs due to a different execution path
        'test/sql/catalog/test_set_search_path.test',  # current_query() is not the same
        'test/sql/catalog/table/create_table_parameters.test',  # prepared statement error quirks
        'test/sql/pragma/profiling/test_custom_profiling_rows_scanned.test',  # we perform additional queries that mess with the expected metrics
        'test/sql/pragma/profiling/test_custom_profiling_disable_metrics.test',  # we perform additional queries that mess with the expected metrics
        'test/sql/pragma/profiling/test_custom_profiling_result_set_size.test',  # we perform additional queries that mess with the expected metrics
        'test/sql/pragma/profiling/test_custom_profiling_result_set_size.test',  # we perform additional queries that mess with the expected metrics
        'test/sql/cte/materialized/materialized_cte_modifiers.test',  # problems connected to auto installing tpcds from remote
        'test/sql/tpcds/dsdgen_readonly.test',  # problems connected to auto installing tpcds from remote
        'test/sql/tpcds/tpcds_sf0.test',  # problems connected to auto installing tpcds from remote
        'test/sql/optimizer/plan/test_filter_pushdown_materialized_cte.test',  # problems connected to auto installing tpcds from remote
        'test/sql/explain/test_explain_analyze.test',  # unknown problem with changes in API
    ]
)
