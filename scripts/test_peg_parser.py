import argparse
import os
import sqllogictest
from sqllogictest import SQLParserException, SQLLogicParser, SQLLogicTest
import duckdb
import subprocess

parser = argparse.ArgumentParser(description="Test serialization")
parser.add_argument("--shell", type=str, help="Shell binary to run", default=os.path.join('build', 'debug', 'duckdb'))
parser.add_argument("--offset", type=int, help="File offset", default=None)
parser.add_argument("--count", type=int, help="File count", default=None)
parser.add_argument('--no-exit', action='store_true', help='Do not exit after a test fails', default=False)
parser.add_argument('--print-failing-only', action='store_true', help='Print failing tests only', default=False)
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("--test-file", type=str, help="Path to the SQL logic file", default='')
group.add_argument(
    "--test-list", type=str, help="Path to the file that contains a newline separated list of test files", default=''
)
group.add_argument("--all-tests", action='store_true', help="Run all tests", default=False)
args = parser.parse_args()


def find_tests_recursive(dir, excluded_paths):
    test_list = []
    for f in os.listdir(dir):
        path = os.path.join(dir, f)
        if path in excluded_paths:
            continue
        if os.path.isdir(path):
            test_list += find_tests_recursive(path, excluded_paths)
        elif path.endswith('.test') or path.endswith('.test_slow'):
            test_list.append(path)
    return test_list


def parse_test_file(filename):
    if not os.path.isfile(filename):
        raise Exception(f"File {filename} not found")
    parser = SQLLogicParser()
    try:
        out: Optional[SQLLogicTest] = parser.parse(filename)
        if not out:
            raise SQLParserException(f"Test {filename} could not be parsed")
    except:
        return []
    loop_count = 0
    statements = []
    for stmt in out.statements:
        if type(stmt) is sqllogictest.statement.skip.Skip:
            # mode skip - just skip entire test
            break
        if type(stmt) is sqllogictest.statement.loop.Loop or type(stmt) is sqllogictest.statement.foreach.Foreach:
            loop_count += 1
        if type(stmt) is sqllogictest.statement.endloop.Endloop:
            loop_count -= 1
        if loop_count > 0:
            # loops are ignored currently
            continue
        if not (
            type(stmt) is sqllogictest.statement.query.Query or type(stmt) is sqllogictest.statement.statement.Statement
        ):
            # only handle query and statement nodes for now
            continue
        if type(stmt) is sqllogictest.statement.statement.Statement:
            # skip expected errors
            if stmt.expected_result.type == sqllogictest.ExpectedResult.Type.ERROR:
                continue
        query = ' '.join(stmt.lines)
        statements.append(query)
    return statements


files = []
excluded_tests = {
    # reserved keyword mismatches
    'test/sql/attach/attach_nested_types.test',  # table
    'test/sql/binder/test_function_chainging_alias.test',  # trim
    'test/sql/cast/test_try_cast.test',  # try_cast
    'test/sql/copy/csv/auto/test_auto_8573.test',  # columns
    'test/sql/copy/csv/auto/test_csv_auto.test',
    'test/sql/copy/csv/auto/test_normalize_names.test',
    'test/sql/copy/csv/auto/test_sniffer_blob.test',
    'test/sql/copy/csv/bug_10283.test',
    'test/sql/copy/csv/code_cov/buffer_manager_finalize.test',
    'test/sql/copy/csv/code_cov/csv_state_machine_invalid_utf.test',
    'test/sql/copy/csv/column_names.test',
    'test/sql/copy/csv/csv_enum.test',
    'test/sql/copy/csv/csv_enum_storage.test',
    'test/sql/copy/csv/csv_null_byte.test',
    'test/sql/copy/csv/csv_nullstr_list.test',
    'test/sql/copy/csv/empty_first_line.test',
    'test/sql/copy/csv/glob/read_csv_glob.test',
    'test/sql/copy/csv/null_padding_big.test',
    'test/sql/copy/csv/parallel/csv_parallel_buffer_size.test',
    'test/sql/copy/csv/parallel/csv_parallel_new_line.test_slow',
    'test/sql/copy/csv/parallel/csv_parallel_null_option.test',
    'test/sql/copy/csv/parallel/test_5438.test',
    'test/sql/copy/csv/parallel/test_7578.test',
    'test/sql/copy/csv/parallel/test_multiple_files.test',
    'test/sql/copy/csv/recursive_read_csv.test',
    'test/sql/copy/csv/rejects/csv_incorrect_columns_amount_rejects.test',
    'test/sql/copy/csv/rejects/csv_rejects_flush_cast.test',
    'test/sql/copy/csv/rejects/csv_rejects_flush_message.test',
    'test/sql/copy/csv/rejects/csv_rejects_maximum_line.test',
    'test/sql/copy/csv/rejects/csv_rejects_read.test',
    'test/sql/copy/csv/rejects/csv_unquoted_rejects.test',
    'test/sql/copy/csv/rejects/test_invalid_utf_rejects.test',
    'test/sql/copy/csv/rejects/test_mixed.test',
    'test/sql/copy/csv/rejects/test_multiple_errors_same_line.test',
    'test/sql/copy/csv/struct_padding.test',
    'test/sql/copy/csv/test_12596.test',
    'test/sql/copy/csv/test_8890.test',
    'test/sql/copy/csv/test_big_header.test',
    'test/sql/copy/csv/test_comment_midline.test',
    'test/sql/copy/csv/test_comment_option.test',
    'test/sql/copy/csv/test_csv_column_count_mismatch.test_slow',
    'test/sql/copy/csv/test_csv_json.test',
    'test/sql/copy/csv/test_csv_mixed_casts.test',
    'test/sql/copy/csv/test_csv_projection_pushdown.test',
    'test/sql/copy/csv/test_date.test',
    'test/sql/copy/csv/test_date_sniffer.test',
    'test/sql/copy/csv/test_dateformat.test',
    'test/sql/copy/csv/test_decimal.test',
    'test/sql/copy/csv/test_encodings.test',
    'test/sql/copy/csv/test_greek_utf8.test',
    'test/sql/copy/csv/test_headers_12089.test',
    'test/sql/copy/csv/test_ignore_errors.test',
    'test/sql/copy/csv/test_ignore_mid_null_line.test',
    'test/sql/copy/csv/test_issue3562_assertion.test',
    'test/sql/copy/csv/test_missing_row.test',
    'test/sql/copy/csv/test_null_padding_projection.test',
    'test/sql/copy/csv/test_quote_default.test',
    'test/sql/copy/csv/test_read_csv.test',
    'test/sql/copy/csv/test_skip_bom.test',
    'test/sql/copy/csv/test_skip_header.test',
    'test/sql/copy/csv/test_sniff_csv.test',
    'test/sql/copy/csv/test_sniff_csv_options.test',
    'test/sql/copy/csv/test_sniffer_tab_delimiter.test',
    'test/sql/copy/csv/test_time.test',
    'test/sql/copy/csv/test_validator.test',
    'test/sql/copy/csv/tsv_copy.test',
    'test/sql/copy/per_thread_output.test',
    'test/sql/detailed_profiler/test_detailed_profiler.test',
    'test/sql/function/timestamp/date_diff.test_slow',
    'test/sql/json/issues/issue10784.test',
    'test/sql/json/issues/issue10866.test',
    'test/sql/json/issues/issue13212.test',
    'test/sql/json/issues/issue14167.test',
    'test/sql/json/table/read_json.test',
    'test/sql/json/table/read_json_dates.test',
    'test/sql/json/table/read_json_objects.test',
    'test/sql/json/table/read_json_union.test',
    # struct keyword
    'test/sql/copy/parquet/writer/write_struct.test',
    'test/sql/json/scalar/json_nested_casts.test',
    # when keyword
    'test/sql/join/asof/test_asof_join.test',
    # end keyword
    'test/sql/join/asof/test_asof_join_doubles.test',
    'test/sql/join/iejoin/iejoin_issue_6314.test_slow',
    'test/sql/join/iejoin/test_iejoin.test',
    'test/sql/join/iejoin/test_iejoin_events.test',
    'test/sql/join/iejoin/test_iejoin_sort_tasks.test_slow',
    'test/sql/join/semianti/plan_blockwise_NL_join_with_mutliple_conditions.test',
    # single quotes as identifier
    'test/sql/binder/table_alias_single_quotes.test',
    'test/sql/binder/test_string_alias.test',
    # optional "AS" in UPDATE table alias
    'test/sql/catalog/function/test_complex_macro.test',
    # env parameters (${PARAMETER})
    'test/sql/copy/s3/download_config.test',
    # complex arithmetic
    'test/sql/function/operator/test_arithmetic_sqllogic.test',
    # dollar quoted strings
    'test/sql/parser/dollar_quotes_internal_issue2224.test',
    # unicode spaces
    'test/sql/parser/invisible_spaces.test',
    # NOT INVESTIGATED
    'test/sql/parser/join_alias.test',
    'test/sql/pg_catalog/sqlalchemy.test',
    'test/sql/pivot/pivot_bigquery.test',
    'test/sql/pivot/pivot_databricks.test',
    'test/sql/pivot/pivot_enum.test',
    'test/sql/pragma/profiling/test_custom_profiling_blocked_thread_time.test',
    'test/sql/pragma/profiling/test_custom_profiling_disable_metrics.test',
    'test/sql/pragma/profiling/test_custom_profiling_optimizer.test',
    'test/sql/pragma/profiling/test_custom_profiling_planner.test_slow',
    'test/sql/pragma/profiling/test_default_profiling_settings.test',
    'test/sql/pragma/profiling/test_empty_profiling_settings.test',
    'test/sql/pragma/test_query_log.test',
    'test/sql/prepared/parameter_variants.test',
    'test/sql/prepared/prepare_copy.test',
    'test/sql/prepared/prepared_named_param.test',
    'test/sql/prepared/test_issue_6276.test',
    'test/sql/projection/select_star_exclude.test',
    'test/sql/projection/select_star_rename.test',
    'test/sql/projection/test_scalar_projection.test',
    'test/sql/sample/large_sample.test_slow',
    'test/sql/sample/test_sample.test_slow',
    'test/sql/select/test_select_alias_prefix_colon.test',
    'test/sql/storage/optimistic_write/optimistic_write_cyclic_dictionary.test_slow',
    'test/sql/subquery/lateral/lateral_binding_views.test',
    'test/sql/subquery/lateral/pg_lateral.test',
    'test/sql/subquery/scalar/array_order_subquery.test',
    'test/sql/subquery/scalar/array_subquery.test',
    'test/sql/subquery/scalar/order_by_correlated.test',
    'test/sql/subquery/scalar/test_delete_subquery.test',
    'test/sql/subquery/scalar/test_update_subquery.test',
    'test/sql/table_function/information_schema.test',
    'test/sql/table_function/test_information_schema_columns.test',
    'test/sql/types/bit/bit_issue_11211.test',
    'test/sql/types/bit/test_bit_bitwise_operations.test',
    'test/sql/types/decimal/test_empty_dec.test',
    'test/sql/types/decimal/test_empty_decimal.test',
    'test/sql/types/enum/test_5983.test',
    'test/sql/types/enum/test_enum.test',
    'test/sql/types/enum/test_enum_from_query.test_slow',
    'test/sql/types/hugeint/test_hugeint_bitwise.test',
    'test/sql/types/list/unnest_table_function.test',
    'test/sql/types/nested/list/lineitem_list.test_slow',
    'test/sql/types/nested/list/test_list_slice_negative_step.test',
    'test/sql/types/nested/list/test_list_slice_step.test',
    'test/sql/types/nested/map/test_map_constant.test',
    'test/sql/types/nested/struct/lineitem_struct.test_slow',
    'test/sql/types/nested/struct/test_struct.test',
    'test/sql/types/numeric/test_empty_numeric.test',
    'test/sql/types/uhugeint/test_uhugeint_arithmetic.test',
    'test/sql/update/test_multiple_assignment.test',
    'test/sql/update/test_update_from.test',
    'test/sql/upsert/minimal_reproducable_example.test',
    'test/sql/upsert/postgres/composite_key.test',
    'test/sql/upsert/postgres/non_spurious_duplicate_violation.test',
    'test/sql/upsert/postgres/planner_preprocessing.test',
    'test/sql/upsert/postgres/single_key.test',
    'test/sql/upsert/test_big_insert.test',
    'test/sql/upsert/test_generated_column.test',
    'test/sql/upsert/upsert_aliased.test',
    'test/sql/upsert/upsert_basic.test',
    'test/sql/upsert/upsert_conflict_target.test',
    'test/sql/upsert/upsert_default.test',
    'test/sql/upsert/upsert_default_expressions.test',
    'test/sql/upsert/upsert_default_value_causes_conflict.test',
    'test/sql/upsert/upsert_excluded_references.test',
    'test/sql/upsert/upsert_explicit_index.test',
    'test/sql/upsert/upsert_lambda.test',
    'test/sql/upsert/upsert_order_coverage.test',
    'test/sql/upsert/upsert_partial_update.test',
    'test/sql/upsert/upsert_set_expressions.test',
    'test/sql/upsert/upsert_shorthand.test',
    'test/sql/upsert/upsert_transaction.test',
    'test/sql/upsert/upsert_unique_null.test',
    'test/sql/vacuum/test_analyze.test',
    'test/sql/vacuum/vacuum_nested_types.test',
    'test/sql/variables/test_variables.test',
    'test/sql/window/test_ignore_nulls.test',
    'test/sql/window/test_mad_window.test',
    'test/sql/window/test_mode_window.test',
    'test/sql/window/test_nthvalue.test',
    'test/sql/window/test_quantile_window.test',
    'test/sql/window/test_streaming_window.test',
    'test/sql/window/test_window_clause.test',
    'test/sql/window/test_window_constant_aggregate.test',
    'test/sql/window/test_window_exclude.test',
    'test/sql/window/test_window_range.test',
    'test/sql/create/create_as.test',
    'test/sql/function/autocomplete/tpch.test',
}
if args.all_tests:
    # run all tests
    test_dir = os.path.join('test', 'sql')
    files = find_tests_recursive(test_dir, excluded_tests)
elif len(args.test_list) > 0:
    with open(args.test_list, 'r') as f:
        files = [x.strip() for x in f.readlines() if x.strip() not in excluded_tests]
else:
    # run a single test
    files.append(args.test_file)
files.sort()

start = args.offset if args.offset is not None else 0
end = start + args.count if args.count is not None else len(files)

for i in range(start, end):
    file = files[i]
    if not args.print_failing_only:
        print(f"Run test {i}/{end}: {file}")

    statements = parse_test_file(file)
    for statement in statements:
        with open('peg_test.sql', 'w+') as f:
            f.write(f'CALL check_peg_parser($TEST_PEG_PARSER${statement}$TEST_PEG_PARSER$);\n')
        proc = subprocess.run([args.shell, '-init', 'peg_test.sql', '-c', '.exit'], capture_output=True)
        stderr = proc.stderr.decode('utf8')
        if proc.returncode == 2 and ' Error:' not in stderr:
            continue
        if args.print_failing_only:
            print(f"Failed test {i}/{end}: {file}")
        else:
            print(f'Failed')
            print(f'-- STDOUT --')
            print(proc.stdout.decode('utf8'))
            print(f'-- STDERR --')
            print(stderr)
        if not args.no_exit:
            exit(1)
        else:
            break
