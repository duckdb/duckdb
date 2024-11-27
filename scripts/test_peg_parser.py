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
group.add_argument("--test-list", type=str, help="Path to the file that contains a newline separated list of test files", default='')
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
    'test/sql/attach/attach_nested_types.test',           # table
    'test/sql/binder/test_function_chainging_alias.test', # trim
    'test/sql/cast/test_try_cast.test',                   # try_cast
    'test/sql/copy/csv/auto/test_auto_8573.test',         # columns
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
    # struct keyword
    'test/sql/copy/parquet/writer/write_struct.test',
    # single quotes as identifier
    'test/sql/binder/table_alias_single_quotes.test',
    'test/sql/binder/test_string_alias.test',
    # optional "AS" in UPDATE table alias
    'test/sql/catalog/function/test_complex_macro.test',
    # env parameters (${PARAMETER})
    ' test/sql/copy/s3/download_config.test',
}
if args.all_tests:
    # run all tests
    test_dir = os.path.join('test', 'sql')
    files = find_tests_recursive(test_dir, excluded_tests)
else:
    # run a single test
    files.append(args.test_file)
files.sort()

start = args.offset if args.offset is not None else 0
end = start + args.count if args.count is not None else len(files)

for i in range(start, end):
    file = files[i]
    print(f"Run test {i}/{end}: {file}")

    statements = parse_test_file(file)
    for statement in statements:
        with open('peg_test.sql', 'w+') as f:
            f.write(f'CALL check_peg_parser($TEST_PEG_PARSER${statement}$TEST_PEG_PARSER$);\n')
        proc = subprocess.run([args.shell, '-init', 'peg_test.sql', '-c', '.exit'], capture_output=True)
        stderr = proc.stderr.decode('utf8')
        if proc.returncode != 2 or ' Error:' in stderr:
            print(f'Failed')
            print(f'-- STDOUT --')
            print(proc.stdout.decode('utf8'))
            print(f'-- STDERR --')
            print(stderr)
            if not args.no_exit:
                exit(1)
            else:
                break


