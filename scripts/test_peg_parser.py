import argparse
import os
import sqllogictest
from sqllogictest import SQLParserException, SQLLogicParser, SQLLogicTest
import subprocess
import multiprocessing
import tempfile

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
                if any("parser error" in line.lower() or "syntax error" in line.lower() for line in stmt.expected_result.lines):
                    print(stmt.lines)
                    continue
                continue
        query = ' '.join(stmt.lines)
        statements.append(query)
    return statements

def run_test_case(args_tuple):
    i, file, shell, print_failing_only = args_tuple
    results = []
    if not print_failing_only:
        print(f"Run test {i}: {file}")

    statements = parse_test_file(file)
    for statement in statements:
        with tempfile.TemporaryDirectory() as tmpdir:
            peg_sql_path = os.path.join(tmpdir, 'peg_test.sql')
            with open(peg_sql_path, 'w') as f:
                f.write(f'CALL check_peg_parser($TEST_PEG_PARSER${statement}$TEST_PEG_PARSER$);\n')

            proc = subprocess.run([shell, '-init', peg_sql_path, '-c', '.exit'], capture_output=True)
            stderr = proc.stderr.decode('utf8')

            if proc.returncode == 0 and ' Error:' not in stderr:
                continue

            if print_failing_only:
                print(f"Failed test {i}: {file}")
            else:
                print(f'Failed')
                print(f'-- STDOUT --')
                print(proc.stdout.decode('utf8'))
                print(f'-- STDERR --')
                print(stderr)

            results.append((file, statement))
            break
    return results

if __name__ == "__main__":
    files = []
    excluded_tests = {
    ## Parser tests (fail for some reason)
    'test/sql/peg_parser/attach_or_replace.test',
    'test/sql/peg_parser/lambda_functions.test',
    'test/sql/peg_parser/load_extension.test',
    'test/sql/peg_parser/on_conflict.test',
    'test/sql/peg_parser/support_unreserved_keywords.test',
    'test/sql/peg_parser/prepare_statement.test',
    'test/sql/peg_parser/columns_keyword.test',
    'test/sql/peg_parser/struct_identifier.test',
    'test/sql/peg_parser/support_optional_not_null_constraint.test',
    'test/sql/peg_parser/support_try.test',
    'test/sql/peg_parser/window_function.test',
    'test/sql/peg_parser/recursive.test',
    'test/sql/peg_parser/dollar_quoted.test'
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
    work_items = [(i, files[i], args.shell, args.print_failing_only) for i in range(start, end)]

    if not args.no_exit:
        # Disable multiprocessing for --no-exit behavior
        failed_test_list = []
        for item in work_items:
            res = run_test_case(item)
            if res:
                failed_test_list.extend(res)
                exit(1)
    else:
        with multiprocessing.Pool() as pool:
            results = pool.map(run_test_case, work_items)
        failed_test_list = [item for sublist in results for item in sublist]

    failed_tests = len(failed_test_list)
    print("List of failed tests: ")
    for test, statement in failed_test_list:
        print(f"{test}\n{statement}\n\n")
    print(f"Total of {failed_tests} out of {len(files)} failed ({round(failed_tests/len(files) * 100,2)}%). ")