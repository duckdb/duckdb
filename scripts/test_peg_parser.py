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
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument("--test-file", type=str, help="Path to the SQL logic file", default='')
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
    # single quotes as identifier
    'test/sql/binder/table_alias_single_quotes.test',
    'test/sql/binder/test_string_alias.test'
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
            exit(1)


