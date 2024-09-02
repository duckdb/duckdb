import sqllogictest
from sqllogictest import SQLParserException, SQLLogicParser, SQLLogicTest
import duckdb
from typing import Optional
import argparse
import shutil
import os
import subprocess

# example usage: python3 scripts/test_serialization_bwc.py --old-source ../duckdb-bugfix --test-file test/sql/aggregate/aggregates/test_median.test

serialized_path = os.path.join('test', 'api', 'serialized_plans')
db_load_path = os.path.join(serialized_path, 'db_load.sql')
queries_path = os.path.join(serialized_path, 'queries.sql')
result_binary = os.path.join(serialized_path, 'serialized_plans.binary')
unittest_binary = os.path.join('build', 'debug', 'test', 'unittest')


def complete_query(q):
    q = q.strip()
    if q.endswith(';'):
        return q
    return q + ';'


def parse_test_file(filename):
    parser = SQLLogicParser()
    try:
        out: Optional[SQLLogicTest] = parser.parse(filename)
        if not out:
            raise SQLParserException(f"Test {filename} could not be parsed")
    except:
        return {'load': [], 'query': []}
    loop_count = 0
    load_statements = []
    query_statements = []
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
        try:
            sql_stmt_list = duckdb.extract_statements(query)
        except KeyboardInterrupt:
            raise
        except:
            continue
        for sql_stmt in sql_stmt_list:
            if sql_stmt.type == duckdb.StatementType.SELECT:
                query_statements.append(query)
            elif sql_stmt.type == duckdb.StatementType.PRAGMA:
                continue
            else:
                load_statements.append(query)
    return {'load': load_statements, 'query': query_statements}


def build_sources(old_source, new_source):
    # generate the sources
    current_path = os.getcwd()
    os.chdir(old_source)
    # build if not yet build
    if not os.path.isfile(unittest_binary):
        res = subprocess.run(['make', 'debug']).returncode
        if res != 0:
            raise Exception("Failed to build old sources")

    # run the verification
    os.chdir(current_path)
    os.chdir(new_source)

    # build if not yet build
    if not os.path.isfile(unittest_binary):
        res = subprocess.run(['make', 'debug']).returncode
        if res != 0:
            raise Exception("Failed to build new sources")
    os.chdir(current_path)


def run_test(filename, old_source, new_source, no_exit):
    statements = parse_test_file(filename)

    # generate the sources
    current_path = os.getcwd()
    os.chdir(old_source)
    # write the files
    with open(os.path.join(old_source, db_load_path), 'w+') as f:
        for stmt in statements['load']:
            f.write(complete_query(stmt) + '\n')

    with open(os.path.join(old_source, queries_path), 'w+') as f:
        for stmt in statements['query']:
            f.write(complete_query(stmt) + '\n')

    # generate the serialization
    my_env = os.environ.copy()
    my_env['GEN_PLAN_STORAGE'] = '1'
    res = subprocess.run(['build/debug/test/unittest', 'Generate serialized plans file'], env=my_env).returncode
    if res != 0:
        print(f"SKIPPING TEST {filename}")
        return True

    os.chdir(current_path)

    # copy over the files
    for f in [db_load_path, queries_path, result_binary]:
        shutil.copy(os.path.join(old_source, f), os.path.join(new_source, f))

    # run the verification
    os.chdir(new_source)

    res = subprocess.run(['build/debug/test/unittest', "Test deserialized plans from file"]).returncode
    if res != 0:
        if no_exit:
            print("BROKEN TEST")
            with open('broken_tests.list', 'a') as f:
                f.write(filename + '\n')
            return False
        raise Exception("Deserialization failure")
    os.chdir(current_path)
    return True


def parse_excluded_tests(path):
    exclusion_list = {}
    with open(path) as f:
        for line in f:
            if len(line.strip()) == 0 or line[0] == '#':
                continue
            exclusion_list[line.strip()] = True
    return exclusion_list


def find_tests_recursive(dir, excluded_paths):
    test_list = []
    for f in os.listdir(dir):
        path = os.path.join(dir, f)
        if path in excluded_paths:
            continue
        if os.path.isdir(path):
            test_list += find_tests_recursive(path, excluded_paths)
        elif path.endswith('.test'):
            test_list.append(path)
    return test_list


def main():
    parser = argparse.ArgumentParser(description="Test serialization")
    parser.add_argument("--new-source", type=str, help="Path to the new source", default='.')
    parser.add_argument("--old-source", type=str, help="Path to the old source")
    parser.add_argument("--start-at", type=str, help="Start running tests at this specific test", default=None)
    parser.add_argument("--no-exit", action="store_true", help="Keep running even if a test fails", default=False)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--test-file", type=str, help="Path to the SQL logic file", default='')
    group.add_argument("--all-tests", action='store_true', help="Run all tests", default=False)
    group.add_argument("--test-list", type=str, help="Load tests to run from a file list", default=None)
    args = parser.parse_args()

    old_source = args.old_source
    new_source = args.new_source
    files = []
    if args.all_tests:
        # run all tests
        excluded_tests = parse_excluded_tests(
            os.path.join(new_source, 'test', 'api', 'serialized_plans', 'excluded_tests.list')
        )
        test_dir = os.path.join('test', 'sql')
        if new_source != '.':
            test_dir = os.path.join(new_source, test_dir)
        files = find_tests_recursive(test_dir, excluded_tests)
    elif args.test_list is not None:
        with open(args.test_list, 'r') as f:
            for line in f:
                if len(line.strip()) == 0:
                    continue
                files.append(line.strip())
    else:
        # run a single test
        files.append(args.test_file)
    files.sort()

    current_path = os.getcwd()
    try:
        build_sources(old_source, new_source)

        all_succeeded = True
        started = False
        if args.start_at is None:
            started = True
        for filename in files:
            if not started:
                if filename == args.start_at:
                    started = True
                else:
                    continue

            print(f"Run test {filename}")
            os.chdir(current_path)
            if not run_test(filename, old_source, new_source, args.no_exit):
                all_succeeded = False
        if not all_succeeded:
            exit(1)
    except:
        raise
    finally:
        os.chdir(current_path)


if __name__ == "__main__":
    main()
