import sys
import os
import glob
import json
from typing import Optional, List, Dict, Any, Generator
import duckdb
from enum import Enum
import time
import shutil

script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(script_path, '..', '..', '..', 'scripts'))
from sqllogictest import (
    SQLLogicParser,
    SQLLogicEncoder,
    SQLLogicTest,
    BaseStatement,
    Statement,
    Require,
    Mode,
    Halt,
    Set,
    Load,
    Query,
    HashThreshold,
    Loop,
    Foreach,
    Endloop,
    RequireEnv,
    Restart,
    Reconnect,
    Sleep,
    SleepUnit,
    Skip,
    Unskip,
    ExpectedResult,
)

from sqllogictest.result import (
    SQLLogicRunner,
    SQLLogicContext,
    RequireResult,
    ExecuteResult,
    SkipException,
    QueryResult,
)

from enum import Enum, auto

TEST_DIRECTORY_PATH = os.path.join(script_path, 'duckdb_unittest_tempdir')


# This is pretty much just a VM
class SQLLogicTestExecutor(SQLLogicRunner):
    def __init__(self):
        super().__init__()
        self.SKIPPED_TESTS = set(
            [
                'test/sql/types/map/map_empty.test',
                'test/sql/types/nested/list/test_list_slice_step.test',  # <-- skipping because it causes an InternalException currently
                'test/sql/insert/test_insert_invalid.test',  # <-- doesn't parse properly
                'test/sql/cast/cast_error_location.test',  # <-- python exception doesn't contain error location yet
                'test/sql/pragma/test_query_log.test',  # <-- query_log gets filled with NULL when con.query(...) is used
                'test/sql/function/list/lambdas/transform_with_index.test',  # <-- same InternalException
                'test/sql/function/list/lambdas/transform.test',  # <-- same InternalException
                'test/sql/function/list/lambdas/filter.test',  # <-- same InternalException
                'test/sql/function/list/lambdas/reduce.test',  # <-- same InternalException
                'test/sql/function/list/list_resize.test',  # <-- same InternalException
                'test/sql/function/list/aggregates/null_or_empty.test',  # <-- same InternalException
                'test/sql/json/table/read_json_objects.test',  # <-- Python client is always loaded with JSON available
                'test/sql/copy/csv/zstd_crash.test',  # <-- Python client is always loaded with Parquet available
                'test/sql/error/extension_function_error.test',  # <-- Python client is always loaded with TPCH available
                'test/sql/types/timestamp/test_timestamp_tz.test',  # <-- Python client is always loaded wih ICU available - making the TIMESTAMPTZ::DATE cast pass
                'test/sql/parser/invisible_spaces.test',  # <-- Parser is getting tripped up on the invisible spaces
                'test/sql/copy/csv/code_cov/csv_state_machine_invalid_utf.test',  # <-- ConversionException is empty, see Python Mega Issue (duckdb-internal #1488)
            ]
        )
        # TODO: get this from the `duckdb` package
        self.AUTOLOADABLE_EXTENSIONS = [
            "arrow",
            "aws",
            "autocomplete",
            "excel",
            "fts",
            "httpfs",
            "json",
            "parquet",
            "postgres_scanner",
            "sqlsmith",
            "sqlite_scanner",
            "tpcds",
            "tpch",
            # "azure",
            # "inet",
            # "icu",
            # "spatial",
            # TODO: table function isnt always autoloaded so test fails
        ]

    def get_unsupported_statements(self, context: SQLLogicContext, test: SQLLogicTest) -> List[BaseStatement]:
        unsupported_statements = [
            statement for statement in test.statements if statement.__class__ not in context.STATEMENTS
        ]
        return unsupported_statements

    def get_connection(self, name: Optional[str] = None) -> duckdb.DuckDBPyConnection:
        if not name:
            return self.con

        if name not in self.cursors:
            self.cursors[name] = self.con.cursor()
        return self.cursors[name]

    def get_test_directory(self) -> str:
        test_directory = TEST_DIRECTORY_PATH
        if not os.path.exists(test_directory):
            os.makedirs(test_directory)
        return test_directory

    def delete_database(self, path):
        def test_delete_file(path):
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass

        # FIXME: support custom test directory
        test_delete_file(path)
        test_delete_file(path + ".wal")

    def reconnect(self):
        self.con = self.db.cursor()
        if self.test.is_sqlite_test():
            self.con.execute("SET integer_division=true")
        if 'icu' in self.extensions:
            self.con.query("SET timezone='UTC'")
        # Check for alternative verify
        # if DUCKDB_ALTERNATIVE_VERIFY:
        #    con.query("SET pivot_filter_threshold=0")
        # if enable_verification:
        #    con.enable_query_verification()
        # Set the local extension repo for autoinstalling extensions
        env_var = os.getenv("LOCAL_EXTENSION_REPO")
        if env_var:
            self.con.execute("SET autoload_known_extensions=True")
            self.con.execute(f"SET autoinstall_extension_repository='{env_var}'")

    # TODO: this does not support parallel execution
    # We likely need to add another method inbetween that takes a list of statements to execute
    # This method should be defined on a SQLLogicContext, to support parallelism
    def execute_test(self, test: SQLLogicTest) -> ExecuteResult:
        self.reset()
        self.test = test
        self.original_sqlite_test = self.test.is_sqlite_test()

        # Top level keywords
        keywords = {
            '__TEST_DIR__': self.get_test_directory(),
            '__WORKING_DIRECTORY__': os.getcwd(),
            '__BUILD_DIRECTORY__': duckdb.__build_dir__,
        }

        def update_value(keywords: Dict[str, str]) -> Generator[Any, Any, Any]:
            # Yield once to represent one iteration, do not touch the keywords
            yield None

        context = SQLLogicContext(self, test.statements, keywords, update_value)
        unsupported = self.get_unsupported_statements(context, test)
        if unsupported != []:
            error = f'Test {test.path} skipped because the following statement types are not supported: '
            types = set([x.__class__ for x in unsupported])
            error += str(list([x.__name__ for x in types]))
            raise Exception(error)

        self.load_database(self.dbpath)
        return context.execute()


import argparse


def main():
    sql_parser = SQLLogicParser()
    executor = SQLLogicTestExecutor()

    arg_parser = argparse.ArgumentParser(description='Execute SQL logic tests.')
    arg_parser.add_argument('--file-path', type=str, help='Path to the test file')
    arg_parser.add_argument('--file-list', type=str, help='Path to the file containing a list of tests to run')
    arg_parser.add_argument('--start-offset', '-s', type=int, help='Start offset for the tests', default=0)
    args = arg_parser.parse_args()

    if os.path.exists(TEST_DIRECTORY_PATH):
        shutil.rmtree(TEST_DIRECTORY_PATH)

    test_directory = None
    if args.file_path:
        if args.file_list:
            raise Exception("Can not provide both a file-path and a file-list")
        file_paths = [args.file_path]
    elif args.file_list:
        if args.file_path:
            raise Exception("Can not provide both a file-path and a file-list")
        file_paths = open(args.file_list).read().split()
    else:
        test_directory = os.path.join(script_path, '..', '..', '..')
        file_paths = glob.iglob(test_directory + '/test/**/*.test', recursive=True)
        file_paths = [os.path.relpath(path, test_directory) for path in file_paths]

    start_offset = args.start_offset

    total_tests = len(file_paths)
    for i, file_path in enumerate(file_paths):
        print(f'[{i}/{total_tests}] {file_path}')
        if file_path in executor.SKIPPED_TESTS:
            print(file_path)
            continue
        if test_directory:
            file_path = os.path.join(test_directory, file_path)
        test = sql_parser.parse(file_path)
        if i < start_offset:
            continue
        if not test:
            print(f'Failed to parse {file_path}')
            exit(1)
        try:
            result = executor.execute_test(test)
        except SkipException as e:
            print(e)
            continue
        except Exception as e:
            print(e)
            if 'skipped because the following statement types are not supported' in str(e):
                continue
            raise e
        print(result.type.name)
        if result.type == ExecuteResult.Type.SKIPPED:
            continue


if __name__ == '__main__':
    main()
