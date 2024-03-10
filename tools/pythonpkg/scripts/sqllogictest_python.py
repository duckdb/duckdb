import sys
import os
import glob
from typing import Any, Generator
import duckdb
import shutil
import gc

script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(script_path, '..', '..', '..', 'scripts'))
from sqllogictest import (
    SQLLogicParser,
    SQLLogicTest,
)

from sqllogictest.result import (
    TestException,
    SQLLogicRunner,
    SQLLogicDatabase,
    SQLLogicContext,
    ExecuteResult,
    SkipException,
)

TEST_DIRECTORY_PATH = os.path.join(script_path, 'duckdb_unittest_tempdir')

### TODO
# - Clean up the 'error_message', 'error_line', etc.. structure, probably move it into a class
#   then we can streamline the parallel/non-parallel SQLLogicContext uses
# - Properly register skipped tests / errored tests
# - Clean up the exception handling
#   'except:' and 'except as Exception:' are much too broad, these should be narrowed
# - Split up the logic, ideally a good portion of this should be language-agnostic
#   so we can reuse much of this to build a codegen solution for other languages
# - Properly implement the rest of the 'require' statement
#   currently we skip on every require that is not an extension


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

        def update_value(_: SQLLogicContext) -> Generator[Any, Any, Any]:
            # Yield once to represent one iteration, do not touch the keywords
            yield None

        self.database = SQLLogicDatabase(':memory:', list())
        context = SQLLogicContext(self.database.connect(), self, test.statements, keywords, update_value)
        # The outer context is not a loop!
        context.is_loop = False

        try:
            context.verify_statements()
            res = context.execute()
        except TestException as e:
            res = e.handle_result()

        self.database.reset()

        # Clean up any databases that we created
        for loaded_path in self.loaded_databases:
            if not loaded_path:
                continue
            # Only delete database files that were created during the tests
            if not loaded_path.startswith(self.get_test_directory()):
                continue
            os.remove(loaded_path)
        return res


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
        if file_path in executor.SKIPPED_TESTS:
            continue
        if i < start_offset:
            continue
        if test_directory:
            file_path = os.path.join(test_directory, file_path)
        test = sql_parser.parse(file_path)
        if not test:
            print(f'Failed to parse {file_path}')
            exit(1)
        print(f'[{i}/{total_tests}] {file_path}')
        # This is necessary to clean up databases/connections
        # So previously created databases are not still cached in the instance_cache
        gc.collect()
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
