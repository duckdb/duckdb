import sys
import os
import glob
from typing import Any, Generator, Optional
import shutil
import gc

script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(script_path, '..', '..', '..', 'scripts'))
from sqllogictest import (
    SQLParserException,
    SQLLogicParser,
    SQLLogicTest,
)

from sqllogictest.result import (
    TestException,
    SQLLogicRunner,
    SQLLogicDatabase,
    SQLLogicContext,
    ExecuteResult,
)

TEST_DIRECTORY_PATH = os.path.join(script_path, 'duckdb_unittest_tempdir')


# This is pretty much just a VM
class SQLLogicTestExecutor(SQLLogicRunner):
    def __init__(self, build_directory: Optional[str] = None):
        super().__init__(build_directory)
        self.SKIPPED_TESTS = set(
            [
                'test/sql/types/map/map_empty.test',
                'test/extension/wrong_function_type.test',  # <-- JSON is always loaded
                'test/sql/insert/test_insert_invalid.test',  # <-- doesn't parse properly
                'test/sql/cast/cast_error_location.test',  # <-- python exception doesn't contain error location yet
                'test/sql/pragma/test_query_log.test',  # <-- query_log gets filled with NULL when con.query(...) is used
                'test/sql/json/table/read_json_objects.test',  # <-- Python client is always loaded with JSON available
                'test/sql/copy/csv/zstd_crash.test',  # <-- Python client is always loaded with Parquet available
                'test/sql/error/extension_function_error.test',  # <-- Python client is always loaded with TPCH available
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
        self.skip_log = []

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
            except FileNotFoundError:
                pass

        # FIXME: support custom test directory
        test_delete_file(path)
        test_delete_file(path + ".wal")

    def execute_test(self, test: SQLLogicTest) -> ExecuteResult:
        self.reset()
        self.test = test
        self.original_sqlite_test = self.test.is_sqlite_test()

        # Top level keywords
        keywords = {'__TEST_DIR__': self.get_test_directory(), '__WORKING_DIRECTORY__': os.getcwd()}

        def update_value(_: SQLLogicContext) -> Generator[Any, Any, Any]:
            # Yield once to represent one iteration, do not touch the keywords
            yield None

        self.database = SQLLogicDatabase(':memory:', None)
        pool = self.database.connect()
        context = SQLLogicContext(pool, self, test.statements, keywords, update_value)
        pool.initialize_connection(context, pool.get_connection())
        # The outer context is not a loop!
        context.is_loop = False

        try:
            context.verify_statements()
            res = context.execute()
        except TestException as e:
            res = e.handle_result()
            if res.type == ExecuteResult.Type.SKIPPED:
                self.skip_log.append(str(e.message))
            else:
                print(str(e.message))

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

    arg_parser = argparse.ArgumentParser(description='Execute SQL logic tests.')
    arg_parser.add_argument('--file-path', type=str, help='Path to the test file')
    arg_parser.add_argument('--file-list', type=str, help='Path to the file containing a list of tests to run')
    arg_parser.add_argument('--start-offset', '-s', type=int, help='Start offset for the tests', default=0)
    arg_parser.add_argument(
        '--build-dir', type=str, help='Path to the build directory, used for loading extensions', default=None
    )
    args = arg_parser.parse_args()

    executor = SQLLogicTestExecutor(args.build_dir)
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

        try:
            test = sql_parser.parse(file_path)
        except SQLParserException as e:
            executor.skip_log.append(str(e.message))
            continue

        print(f'[{i}/{total_tests}] {file_path}')
        # This is necessary to clean up databases/connections
        # So previously created databases are not still cached in the instance_cache
        gc.collect()
        result = executor.execute_test(test)
        if result.type == ExecuteResult.Type.SUCCESS:
            print("SUCCESS")
        if result.type == ExecuteResult.Type.SKIPPED:
            print("SKIPPED")
            continue
        if result.type == ExecuteResult.Type.ERROR:
            print("ERROR")
            exit(1)
    if len(executor.skip_log) != 0:
        for item in executor.skip_log:
            print(item)
        executor.skip_log.clear()


if __name__ == '__main__':
    main()
