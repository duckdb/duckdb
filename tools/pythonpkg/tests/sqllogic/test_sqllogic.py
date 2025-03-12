import sys
import os
import pathlib
import pytest
from typing import Any, Generator, Optional
import gc

##### Copied from sqllogictest_python.py #####

script_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(script_path, '..', '..', '..', '..', 'scripts'))
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
                pytest.skip(str(e.message))
            else:
                pytest.fail(str(e.message), pytrace=False)

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


def test_sqllogic(test_script_path: pathlib.Path):
    gc.collect()
    sql_parser = SQLLogicParser()
    try:
        test = sql_parser.parse(str(test_script_path))
    except SQLParserException as e:
        pytest.skip("Failed to parse SQLLogic script: " + str(e.message))
        return

    executor = SQLLogicTestExecutor()
    result = executor.execute_test(test)
    assert result.type == ExecuteResult.Type.SUCCESS


if __name__ == '__main__':
    # Pass all arguments including the script name to pytest
    sys.exit(pytest.main(sys.argv))
