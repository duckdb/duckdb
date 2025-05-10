import gc
import os
import pathlib
import pytest
import signal
import sys
from typing import Any, Generator, Optional

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


def sigquit_handler(signum, frame):
    # Access the executor from the test_sqllogic function
    if hasattr(test_sqllogic, 'executor') and test_sqllogic.executor:
        if test_sqllogic.executor.database and hasattr(test_sqllogic.executor.database, 'connection'):
            test_sqllogic.executor.database.connection.interrupt()
        test_sqllogic.executor.cleanup()
        test_sqllogic.executor = None
    # Re-raise the signal to let the default handler take over
    signal.signal(signal.SIGQUIT, signal.default_int_handler)
    os.kill(os.getpid(), signal.SIGQUIT)


# Register the SIGQUIT handler
signal.signal(signal.SIGQUIT, sigquit_handler)


class SQLLogicTestExecutor(SQLLogicRunner):
    def __init__(self, test_directory: str, build_directory: Optional[str] = None):
        super().__init__(build_directory)
        self.test_directory = test_directory
        # TODO: get this from the `duckdb` package
        self.AUTOLOADABLE_EXTENSIONS = [
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
        return self.test_directory

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
        try:
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
        except KeyboardInterrupt:
            if self.database:
                self.database.interrupt()
            raise

    def cleanup(self):
        if self.database:
            if hasattr(self.database, 'connection'):
                self.database.connection.interrupt()
            self.database.reset()
            self.database = None
        # Clean up any remaining test databases
        for loaded_path in self.loaded_databases:
            if loaded_path and loaded_path.startswith(self.get_test_directory()):
                try:
                    os.remove(loaded_path)
                except FileNotFoundError:
                    pass


def test_sqllogic(test_script_path: pathlib.Path, pytestconfig: pytest.Config, tmp_path: pathlib.Path):
    gc.collect()
    sql_parser = SQLLogicParser()
    try:
        test = sql_parser.parse(str(test_script_path))
    except SQLParserException as e:
        pytest.skip("Failed to parse SQLLogic script: " + str(e.message))
        return

    build_dir = pytestconfig.getoption("build_dir")
    executor = SQLLogicTestExecutor(str(tmp_path), build_dir)
    # Store executor in the function's arguments so it can be accessed by the interrupt handler
    test_sqllogic.executor = executor
    try:
        result = executor.execute_test(test)
        assert result.type == ExecuteResult.Type.SUCCESS
    finally:
        executor.cleanup()
        test_sqllogic.executor = None


if __name__ == '__main__':
    # Pass all arguments including the script name to pytest
    sys.exit(pytest.main(sys.argv))
