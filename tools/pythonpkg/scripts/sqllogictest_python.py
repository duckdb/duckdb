import sys
import os
import glob
import json
from typing import Optional, List, Dict, Any
import duckdb
from enum import Enum
import time

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

from sqllogictest.result import SQLLogicRunner, QueryResult

from enum import Enum, auto

TEST_DIRECTORY_PATH = os.path.join(script_path, 'duckdb_unittest_tempdir')


class SkipException(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)


class ExecuteResult:
    class Type(Enum):
        SUCCES = 0
        ERROR = 1
        SKIPPED = 2

    def __init__(self, type: "ExecuteResult.Type"):
        self.type = type


class SQLLogicTestExecutor(SQLLogicRunner):
    def __init__(self):
        super().__init__()
        self.STATEMENTS = {
            Statement: self.execute_statement,
            RequireEnv: self.execute_require_env,
            Query: self.execute_query,
            Load: self.execute_load,
            Require: self.execute_require,
            Skip: self.execute_skip,
            Unskip: self.execute_unskip,
            Mode: self.execute_mode,
            Sleep: self.execute_sleep,
            Reconnect: self.execute_reconnect,
            # Restart: None,  # <-- restart is hard, have to get transaction status
        }
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

    def get_unsupported_statements(self, test: SQLLogicTest) -> List[BaseStatement]:
        unsupported_statements = [
            statement for statement in test.statements if statement.__class__ not in self.STATEMENTS
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

    def replace_keywords(self, input: str):
        # Replace environment variables in the SQL
        for name, value in self.environment_variables.items():
            input = input.replace(f"${{{name}}}", value)

        input = input.replace("__TEST_DIR__", self.get_test_directory())
        input = input.replace("__WORKING_DIRECTORY__", os.getcwd())
        input = input.replace("__BUILD_DIRECTORY__", duckdb.__build_dir__)
        return input

    def in_loop(self) -> bool:
        return False

    def skiptest(self, message: str):
        raise SkipException(message)

    def test_delete_file(self, path):
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass

    def delete_database(self, path):
        if False:
            return
        self.test_delete_file(path)
        self.test_delete_file(path + ".wal")

    def reconnect(self):
        self.con = self.db.cursor()
        if self.test.is_sqlite_test():
            self.con.execute("SET integer_division=true")
        # Check for alternative verify
        # if DUCKDB_ALTERNATIVE_VERIFY:
        #    con.query("SET pivot_filter_threshold=0")
        # if enable_verification:
        #    con.enable_query_verification()
        # Set the local extension repo for autoinstalling extensions
        env_var = os.getenv("LOCAL_EXTENSION_REPO")
        if env_var:
            self.con.execute("set autoload_known_extensions=True")
            self.con.execute(f"SET autoinstall_extension_repository='{env_var}'")

    def load_extension(self, db: duckdb.DuckDBPyConnection, extension: str):
        root = duckdb.__build_dir__
        path = os.path.join(root, "extension", extension, f"{extension}.duckdb_extension")
        # Serialize it as a POSIX compliant path
        path = path.as_posix()
        db.execute(f"LOAD {path}")

    def load_database(self, dbpath):
        self.dbpath = dbpath

        # Restart the database with the specified db path
        self.db = ''
        self.con = None
        self.cursors = {}

        # Now re-open the current database
        read_only = 'access_mode' in self.config and self.config['access_mode'] == 'read_only'
        self.db = duckdb.connect(dbpath, read_only, self.config)
        self.loaded_databases[dbpath] = self.db
        self.reconnect()

        # Load any previously loaded extensions again
        for extension in self.extensions:
            self.load_extension(self.db, extension)

    def execute_load(self, load: Load):
        if self.in_loop():
            self.fail("load cannot be called in a loop")

        readonly = load.readonly

        if load.header.parameters:
            dbpath = load.header.parameters[0]
            dbpath = self.replace_keywords(dbpath)
            if not readonly:
                # delete the target database file, if it exists
                self.delete_database(dbpath)
        else:
            dbpath = ""

        # set up the config file
        if readonly:
            self.config['temp_directory'] = False
            self.config['access_mode'] = 'read_only'
        else:
            self.config['temp_directory'] = True
            self.config['access_mode'] = 'automatic'

        # now create the database file
        self.load_database(dbpath)

    def execute_query(self, query: Query):
        assert isinstance(query, Query)
        conn = self.get_connection(query.connection_name)
        sql_query = '\n'.join(query.lines)
        sql_query = self.replace_keywords(sql_query)

        expected_result = query.expected_result
        assert expected_result.type == ExpectedResult.Type.SUCCES

        try:
            statements = conn.extract_statements(sql_query)
            statement = statements[-1]
            expected_result_types = statement.expected_result_type
            if duckdb.ExpectedResultType.QUERY_RESULT in expected_result_types:
                original_rel = conn.query(sql_query)
                original_types = original_rel.types
                aliased_columns = ", ".join([f'c{i}' for i in range(len(original_types))])
                try:
                    stringified_rel = conn.query(
                        f"select COLUMNS(*)::VARCHAR from original_rel unnamed_subquery_blabla({aliased_columns})"
                    )
                except Exception as e:
                    self.fail(f"Could not select from the ValueRelation: {str(e)}")
                result = stringified_rel.fetchall()
                query_result = QueryResult(result, original_types)
            else:
                conn.execute(sql_query)
                result = conn.fetchall()
                query_result = QueryResult(result, [])
            if expected_result.lines == None:
                return
        except SkipException as e:
            self.skipped = True
            return
        except Exception as e:
            print(e)
            query_result = QueryResult([], [], e)

        self.check_query_result(query, query_result)

    def execute_skip(self, statement: Skip):
        self.skip()

    def execute_unskip(self, statement: Unskip):
        self.unskip()

    def execute_reconnect(self, statement: Reconnect):
        # if self.is_parallel:
        #   raise Error(...)
        self.reconnect()

    def execute_sleep(self, statement: Sleep):
        def calculate_sleep_time(duration: float, unit: SleepUnit) -> float:
            if unit == SleepUnit.SECOND:
                return duration
            elif unit == SleepUnit.MILLISECOND:
                return duration / 1000
            elif unit == SleepUnit.MICROSECOND:
                return duration / 1000000
            elif unit == SleepUnit.NANOSECOND:
                return duration / 1000000000
            else:
                raise ValueError("Unknown sleep unit")

        unit = statement.get_unit()
        duration = statement.get_duration()

        time_to_sleep = calculate_sleep_time(duration, unit)
        time.sleep(time_to_sleep)

    def execute_mode(self, statement: Mode):
        parameter = statement.header.parameters[0]
        if parameter == "output_hash":
            self.output_hash_mode = True
        elif parameter == "output_result":
            self.output_result_mode = True
        elif parameter == "no_output":
            self.output_hash_mode = False
            self.output_result_mode = False
        elif parameter == "debug":
            self.debug_mode = True
        else:
            raise RuntimeError("unrecognized mode: " + parameter)

    def execute_statement(self, statement: Statement):
        assert isinstance(statement, Statement)
        conn = self.get_connection(statement.connection_name)
        sql_query = '\n'.join(statement.lines)
        sql_query = self.replace_keywords(sql_query)
        print(sql_query)

        expected_result = statement.expected_result
        try:
            conn.execute(sql_query)
            result = conn.fetchall()
            if expected_result.type == ExpectedResult.Type.ERROR:
                self.fail(f"Query unexpectedly succeeded")
            assert expected_result.lines == None
        except duckdb.Error as e:
            if expected_result.type == ExpectedResult.Type.SUCCES:
                self.fail(f"Query unexpectedly failed: {str(e)}")
            if expected_result.lines == None:
                return
            expected = '\n'.join(expected_result.lines)
            if expected not in str(e):
                self.fail(
                    f"Query failed, but did not produce the right error: {expected}\nInstead it produced: {str(e)}"
                )

    class RequireResult(Enum):
        MISSING = 0
        PRESENT = 1

    def check_require(self, statement: Require) -> RequireResult:
        not_an_extension = [
            "notmingw",
            "mingw",
            "notwindows",
            "windows",
            "longdouble",
            "64bit",
            "noforcestorage",
            "nothreadsan",
            "strinline",
            "vector_size",
            "exact_vector_size",
            "block_size",
            "skip_reload",
            "noalternativeverify",
        ]
        param = statement.header.parameters[0].lower()
        if param in not_an_extension:
            return self.RequireResult.MISSING

        if param == "no_extension_autoloading":
            if 'autoload_known_extensions' in self.config:
                # If autoloading is on, we skip this test
                return self.RequireResult.MISSING
            return self.RequireResult.PRESENT

        excluded_from_autoloading = True
        for ext in self.AUTOLOADABLE_EXTENSIONS:
            if ext == param:
                excluded_from_autoloading = False
                break

        if not 'autoload_known_extensions' in self.config:
            try:
                self.load_extension(self.con, param)
                self.extensions.add(param)
            except:
                return self.RequireResult.MISSING
        elif excluded_from_autoloading:
            return self.RequireResult.MISSING

        return self.RequireResult.PRESENT

    def execute_require(self, statement: Require):
        require_result = self.check_require(statement)
        if require_result == self.RequireResult.MISSING:
            param = statement.header.parameters[0].lower()
            if self.is_required(param):
                # This extension / setting was explicitly required
                self.fail("require {}: FAILED".format(param))
            self.skipped = True

    def execute_require_env(self, statement: BaseStatement):
        self.skipped = True

    # TODO: this does not support parallel execution
    # We likely need to add another method inbetween that takes a list of statements to execute
    # This method should be defined on a SQLLogicContext, to support parallelism
    def execute_test(self, test: SQLLogicTest) -> ExecuteResult:
        self.reset()
        self.test = test
        self.original_sqlite_test = self.test.is_sqlite_test()
        unsupported = self.get_unsupported_statements(test)
        if unsupported != []:
            error = f'Test {test.path} skipped because the following statement types are not supported: '
            types = set([x.__class__ for x in unsupported])
            error += str(list([x.__name__ for x in types]))
            raise Exception(error)

        self.load_database(self.dbpath)
        for statement in test.statements:
            if self.skip_active() and statement.__class__ != Unskip:
                # Keep skipping until Unskip is found
                continue
            method = self.STATEMENTS.get(statement.__class__)
            if not method:
                raise Exception(f"Not supported: {statement.__class__.__name__}")
            print(statement.header.type.name)
            method(statement)
            if self.skipped:
                return ExecuteResult(ExecuteResult.Type.SKIPPED)
        return ExecuteResult(ExecuteResult.Type.SUCCES)


import argparse


def main():
    sql_parser = SQLLogicParser()
    executor = SQLLogicTestExecutor()

    arg_parser = argparse.ArgumentParser(description='Execute SQL logic tests.')
    arg_parser.add_argument('--file-path', '-f', type=str, help='Path to the test file')
    args = arg_parser.parse_args()

    if args.file_path:
        file_paths = [args.file_path]
    else:
        test_directory = os.path.join(script_path, '..', '..', '..', 'test')
        file_paths = glob.iglob(test_directory + '/**/*.test', recursive=True)

    for file_path in file_paths:
        print(file_path)
        test = sql_parser.parse(file_path)
        if not test:
            print(f'Failed to parse {file_path}')
            exit(1)
        try:
            result = executor.execute_test(test)
        except SkipException as e:
            continue
        except Exception as e:
            if 'skipped because the following statement types are not supported' in str(e):
                continue
            raise e
        print(result.type.name)
        if result.type == ExecuteResult.Type.SKIPPED:
            continue


if __name__ == '__main__':
    main()
