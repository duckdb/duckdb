import sys
import os
import glob
import json
from typing import Optional, List, Dict, Any
import duckdb
from enum import Enum

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
    Skip,
    Unskip,
    ExpectedResult,
)

from sqllogictest.result import SQLLogicRunner, QueryResult

from enum import Enum, auto

TEST_DIRECTORY_PATH = os.path.join(script_path, 'duckdb_unittest_tempdir')


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
            Restart: None,  # <-- restart is hard, have to get transaction status
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

    def fail(self, message):
        raise Exception(message)

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
        # for extension in extensions:
        #    self.load_extension(db, extension)

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
            conn.execute(sql_query)
            result = conn.fetchall()
            try:
                conn.execute("BEGIN TRANSACTION")
                rel = conn.query(f'{sql_query}')
                if rel != None:
                    types = rel.types
                else:
                    types = []
                conn.execute("ABORT")
            except Exception as e:
                types = []
            if expected_result.lines == None:
                return
            actual = []
            for row in result:
                converted_row = tuple([str(x) for x in list(row)])
                actual.append(converted_row)
            actual = str(actual)

            query_result = QueryResult(result, types)
        except Exception as e:
            query_result = QueryResult([], [], e)

        print(sql_query)
        compare_result = self.check_query_result(query, query_result)
        if not compare_result:
            self.fail(f'Failed: {self.test.path}:{query.get_query_line()}')
            # if context.is_parallel:
            #    self.finished_processing_file = True
            #    context.error_file = file_name
            #    context.error_line = query_line
            # else:
            #    fail_line(file_name, query_line, 0)

    def execute_skip(self, statement: Skip):
        self.skip()

    def execute_unskip(self, statement: Unskip):
        self.unskip()

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
            query = f"""
                LOAD '__BUILD_DIRECTORY__/test/extension/{param}/{param}.duckdb_extension';
            """
            query = self.replace_keywords(query)
            try:
                self.con.execute(query)
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

        result = executor.execute_test(test)
        print(result.type.name)
        if result.type == ExecuteResult.Type.SKIPPED:
            continue


if __name__ == '__main__':
    main()
