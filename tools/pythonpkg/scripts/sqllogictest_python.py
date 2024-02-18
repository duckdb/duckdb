import sys
import os
import glob
import json
from typing import Optional, List, Dict, Any
import duckdb

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
            Restart: None,  # <-- restart is hard, have to get transaction status
        }

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
            self.cursors[name] = self.conn.cursor()
        return self.cursors[name]

    def replace_keywords(self, input: str):
        # Replace environment variables in the SQL
        for name, value in self.environment_variables.items():
            input = input.replace(f"${{{name}}}", value)

        input = input.replace("__TEST_DIR__", TEST_DIRECTORY_PATH)
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
        print(sql_query)

        expected_result = query.expected_result
        assert expected_result.type == ExpectedResult.Type.SUCCES
        try:
            conn.execute(sql_query)
            result = conn.fetchall()
            print(result)
            if expected_result.lines == None:
                return
            actual = []
            for row in result:
                converted_row = tuple([str(x) for x in list(row)])
                actual.append(converted_row)
            actual = str(actual)

            query_result = QueryResult(result)
        except Exception as e:
            query_result = QueryResult([], e)
        compare_result = self.check_query_result(query, query_result)

    def execute_statement(self, statement: Statement):
        assert isinstance(statement, Statement)
        conn = self.get_connection(statement.connection_name)
        sql_query = '\n'.join(statement.lines)
        print(sql_query)

        expected_result = statement.expected_result
        try:
            conn.execute(sql_query)
            result = conn.fetchall()
            if len(result) > 0:
                print(result[0])
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

    def execute_require_env(self, statement: BaseStatement):
        self.skipped = True

    def execute_test(self, test: SQLLogicTest) -> ExecuteResult:
        self.reset()
        self.test = test
        self.original_sqlite_test = self.test.is_sqlite_test()
        unsupported = self.get_unsupported_statements(test)
        if unsupported != []:
            error = 'Test skipped because the following statement types are not supported: '
            types = set([x.__class__ for x in unsupported])
            error += str(list([x.__name__ for x in types]))
            raise Exception(error)

        self.load_database(self.dbpath)
        for statement in test.statements:
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

        result = executor.execute_test(test)
        print(result.type.name)
        if result.type == ExecuteResult.Type.SKIPPED:
            continue
        exit()


if __name__ == '__main__':
    main()
