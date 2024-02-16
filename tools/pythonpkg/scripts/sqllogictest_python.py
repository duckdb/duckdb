import sys
import os
import glob
import json
from typing import Optional, List
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

from enum import Enum, auto


class ExecuteResult:
    class Type(Enum):
        SUCCES = (auto(),)
        ERROR = (auto(),)
        SKIPPED = auto()

    def __init__(self, type: "ExecuteResult.Type"):
        self.type = type


class SQLLogicTestExecutor:
    def reset(self):
        self.skipped = False
        self.error: Optional[str] = None
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.cursors = {}

    def __init__(self):
        self.reset()
        self.STATEMENTS = {
            Statement: self.execute_statement,
            RequireEnv: self.execute_require_env,
            Query: self.execute_query,
        }

    def fail(self, message):
        raise Exception(message)

    def get_unsupported_statements(self, test: SQLLogicTest) -> List[BaseStatement]:
        unsupported_statements = [
            statement for statement in test.statements if statement.__class__ not in self.STATEMENTS
        ]
        return unsupported_statements

    def get_connection(self, name: Optional[str] = None) -> duckdb.DuckDBPyConnection:
        if not self.conn:
            self.conn = duckdb.connect(':memory:__named__')
        if not name:
            return self.conn
        if name not in self.cursors:
            self.cursors[name] = self.conn.cursor()
        return self.cursors[name]

    def execute_query(self, query: Query):
        assert isinstance(query, Query)
        conn = self.get_connection(query.connection_name)
        sql_query = '\n'.join(query.lines)

        expected_result = query.expected_result
        assert expected_result.type == ExpectedResult.Type.SUCCES
        try:
            conn.execute(sql_query)
            result = conn.fetchall()
            if expected_result.lines == None:
                return
            actual = []
            for row in result:
                converted_row = tuple([str(x) for x in list(row)])
                actual.append(converted_row)
            actual = str(actual)

            expected = []
            for line in expected_result.lines:
                tuples = tuple(line.split('\t'))
                expected.append(tuples)
            expected = str(expected)
            if expected != actual:
                self.fail(f"Query result mismatch!")
        except duckdb.Error as e:
            self.fail(f"Query unexpectedly failed: {str(e)}")

    def execute_statement(self, statement: Statement):
        assert isinstance(statement, Statement)
        conn = self.get_connection(statement.connection_name)
        query = '\n'.join(statement.lines)

        expected_result = statement.expected_result
        try:
            conn.execute(query)
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

    def execute_require_env(self, statement: BaseStatement):
        self.skipped = True

    def execute_test(self, test: SQLLogicTest) -> ExecuteResult:
        self.reset()
        unsupported = self.get_unsupported_statements(test)
        if unsupported != []:
            error = 'Test skipped because the following statement types are not supported: '
            types = set([x.__class__ for x in unsupported])
            error += str(list([x.__name__ for x in types]))
            raise Exception(error)

        for statement in test.statements:
            method = self.STATEMENTS.get(statement.__class__)
            if not method:
                raise Exception(f"Not supported: {statement.__class__.__name__}")
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
        if result.type == ExecuteResult.Type.SKIPPED:
            continue
        exit()


if __name__ == '__main__':
    main()
