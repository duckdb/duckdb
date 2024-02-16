import os

from enum import Enum, auto
from typing import List, Dict, Optional
import json

from sqllogic_parser import (
    Token,
    TokenType,
    BaseStatement,
    Statement,
    Require,
    NoOp,
    Mode,
    Halt,
    Set,
    Load,
    SkipIf,
    OnlyIf,
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
)

# TODO: add 'dbpath' with argparse


def create_formatted_list(items) -> str:
    res = ''
    for i, option in enumerate(items):
        if i + 1 == len(items):
            spacer = ' or '
        elif i != 0:
            spacer = ', '
        else:
            spacer = ''
        res += f"{spacer}'{option}'"
    return res


class SortStyle(Enum):
    NO_SORT = (auto(),)
    ROW_SORT = (auto(),)
    VALUE_SORT = (auto(),)
    UNKNOWN = auto()


class SleepUnit(Enum):
    SECOND = auto()
    MILLISECOND = auto()
    MICROSECOND = auto()
    NANOSECOND = auto()


def get_sleep_unit(unit):
    seconds = ["second", "seconds", "sec"]
    miliseconds = ["millisecond", "milliseconds", "milli"]
    microseconds = ["microsecond", "microseconds", "micro"]
    nanoseconds = ["nanosecond", "nanoseconds", "nano"]
    if unit in seconds:
        return SleepUnit.SECOND
    elif unit in miliseconds:
        return SleepUnit.MILLISECOND
    elif unit in microseconds:
        return SleepUnit.MICROSECOND
    elif unit in nanoseconds:
        return SleepUnit.NANOSECOND
    else:
        options = ['second', 'millisecond', 'microsecond', 'nanosecond']
        raise RuntimeError(f"Unrecognized sleep mode - expected {create_formatted_list(options)}")


class ExpectedResult:
    class Type(Enum):
        SUCCES = (auto(),)
        ERROR = (auto(),)
        UNKNOWN = auto()

    def __init__(self, type: "ExpectedResult.Type"):
        self.type = type
        self.lines: Optional[List[str]] = None

    def add_lines(self, lines: List[str]):
        self.lines = lines


class SQLLogicTest:
    def __init__(self, path):
        self.path = path
        self.statements = []

    def add_statement(self, statement: BaseStatement):
        self.statements.append(statement)

    def is_sqlite_test(self):
        return 'test/sqlite/select' in self.path or 'third_party/sqllogictest' in self.path


### -------- JSON ENCODER ----------


class SQLLogicEncoder(json.JSONEncoder):
    def encode_base(self, base: BaseStatement):
        return {'type': base.header.type.name, 'parameters': base.header.parameters, 'query_line': base.query_line}

    def encode_expected_lines(self, expected: ExpectedResult):
        if expected.lines != None:
            return {'lines': expected.lines}
        else:
            return {}

    def default(self, obj):
        if isinstance(obj, ExpectedResult):
            return {'type': obj.type.name, **self.encode_expected_lines(obj)}
        if isinstance(obj, SQLLogicTest):
            return {'path': obj.path, 'statements': [x for x in obj.statements]}
        if isinstance(obj, Statement):
            assert obj.header.type == TokenType.SQLLOGIC_STATEMENT, "Object is not an instance of Statement"
            return {
                **self.encode_base(obj),
                'lines': obj.lines,
                'expected_result': obj.expected_result,
            }
        elif isinstance(obj, Query):
            assert obj.header.type == TokenType.SQLLOGIC_QUERY, "Object is not an instance of Query"
            return {
                **self.encode_base(obj),
                'lines': obj.lines,
                'expected_result': obj.expected_result,
            }
        elif isinstance(obj, Require):
            assert obj.header.type == TokenType.SQLLOGIC_REQUIRE, "Object is not an instance of Require"
            return {
                **self.encode_base(obj),
            }
        elif isinstance(obj, SkipIf):
            assert obj.header.type == TokenType.SQLLOGIC_SKIP_IF, "Object is not an instance of SkipIf"
            return {
                **self.encode_base(obj),
            }
        elif isinstance(obj, OnlyIf):
            assert obj.header.type == TokenType.SQLLOGIC_ONLY_IF, "Object is not an instance of OnlyIf"
            return {
                **self.encode_base(obj),
            }
        elif isinstance(obj, HashThreshold):
            assert obj.header.type == TokenType.SQLLOGIC_HASH_THRESHOLD, "Object is not an instance of HashThreshold"
            return {
                **self.encode_base(obj),
            }
        elif isinstance(obj, Halt):
            assert obj.header.type == TokenType.SQLLOGIC_HALT, "Object is not an instance of Halt"
            return {
                **self.encode_base(obj),
            }
        elif isinstance(obj, Mode):
            assert obj.header.type == TokenType.SQLLOGIC_MODE, "Object is not an instance of Mode"
            return {
                **self.encode_base(obj),
            }
        elif isinstance(obj, Skip):
            assert obj.header.type == TokenType.SQLLOGIC_MODE, "Object is not an instance of Skip"
            return {
                **self.encode_base(obj),
            }
        elif isinstance(obj, Unskip):
            assert obj.header.type == TokenType.SQLLOGIC_MODE, "Object is not an instance of Unskip"
            return {
                **self.encode_base(obj),
            }
        elif isinstance(obj, Set):
            assert obj.header.type == TokenType.SQLLOGIC_SET, "Object is not an instance of Set"
            return {
                **self.encode_base(obj),
            }
        elif isinstance(obj, Loop):
            type = obj.header.type
            assert (
                type == TokenType.SQLLOGIC_LOOP or type == TokenType.SQLLOGIC_CONCURRENT_LOOP
            ), "Object is not an instance of Loop"
            return {
                **self.encode_base(obj),
                'parallel': obj.parallel,
                'name': obj.name,
                'start': obj.start,
                'end': obj.end,
            }
        elif isinstance(obj, Foreach):
            type = obj.header.type
            assert (
                type == TokenType.SQLLOGIC_FOREACH or type == TokenType.SQLLOGIC_CONCURRENT_FOREACH
            ), "Object is not an instance of Foreach"
            return {**self.encode_base(obj), 'parallel': obj.parallel, 'name': obj.name, 'values': obj.values}
        elif isinstance(obj, Endloop):
            assert obj.header.type == TokenType.SQLLOGIC_ENDLOOP, "Object is not an instance of Endloop"
            return {
                **self.encode_base(obj),
            }
        elif isinstance(obj, RequireEnv):
            assert obj.header.type == TokenType.SQLLOGIC_REQUIRE_ENV, "Object is not an instance of RequireEnv"
            return {
                **self.encode_base(obj),
            }
        elif isinstance(obj, Load):
            assert obj.header.type == TokenType.SQLLOGIC_LOAD, "Object is not an instance of Load"
            return {
                **self.encode_base(obj),
            }
        elif isinstance(obj, Restart):
            assert obj.header.type == TokenType.SQLLOGIC_RESTART, "Object is not an instance of Restart"
            return {
                **self.encode_base(obj),
            }
        elif isinstance(obj, Reconnect):
            assert obj.header.type == TokenType.SQLLOGIC_RECONNECT, "Object is not an instance of Reconnect"
            return {
                **self.encode_base(obj),
            }
        elif isinstance(obj, Sleep):
            assert obj.header.type == TokenType.SQLLOGIC_SLEEP, "Object is not an instance of Sleep"
            return {
                **self.encode_base(obj),
            }
        else:
            raise Exception(f"Invalid TokenType ({obj.header.type.name})")
        return super().default(obj)


### -------- PARSER ----------


class SQLLogicParser:
    def reset(self):
        self.current_line = 0
        self.seen_statement = False
        self.lines = []
        self.current_test = None

    def __init__(self):
        self.reset()
        self.PARSER = {
            TokenType.SQLLOGIC_STATEMENT: self.parse_statement,
            TokenType.SQLLOGIC_QUERY: self.parse_query,
            TokenType.SQLLOGIC_REQUIRE: self.parse_require,
            TokenType.SQLLOGIC_HASH_THRESHOLD: self.parse_hash_threshold,
            TokenType.SQLLOGIC_HALT: self.parse_halt,
            TokenType.SQLLOGIC_SKIP_IF: self.parse_skipif,
            TokenType.SQLLOGIC_ONLY_IF: self.parse_onlyif,
            TokenType.SQLLOGIC_MODE: self.parse_mode,
            TokenType.SQLLOGIC_SET: self.parse_set,
            TokenType.SQLLOGIC_LOOP: self.parse_loop,
            TokenType.SQLLOGIC_CONCURRENT_LOOP: self.parse_loop,
            TokenType.SQLLOGIC_FOREACH: self.parse_foreach,
            TokenType.SQLLOGIC_CONCURRENT_FOREACH: self.parse_foreach,
            TokenType.SQLLOGIC_ENDLOOP: self.parse_endloop,
            TokenType.SQLLOGIC_REQUIRE_ENV: self.parse_require_env,
            TokenType.SQLLOGIC_LOAD: self.parse_load,
            TokenType.SQLLOGIC_RESTART: self.parse_restart,
            TokenType.SQLLOGIC_RECONNECT: self.parse_reconnect,
            TokenType.SQLLOGIC_SLEEP: self.parse_sleep,
            TokenType.SQLLOGIC_INVALID: None,
        }

    def peek(self):
        if self.current_line >= len(self.lines):
            raise Exception("File already fully consumed")
        return self.lines[self.current_line].strip()

    def consume(self):
        if self.current_line >= len(self.lines):
            raise Exception("File already fully consumed")
        self.current_line += 1

    def fail(self, message):
        raise Exception(message)

    def get_expected_result(self, statement_type: str) -> ExpectedResult:
        type_map = {
            'ok': ExpectedResult.Type.SUCCES,
            'error': ExpectedResult.Type.ERROR,
            'maybe': ExpectedResult.Type.UNKNOWN,
        }
        if statement_type not in type_map:
            error = 'statement argument should be ' + create_formatted_list(type_map.keys())
            self.fail(error)
        return ExpectedResult(type_map[statement_type])

    def extract_expected_result(self) -> Optional[List[str]]:
        end_of_file = self.current_line >= len(self.lines)
        if end_of_file or self.peek() != "----":
            return None

        self.consume()
        result = []
        while self.current_line < len(self.lines) and self.peek():
            result.append(self.peek())
            self.consume()
        return result

    def parse_statement(self, header: Token) -> Optional[BaseStatement]:
        options = ['ok', 'error', 'maybe']
        if len(header.parameters) < 1:
            self.fail(f"statement requires at least one parameter ({create_formatted_list(options)})")
        expected_result = self.get_expected_result(header.parameters[0])

        statement = Statement(header, self.current_line + 1)
        statement.file_name = self.current_test.path

        self.next_line()
        statement_text = self.extract_statement()
        if statement_text == []:
            self.fail("Unexpected empty statement text")
        statement.add_lines(statement_text)

        expected_lines: Optional[List[str]] = self.extract_expected_result()
        match expected_result.type:
            case ExpectedResult.Type.SUCCES:
                if expected_lines != None:
                    if len(expected_lines) != 0:
                        self.fail(
                            "Failed to parse statement: only statement error can have an expected error message, not statement ok"
                        )
                    expected_result.add_lines(expected_lines)
            case ExpectedResult.Type.ERROR | ExpectedResult.Type.UNKNOWN:
                if expected_lines != None:
                    expected_result.add_lines(expected_lines)
                elif not self.current_test.is_sqlite_test():
                    print(statement)
                    self.fail('Failed to parse statement: statement error needs to have an expected error message')
            case _:
                raise Exception(f"Unexpected ExpectedResult Type: {expected_result.type.name}")

        statement.expected_result = expected_result
        # perform any renames in the text
        # TODO: deal with renames
        if len(header.parameters) >= 2:
            statement.set_connection(header.parameters[1])
        return statement

    def parse_query(self, header: Token) -> BaseStatement:
        if len(header.parameters) < 1:
            self.fail("query requires at least one parameter (query III)")
        statement = Query(header, self.current_line + 1)

        # parse the expected column count
        statement.expected_column_count = 0
        column_text = header.parameters[0]
        accepted_chars = ['T', 'I', 'R']
        if not all(x in accepted_chars for x in column_text):
            self.fail(f"Found unknown character in {column_text}, expected {create_formatted_list(accepted_chars)}")
        expected_column_count = len(column_text)

        statement.expected_column_count = expected_column_count
        if statement.expected_column_count == 0:
            self.fail("Query requires at least a single column in the result")

        statement.file_name = self.current_test.path
        statement.query_line = self.current_line + 1
        # extract the SQL statement
        self.next_line()
        statement_text = self.extract_statement()
        # perform any renames in the text
        # TODO: perform replacements
        # statement.base_sql_query = replace_keywords(statement_text)
        statement.add_lines(statement_text)

        # extract the expected result
        expected_result = self.get_expected_result('ok')
        expected_lines: Optional[List[str]] = self.extract_expected_result()
        if expected_lines == None:
            self.fail("'query' did not provide an expected result")
        expected_result.add_lines(expected_lines)
        statement.expected_result = expected_result

        def get_sort_style(parameters: List[str]) -> SortStyle:
            sort_style = SortStyle.NO_SORT
            if len(parameters) > 1:
                sort_style = parameters[1]
                if sort_style == "nosort":
                    # Do no sorting
                    sort_style = SortStyle.NO_SORT
                elif sort_style == "rowsort" or sort_style == "sort":
                    # Row-oriented sorting
                    sort_style = SortStyle.ROW_SORT
                elif sort_style == "valuesort":
                    # Sort all values independently
                    sort_style = SortStyle.VALUE_SORT
                else:
                    sort_style = SortStyle.UNKNOWN
            return sort_style

        # figure out the sort style
        sort_style = get_sort_style(header.parameters)
        if sort_style == SortStyle.UNKNOWN:
            sort_style = SortStyle.NO_SORT
            statement.set_connection(header.parameters[1])
        statement.sort_style = sort_style

        # check the label of the query
        if len(header.parameters) > 2:
            statement.set_label(header.parameters[2])
        return statement

    def parse_hash_threshold(self, header: Token) -> Optional[BaseStatement]:
        if len(header.parameters) != 1:
            self.fail("hash-threshold requires a parameter")
        threshold = int(header.parameters[0])
        return HashThreshold(header, self.current_line + 1, threshold)

    def parse_halt(self, header: Token) -> Optional[BaseStatement]:
        return Halt(header, self.current_line + 1)

    def parse_mode(self, header: Token) -> Optional[BaseStatement]:
        if len(header.parameters) != 1:
            self.fail("mode requires one parameter")
        parameter = header.parameters[0]
        if parameter == "skip":
            return Skip(header, self.current_line + 1)
        elif parameter == "unskip":
            return Unskip(header, self.current_line + 1)
        else:
            return Mode(header, self.current_line + 1, parameter)

    def parse_require(self, header: Token) -> Optional[BaseStatement]:
        return Require(header, self.current_line + 1)

    def parse_set(self, header: Token) -> Optional[BaseStatement]:
        parameters = header.parameters
        if len(parameters) < 1:
            self.fail("set requires at least 1 parameter (e.g. set ignore_error_messages HTTP Error)")
        if parameters[0] == "ignore_error_messages" or parameters[0] == "always_fail_error_messages":
            # TODO: handle these
            # Since we plan to parse everything first and then execute
            # These should return BaseStatements that can be handled by the executor

            string_set = []
            # Parse the parameter list as a comma separated list of strings that can contain spaces
            # e.g. `set ignore_error_messages This is an error message, This_is_another, and   another`
            tmp = [[y.strip() for y in x.split(',') if y.strip() != ''] for x in parameters[1:]]
            for x in tmp:
                string_set.extend(x)
            statement = Set(header, self.current_line + 1)
            statement.add_parameters(string_set)
            return statement
        else:
            self.fail("unrecognized set parameter: %s" % parameters[0])

    def parse_load(self, header: Token) -> Optional[BaseStatement]:
        statement = Load(header, self.current_line + 1)
        if len(header.parameters) > 1 and header.parameters[1] == "readonly":
            statement.set_readonly()
        return statement

    def parse_loop(self, header: Token) -> Optional[BaseStatement]:
        if len(header.parameters) != 3:
            self.fail("Expected loop [iterator_name] [start] [end] (e.g. loop i 1 300)")
        is_parallel = header.type == TokenType.SQLLOGIC_CONCURRENT_LOOP
        statement = Loop(header, self.current_line + 1, is_parallel)
        statement.set_name(header.parameters[0])
        statement.set_start(int(header.parameters[1]))
        statement.set_end(int(header.parameters[2]))
        return statement

    def parse_foreach(self, header: Token) -> Optional[BaseStatement]:
        if len(header.parameters) < 2:
            self.fail(
                "Expected foreach [iterator_name] [m1] [m2] [etc...] (e.g. foreach type integer " "smallint float)"
            )
        is_parallel = header.type == TokenType.SQLLOGIC_CONCURRENT_FOREACH
        statement = Foreach(header, self.current_line + 1, is_parallel)
        statement.set_name(header.parameters[0])
        statement.set_values(header.parameters[1:])
        return statement

    def parse_endloop(self, header: Token) -> Optional[BaseStatement]:
        return Endloop(header, self.current_line + 1)

    def parse_require_env(self, header: Token) -> Optional[BaseStatement]:
        if len(header.parameters) != 1 and len(header.parameters) != 2:
            self.fail("require-env requires 1 argument: <env name> [optional: <expected env val>]")
        return RequireEnv(header, self.current_line + 1)

    def parse_restart(self, header: Token) -> Optional[BaseStatement]:
        return Restart(header, self.current_line + 1)

    def parse_reconnect(self, header: Token) -> Optional[BaseStatement]:
        return Reconnect(header, self.current_line + 1)

    def parse_sleep(self, header: Token) -> Optional[BaseStatement]:
        if len(header.parameters) != 2:
            self.fail("sleep requires two parameter (e.g. sleep 1 second)")
        # require a specific block size
        sleep_duration = int(header.parameters[0])
        sleep_unit = get_sleep_unit(header.parameters[1])
        return Sleep(header, self.current_line + 1, sleep_duration, sleep_unit)

    def parse_skipif(self, header: Token) -> Optional[BaseStatement]:
        return SkipIf(header, self.current_line + 1)

    def parse_onlyif(self, header: Token) -> Optional[BaseStatement]:
        return OnlyIf(header, self.current_line + 1)

    def parse(self, file_path: str) -> Optional[SQLLogicTest]:
        if not self.open_file(file_path):
            return None

        while self.next_statement():
            token = self.tokenize()

            # throw explicit error on single line statements that are not separated by a comment or newline
            if self.is_single_line_statement(token) and not self.next_line_empty_or_comment():
                self.fail("All test statements need to be separated by an empty line")

            # skip_statement = False
            # while token.type == TokenType.SQLLOGIC_SKIP_IF or token.type == TokenType.SQLLOGIC_ONLY_IF:
            #    skip_if = token.type == TokenType.SQLLOGIC_SKIP_IF
            #    if len(token.parameters) < 1:
            #        self.fail("skipif/onlyif requires a single parameter (e.g. skipif duckdb)")
            #    system_name = token.parameters[0].lower()
            #    accepted_systems = [
            #        'duckdb'
            #    ]
            #    if self.current_test.is_sqlite_test():
            #        accepted_systems.append('postgresql')
            #    condition = system_name in accepted_systems
            #    if condition == skip_if:
            #        # we skip this command in two situations
            #        # (1) skipif duckdb
            #        # (2) onlyif <other_system>
            #        skip_statement = True
            #        break
            #    self.next_line()
            #    token = self.tokenize()

            # if skip_statement:
            #    continue

            method = self.PARSER.get(token.type)
            if method:
                statement = method(token)
            else:
                raise Exception(f"Unexpected token type: {token.type.name}")
            if not statement:
                raise Exception(f"Parser did not produce a statement for {token.type.name}")
            self.current_test.add_statement(statement)
        return self.current_test

    def open_file(self, path):
        self.reset()
        self.current_test = SQLLogicTest(path)
        try:
            with open(path, 'r') as infile:
                self.lines = [line.replace("\r", "") for line in infile.readlines()]
                return True
        except IOError:
            return False

    def empty_or_comment(self, line):
        return not line.strip() or line.startswith("#")

    def next_line_empty_or_comment(self):
        if self.current_line + 1 >= len(self.lines):
            return True
        else:
            return self.empty_or_comment(self.lines[self.current_line + 1])

    def next_statement(self):
        if self.seen_statement:
            while self.current_line < len(self.lines) and not self.empty_or_comment(self.peek()):
                self.consume()
        self.seen_statement = True

        while self.current_line < len(self.lines) and self.empty_or_comment(self.peek()):
            self.consume()

        return self.current_line < len(self.lines)

    def next_line(self):
        self.consume()

    def extract_statement(self):
        statement = []

        while self.current_line < len(self.lines) and not self.empty_or_comment(self.peek()):
            line = self.peek()
            if line == "----":
                break
            statement.append(line)
            self.consume()
        return statement

    def fail_recursive(self, msg, values):
        error_message = f"{self.file_name}:{self.current_line + 1}: {msg.format(*values)}"
        raise RuntimeError(error_message)

    def tokenize(self):
        result = Token()
        if self.current_line >= len(self.lines):
            result.type = TokenType.SQLLOGIC_INVALID
            return result

        line = self.peek()
        argument_list = line.split()
        argument_list = [x for x in line.strip().split() if x != '']

        if not argument_list:
            self.fail("Empty line!?")

        result.type = self.command_to_token(argument_list[0])
        result.parameters.extend(argument_list[1:])
        return result

    def is_single_line_statement(self, token):
        single_line_statements = [
            TokenType.SQLLOGIC_HASH_THRESHOLD,
            TokenType.SQLLOGIC_HALT,
            TokenType.SQLLOGIC_MODE,
            TokenType.SQLLOGIC_SET,
            TokenType.SQLLOGIC_LOOP,
            TokenType.SQLLOGIC_FOREACH,
            TokenType.SQLLOGIC_CONCURRENT_LOOP,
            TokenType.SQLLOGIC_CONCURRENT_FOREACH,
            TokenType.SQLLOGIC_ENDLOOP,
            TokenType.SQLLOGIC_REQUIRE,
            TokenType.SQLLOGIC_REQUIRE_ENV,
            TokenType.SQLLOGIC_LOAD,
            TokenType.SQLLOGIC_RESTART,
            TokenType.SQLLOGIC_RECONNECT,
            TokenType.SQLLOGIC_SLEEP,
        ]

        if token.type in single_line_statements:
            return True
        elif token.type in [
            TokenType.SQLLOGIC_SKIP_IF,
            TokenType.SQLLOGIC_ONLY_IF,
            TokenType.SQLLOGIC_INVALID,
            TokenType.SQLLOGIC_STATEMENT,
            TokenType.SQLLOGIC_QUERY,
        ]:
            return False
        else:
            raise RuntimeError("Unknown SQLLogic token found!")

    def command_to_token(self, token):
        token_map = {
            "skipif": TokenType.SQLLOGIC_SKIP_IF,
            "onlyif": TokenType.SQLLOGIC_ONLY_IF,
            "statement": TokenType.SQLLOGIC_STATEMENT,
            "query": TokenType.SQLLOGIC_QUERY,
            "hash-threshold": TokenType.SQLLOGIC_HASH_THRESHOLD,
            "halt": TokenType.SQLLOGIC_HALT,
            "mode": TokenType.SQLLOGIC_MODE,
            "set": TokenType.SQLLOGIC_SET,
            "loop": TokenType.SQLLOGIC_LOOP,
            "concurrentloop": TokenType.SQLLOGIC_CONCURRENT_LOOP,
            "foreach": TokenType.SQLLOGIC_FOREACH,
            "concurrentforeach": TokenType.SQLLOGIC_CONCURRENT_FOREACH,
            "endloop": TokenType.SQLLOGIC_ENDLOOP,
            "require": TokenType.SQLLOGIC_REQUIRE,
            "require-env": TokenType.SQLLOGIC_REQUIRE_ENV,
            "load": TokenType.SQLLOGIC_LOAD,
            "restart": TokenType.SQLLOGIC_RESTART,
            "reconnect": TokenType.SQLLOGIC_RECONNECT,
            "sleep": TokenType.SQLLOGIC_SLEEP,
        }

        if token in token_map:
            return token_map[token]
        else:
            self.fail("Unrecognized parameter %s", token)
            return TokenType.SQLLOGIC_INVALID


import argparse


def main():
    parser = argparse.ArgumentParser(description="SQL Logic Parser")
    parser.add_argument("filename", type=str, help="Path to the SQL logic file")
    args = parser.parse_args()

    filename = args.filename

    parser = SQLLogicParser()
    out: Optional[SQLLogicTest] = parser.parse(filename)
    if not out:
        raise Exception(f"Test {filename} could not be parsed")
    res = json.dumps(out, cls=SQLLogicEncoder, indent=4)
    print(res)


if __name__ == "__main__":
    main()
