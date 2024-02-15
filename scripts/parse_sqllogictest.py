import os

from enum import Enum, auto


class SQLLogicToken:
    def __init__(self):
        self.type = SQLLogicTokenType.SQLLOGIC_INVALID
        self.parameters = []

    def __repr__(self) -> str:
        return f'{self.type}: {self.parameters}'


class SQLLogicTokenType(Enum):
    SQLLOGIC_INVALID = auto()
    SQLLOGIC_SKIP_IF = auto()
    SQLLOGIC_ONLY_IF = auto()
    SQLLOGIC_STATEMENT = auto()
    SQLLOGIC_QUERY = auto()
    SQLLOGIC_HASH_THRESHOLD = auto()
    SQLLOGIC_HALT = auto()
    SQLLOGIC_MODE = auto()
    SQLLOGIC_SET = auto()
    SQLLOGIC_LOOP = auto()
    SQLLOGIC_CONCURRENT_LOOP = auto()
    SQLLOGIC_FOREACH = auto()
    SQLLOGIC_CONCURRENT_FOREACH = auto()
    SQLLOGIC_ENDLOOP = auto()
    SQLLOGIC_REQUIRE = auto()
    SQLLOGIC_REQUIRE_ENV = auto()
    SQLLOGIC_LOAD = auto()
    SQLLOGIC_RESTART = auto()
    SQLLOGIC_RECONNECT = auto()
    SQLLOGIC_SLEEP = auto()


class SQLLogicParser:
    def __init__(self):
        self.file_name = ""
        self.lines = []
        self.current_line = 0
        self.seen_statement = False

    def open_file(self, path):
        self.file_name = path

        try:
            with open(self.file_name, 'r') as infile:
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
            while self.current_line < len(self.lines) and not self.empty_or_comment(self.lines[self.current_line]):
                self.current_line += 1
        self.seen_statement = True

        while self.current_line < len(self.lines) and self.empty_or_comment(self.lines[self.current_line]):
            self.current_line += 1

        return self.current_line < len(self.lines)

    def next_line(self):
        self.current_line += 1

    def extract_statement(self):
        statement = ""
        first_line = True

        while self.current_line < len(self.lines) and not self.empty_or_comment(self.lines[self.current_line]):
            if self.lines[self.current_line] == "----":
                break
            if not first_line:
                statement += "\n"
            statement += self.lines[self.current_line]
            first_line = False
            self.current_line += 1

        return statement

    def extract_expected_result(self):
        result = []

        if self.current_line < len(self.lines) and self.lines[self.current_line] == "----":
            self.current_line += 1

        while self.current_line < len(self.lines) and self.lines[self.current_line].strip():
            result.append(self.lines[self.current_line])
            self.current_line += 1

        return result

    def extract_expected_error(self, expect_ok, original_sqlite_test):
        if self.current_line >= len(self.lines) or self.lines[self.current_line] != "----":
            if not expect_ok and not original_sqlite_test:
                self.fail("Failed to parse statement: statement error needs to have an expected error message")
            return ""

        if expect_ok:
            self.fail(
                "Failed to parse statement: only statement error can have an expected error message, not statement ok"
            )

        self.current_line += 1
        error_lines = []

        while self.current_line < len(self.lines) and self.lines[self.current_line].strip():
            error_lines.append(self.lines[self.current_line])
            self.current_line += 1

        return "\n".join(error_lines)

    def fail_recursive(self, msg, values):
        error_message = f"{self.file_name}:{self.current_line + 1}: {msg.format(*values)}"
        raise RuntimeError(error_message)

    def tokenize(self):
        result = SQLLogicToken()
        if self.current_line >= len(self.lines):
            result.type = SQLLogicTokenType.SQLLOGIC_INVALID
            return result

        line = self.lines[self.current_line]
        argument_list = line.split()
        argument_list = [x for x in line.strip().split() if x != '']

        if not argument_list:
            self.fail("Empty line!?")

        result.type = self.command_to_token(argument_list[0])
        result.parameters.extend(argument_list[1:])
        return result

    def is_single_line_statement(self, token):
        single_line_statements = [
            SQLLogicTokenType.SQLLOGIC_HASH_THRESHOLD,
            SQLLogicTokenType.SQLLOGIC_HALT,
            SQLLogicTokenType.SQLLOGIC_MODE,
            SQLLogicTokenType.SQLLOGIC_SET,
            SQLLogicTokenType.SQLLOGIC_LOOP,
            SQLLogicTokenType.SQLLOGIC_FOREACH,
            SQLLogicTokenType.SQLLOGIC_CONCURRENT_LOOP,
            SQLLogicTokenType.SQLLOGIC_CONCURRENT_FOREACH,
            SQLLogicTokenType.SQLLOGIC_ENDLOOP,
            SQLLogicTokenType.SQLLOGIC_REQUIRE,
            SQLLogicTokenType.SQLLOGIC_REQUIRE_ENV,
            SQLLogicTokenType.SQLLOGIC_LOAD,
            SQLLogicTokenType.SQLLOGIC_RESTART,
            SQLLogicTokenType.SQLLOGIC_RECONNECT,
            SQLLogicTokenType.SQLLOGIC_SLEEP,
        ]

        if token.type in single_line_statements:
            return True
        elif token.type in [
            SQLLogicTokenType.SQLLOGIC_SKIP_IF,
            SQLLogicTokenType.SQLLOGIC_ONLY_IF,
            SQLLogicTokenType.SQLLOGIC_INVALID,
            SQLLogicTokenType.SQLLOGIC_STATEMENT,
            SQLLogicTokenType.SQLLOGIC_QUERY,
        ]:
            return False
        else:
            raise RuntimeError("Unknown SQLLogic token found!")

    def command_to_token(self, token):
        token_map = {
            "skipif": SQLLogicTokenType.SQLLOGIC_SKIP_IF,
            "onlyif": SQLLogicTokenType.SQLLOGIC_ONLY_IF,
            "statement": SQLLogicTokenType.SQLLOGIC_STATEMENT,
            "query": SQLLogicTokenType.SQLLOGIC_QUERY,
            "hash-threshold": SQLLogicTokenType.SQLLOGIC_HASH_THRESHOLD,
            "halt": SQLLogicTokenType.SQLLOGIC_HALT,
            "mode": SQLLogicTokenType.SQLLOGIC_MODE,
            "set": SQLLogicTokenType.SQLLOGIC_SET,
            "loop": SQLLogicTokenType.SQLLOGIC_LOOP,
            "concurrentloop": SQLLogicTokenType.SQLLOGIC_CONCURRENT_LOOP,
            "foreach": SQLLogicTokenType.SQLLOGIC_FOREACH,
            "concurrentforeach": SQLLogicTokenType.SQLLOGIC_CONCURRENT_FOREACH,
            "endloop": SQLLogicTokenType.SQLLOGIC_ENDLOOP,
            "require": SQLLogicTokenType.SQLLOGIC_REQUIRE,
            "require-env": SQLLogicTokenType.SQLLOGIC_REQUIRE_ENV,
            "load": SQLLogicTokenType.SQLLOGIC_LOAD,
            "restart": SQLLogicTokenType.SQLLOGIC_RESTART,
            "reconnect": SQLLogicTokenType.SQLLOGIC_RECONNECT,
            "sleep": SQLLogicTokenType.SQLLOGIC_SLEEP,
        }

        if token in token_map:
            return token_map[token]
        else:
            self.fail("Unrecognized parameter %s", token)
            return SQLLogicTokenType.SQLLOGIC_INVALID


import argparse


def main():
    parser = argparse.ArgumentParser(description="SQL Logic Parser")
    parser.add_argument("filename", type=str, help="Path to the SQL logic file")
    args = parser.parse_args()

    filename = args.filename

    parser = SQLLogicParser()
    if parser.open_file(filename):
        print(f"Successfully opened file: {filename}")
        while parser.next_statement():
            token = parser.tokenize()
            print(token)
    else:
        print(f"Failed to open file: {filename}")


if __name__ == "__main__":
    main()
