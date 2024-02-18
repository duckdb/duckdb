import logging
import termcolor
from duckdb import tokenize, token_type


class SQLLogicTestLogger:
    def __init__(self, context, command):
        self.log_lock = command.runner.log_lock
        self.file_name = command.file_name
        self.query_line = command.query_line
        self.sql_query = context.sql_query

    def log(self, message):
        logging.error(message)

    def print_expected_result(self, values, columns, row_wise):
        if row_wise:
            for value in values:
                print(value)
        else:
            c = 0
            for value in values:
                if c != 0:
                    print("\t", end="")
                print(value, end="")
                c += 1
                if c >= columns:
                    print()
                    c = 0

    def print_line_sep(self):
        line_sep = "=" * 80
        print(termcolor.color('grey') + line_sep + termcolor.reset())

    def print_header(self, header):
        print(termcolor.bold(header) + termcolor.reset())

    def print_file_header(self):
        self.print_header(f"File {self.file_name}:{self.query_line})")

    def print_sql(self):
        query = self.sql_query.strip()
        if not query.endswith(";"):
            query += ";"
        print(query)

    def print_sql_formatted(self):
        print(termcolor.bold("SQL Query") + termcolor.reset())
        tokens = tokenize(self.sql_query)
        for i, token in enumerate(tokens):
            next_token_start = tokens[i + 1].start if i + 1 < len(tokens) else len(self.sql_query)
            token_text = self.sql_query[token.start : next_token_start]
            # Apply highlighting based on token type
            if token.type in [token_type.identifier, token_type.numeric_const, token_type.string_const]:
                print(termcolor.color('yellow') + token_text + termcolor.reset(), end="")
            elif token.type == token_type.keyword:
                print(termcolor.color('green') + termcolor.bold(token_text) + termcolor.reset(), end="")
            else:
                print(token_text, end="")
        print()

    def print_error_header(self, description):
        self.print_line_sep()
        print(termcolor.color('red') + termcolor.bold(description) + termcolor.reset(), end=" ")
        print(termcolor.bold("(" + self.file_name + ":" + str(self.query_line) + ")!") + termcolor.reset())

    def print_result_error(self, result_values, values, expected_column_count, row_wise):
        self.print_header("Expected result:")
        self.print_line_sep()
        self.print_expected_result(values, expected_column_count, row_wise)
        self.print_line_sep()
        self.print_header("Actual result:")
        self.print_line_sep()
        self.print_expected_result(result_values, expected_column_count, False)

    def unexpected_failure(self, result):
        self.print_line_sep()
        print(f"Query unexpectedly failed ({self.file_name}:{self.query_line})\n")
        self.print_line_sep()
        self.print_sql()
        self.print_line_sep()
        result.print()

    def output_result(self, result, result_values_string):
        for column_name in result.names:
            print(column_name, end="\t")
        print()
        for column_type in result.types:
            print(column_type.to_string(), end="\t")
        print()
        self.print_line_sep()
        for r in range(result.row_count):
            for c in range(result.column_count):
                print(result_values_string[r * result.column_count + c], end="\t")
            print()

    def output_hash(self, hash_value):
        self.print_line_sep()
        self.print_sql()
        self.print_line_sep()
        print(hash_value)
        self.print_line_sep()

    def column_count_mismatch(self, result, result_values_string, expected_column_count, row_wise):
        self.print_error_header("Wrong column count in query!")
        print(
            f"Expected {termcolor.bold(expected_column_count)} columns, but got {termcolor.bold(result.column_count)} columns"
        )
        self.print_line_sep()
        self.print_sql()
        self.print_line_sep()
        self.print_result_error(result, result_values_string, expected_column_count, row_wise)

    def not_cleanly_divisible(self, expected_column_count, actual_column_count):
        self.print_error_header("Error in test!")
        print(f"Expected {expected_column_count} columns, but {actual_column_count} values were supplied")
        print("This is not cleanly divisible (i.e. the last row does not have enough values)")

    def wrong_row_count(self, expected_rows, result, comparison_values, expected_column_count, row_wise):
        self.print_error_header("Wrong row count in query!")
        print(f"Expected {termcolor.bold(expected_rows)} rows, but got {termcolor.bold(result.row_count)} rows")
        self.print_line_sep()
        self.print_sql()
        self.print_line_sep()
        self.print_result_error(result, comparison_values, expected_column_count, row_wise)

    def column_count_mismatch_correct_result(self, original_expected_columns, expected_column_count, result):
        self.print_line_sep()
        self.print_error_header("Wrong column count in query!")
        print(
            f"Expected {termcolor.bold(original_expected_columns)} columns, but got {termcolor.bold(expected_column_count)} columns"
        )
        self.print_line_sep()
        self.print_sql()
        print(f"The expected result {termcolor.bold('matched')} the query result.")
        print(
            f"Suggested fix: modify header to \"{termcolor.color('green')}query {'I' * result.column_count}{termcolor.reset()}\""
        )
        self.print_line_sep()

    def split_mismatch(self, row_number, expected_column_count, split_count):
        self.print_line_sep()
        self.print_error_header(f"Error in test! Column count mismatch after splitting on tab on row {row_number}!")
        print(
            f"Expected {termcolor.bold(expected_column_count)} columns, but got {termcolor.bold(split_count)} columns"
        )
        print("Does the result contain tab values? In that case, place every value on a single row.")
        self.print_line_sep()

    def wrong_result_hash(self, expected_result, result):
        if expected_result:
            expected_result.print()
        else:
            print("???")
        self.print_error_header("Wrong result hash!")
        self.print_line_sep()
        self.print_sql()
        self.print_line_sep()
        self.print_header("Expected result:")
        self.print_line_sep()
        self.print_header("Actual result:")
        self.print_line_sep()

    def unexpected_statement(self, expect_ok, result):
        description = "Query unexpectedly succeeded!" if not expect_ok else "Query unexpectedly failed!"
        self.print_error_header(description)
        self.print_line_sep()
        self.print_sql()
        self.print_line_sep()
        result.print()

    def expected_error_mismatch(self, expected_error, result):
        self.print_error_header(
            f"Query failed, but error message did not match expected error message: {expected_error}"
        )
        self.print_line_sep()
        self.print_sql()
        self.print_header("Actual result:")
        self.print_line_sep()
        result.print()
