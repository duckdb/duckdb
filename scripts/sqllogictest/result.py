from hashlib import md5

from .statement import Query, SortStyle
from .parser import SQLLogicTest
from typing import Optional, Any, Tuple, List, Dict, Set
from .logger import SQLLogicTestLogger
import duckdb
import os
import math

import re
from duckdb.typing import DuckDBPyType
import decimal
from functools import cmp_to_key


class QueryResult:
    def __init__(self, result: List[Tuple[Any]], types: List[str], error: Optional[Exception] = None):
        self._result = result
        self.types = types
        self.error = error
        if not error:
            self._column_count = len(self.types)
            self._row_count = len(result)

    def get_value(self, column, row):
        return self._result[row][column]

    def row_count(self) -> int:
        return self._row_count

    def column_count(self) -> int:
        assert self._column_count != 0
        return self._column_count

    def has_error(self) -> bool:
        return self.error != None

    def get_error(self) -> Optional[Exception]:
        return self.error


def compare_values(result: QueryResult, actual_str, expected_str, current_column):
    error = False

    if actual_str == expected_str:
        return True

    if expected_str.startswith("<REGEX>:") or expected_str.startswith("<!REGEX>:"):
        if expected_str.startswith("<REGEX>:"):
            should_match = True
            regex_str = expected_str.replace("<REGEX>:", "")
        else:
            should_match = False
            regex_str = expected_str.replace("<!REGEX>:", "")
        re_options = re.DOTALL
        re_pattern = re.compile(regex_str, re_options)
        regex_matches = bool(re_pattern.fullmatch(actual_str))
        if regex_matches == should_match:
            return True

    sql_type = result.types[current_column]

    def is_numeric(type) -> bool:
        NUMERIC_TYPES = [
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "UHUGEINT",
        ]
        if str(type) in NUMERIC_TYPES:
            return True
        return 'DECIMAL' in str(type)

    if is_numeric(sql_type):
        if sql_type in [duckdb.typing.FLOAT, duckdb.typing.DOUBLE]:
            # ApproxEqual
            expected = convert_value(expected_str, sql_type)
            actual = convert_value(actual_str, sql_type)
            if expected == actual:
                return True
            if math.isnan(expected) and math.isnan(actual):
                return True
            epsilon = abs(actual) * 0.01 + 0.00000001
            if abs(expected - actual) <= epsilon:
                return True
            return False
        expected = convert_value(expected_str, sql_type)
        actual = convert_value(actual_str, sql_type)
        return expected == actual

    if sql_type == duckdb.typing.BOOLEAN or sql_type.id == 'timestamp with time zone':
        expected = convert_value(expected_str, sql_type)
        actual = convert_value(actual_str, sql_type)
        return expected == actual
    expected = sql_logic_test_convert_value(expected_str, sql_type, False)
    actual = actual_str
    error = actual != expected

    if error:
        return False
    return True


def result_is_hash(result):
    parts = result.split()
    if len(parts) != 5:
        return False
    if not parts[0].isdigit():
        return False
    if parts[1] != "values" or parts[2] != "hashing" or len(parts[4]) != 32:
        return False
    return all([x.islower() or x.isnumeric() for x in parts[4]])


def result_is_file(result: str):
    return result.startswith('<FILE>:')


def load_result_from_file(fname, names):
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={os.cpu_count()}")

    fname = fname.replace("<FILE>:", "")

    struct_definition = "STRUCT_PACK("
    for i in range(len(names)):
        if i > 0:
            struct_definition += ", "
        struct_definition += f"{names[i]} := VARCHAR"
    struct_definition += ")"

    csv_result = con.execute(
        f"""
        SELECT * FROM read_csv(
            '{fname}',
            header=1,
            sep='|',
            columns={struct_definition},
            auto_detect=false,
            all_varchar=true
        )
    """
    )

    return csv_result.fetchall()


def convert_value(value, type: str):
    if value is None or value == 'NULL':
        return 'NULL'
    query = f'select $1::{type}'
    return duckdb.execute(query, [value]).fetchone()[0]


def sql_logic_test_convert_value(value, sql_type, is_sqlite_test: bool) -> str:
    if value is None or value == 'NULL':
        return 'NULL'
    if is_sqlite_test:
        if sql_type in [
            duckdb.typing.BOOLEAN,
            duckdb.typing.DOUBLE,
            duckdb.typing.FLOAT,
        ] or any([type_str in str(sql_type) for type_str in ['DECIMAL', 'HUGEINT']]):
            return convert_value(value, 'BIGINT::VARCHAR')
    if sql_type == duckdb.typing.BOOLEAN:
        return "1" if convert_value(value, sql_type) else "0"
    else:
        res = convert_value(value, 'VARCHAR')
        if len(res) == 0:
            res = "(empty)"
        else:
            res = res.replace("\0", "\\0")
    return res


def duck_db_convert_result(result: QueryResult, is_sqlite_test: bool) -> List[str]:
    out_result = []
    row_count = result.row_count()
    column_count = result.column_count()

    for r in range(row_count):
        for c in range(column_count):
            value = result.get_value(c, r)
            converted_value = sql_logic_test_convert_value(value, result.types[c], is_sqlite_test)
            out_result.append(converted_value)

    return out_result


class SQLLogicRunner:
    def reset(self):
        self.skipped = False
        self.error: Optional[str] = None
        self.skip_level: int = 0

        self.dbpath = ''
        self.loaded_databases: Set[str] = set()
        self.db: Optional[duckdb.DuckDBPyConnection] = None
        self.config: Dict[str, Any] = {
            'allow_unsigned_extensions': True,
        }
        self.extensions: set = set()

        self.con: Optional[duckdb.DuckDBPyConnection] = None
        self.cursors: Dict[str, duckdb.DuckDBPyConnection] = {}

        self.environment_variables: Dict[str, str] = {}
        self.test: Optional[SQLLogicTest] = None

        self.hash_threshold: int = 0
        self.hash_label_map: Dict[str, str] = {}
        self.result_label_map: Dict[str, Any] = {}

        self.required_requires: set = set()
        self.output_hash_mode = False
        self.output_result_mode = False
        self.debug_mode = False

        self.finished_processing_file = False
        # If these error messages occur in a test, the test will abort but still count as passed
        self.ignore_error_messages = {"HTTP", "Unable to connect"}
        # If these error messages occur in a statement that is expected to fail, the test will fail
        self.always_fail_error_messages = {"differs from original result!", "INTERNAL"}

        self.original_sqlite_test = False

    def skip_error_message(self, message):
        for error_message in self.ignore_error_messages:
            if error_message in str(message):
                return True
        return False

    def __init__(self):
        self.reset()

    def fail_query(self, query: Query):
        # if context.is_parallel:
        #    self.finished_processing_file = True
        #    context.error_file = file_name
        #    context.error_line = query_line
        # else:
        #    fail_line(file_name, query_line, 0)
        self.fail(f'Failed: {self.test.path}:{query.get_query_line()}')

    def fail(self, message):
        raise Exception(message)

    def skip(self):
        self.skip_level += 1

    def unskip(self):
        self.skip_level -= 1

    def skip_active(self) -> bool:
        return self.skip_level > 0

    def is_required(self, param):
        return param in self.required_requires

    def check_query_result(self, query: Query, result: QueryResult) -> None:
        expected_column_count = query.expected_result.get_expected_column_count()
        values = query.expected_result.lines
        sort_style = query.get_sortstyle()
        query_label = query.get_label()
        query_has_label = query_label != None

        logger = SQLLogicTestLogger(query, self.test.path)

        # If the result has an error, log it
        if result.has_error():
            logger.unexpected_failure(result)
            if self.skip_error_message(result.get_error()):
                self.finished_processing_file = True
                return
            print(result.get_error())
            self.fail_query(query)

        row_count = result.row_count()
        column_count = result.column_count()
        total_value_count = row_count * column_count

        if len(values) == 1 and result_is_hash(values[0]):
            compare_hash = True
            is_hash = True
        else:
            compare_hash = query_has_label or (self.hash_threshold > 0 and total_value_count > self.hash_threshold)
            is_hash = False

        result_values_string = duck_db_convert_result(result, self.original_sqlite_test)

        if self.output_result_mode:
            logger.output_result(result, result_values_string)

        if sort_style == SortStyle.ROW_SORT:
            ncols = result.column_count()
            nrows = int(total_value_count / ncols)
            rows = [result_values_string[i * ncols : (i + 1) * ncols] for i in range(nrows)]

            # Define the comparison function
            def compare_rows(a, b):
                for col_idx, val in enumerate(a):
                    a_val = val
                    b_val = b[col_idx]
                    if a_val != b_val:
                        return -1 if a_val < b_val else 1
                return 0

            # Sort the individual rows based on element comparison
            sorted_rows = sorted(rows, key=cmp_to_key(compare_rows))
            rows = sorted_rows

            for row_idx, row in enumerate(rows):
                for col_idx, val in enumerate(row):
                    result_values_string[row_idx * ncols + col_idx] = val
        elif sort_style == SortStyle.VALUE_SORT:
            result_values_string.sort()

        comparison_values = []
        if len(values) == 1 and result_is_file(values[0]):
            fname = self.loop_replacement(values[0], context.running_loops)
            csv_error = ""
            comparison_values = load_result_from_file(fname, result.names, expected_column_count, csv_error)
            if csv_error:
                logger.print_error_header(csv_error)
                self.fail_query(query)
        else:
            comparison_values = values

        hash_value = ""
        if self.output_hash_mode or compare_hash:
            hash_context = md5()
            for val in result_values_string:
                hash_context.update(str(val).encode())
                hash_context.update("\n".encode())
            digest = hash_context.hexdigest()
            hash_value = f"{total_value_count} values hashing to {digest}"
            if self.output_hash_mode:
                logger.output_hash(hash_value)
                return

        if not compare_hash:
            original_expected_columns = expected_column_count
            column_count_mismatch = False

            if expected_column_count != result.column_count():
                expected_column_count = result.column_count()
                column_count_mismatch = True

            expected_rows = len(comparison_values) / expected_column_count
            row_wise = expected_column_count > 1 and len(comparison_values) == result.row_count()

            if not row_wise:
                all_tabs = all("\t" in val for val in comparison_values)
                row_wise = all_tabs

            if row_wise:
                expected_rows = len(comparison_values)
                row_wise = True
            elif len(comparison_values) % expected_column_count != 0:
                if column_count_mismatch:
                    logger.column_count_mismatch(result, query.values, original_expected_columns, row_wise)
                else:
                    logger.not_cleanly_divisible(expected_column_count, len(comparison_values))
                self.fail_query(query)

            if expected_rows != result.row_count():
                if column_count_mismatch:
                    logger.column_count_mismatch(result, query.values, original_expected_columns, row_wise)
                else:
                    logger.wrong_row_count(
                        expected_rows, result_values_string, comparison_values, expected_column_count, row_wise
                    )
                self.fail_query(query)

            if row_wise:
                current_row = 0
                for i, val in enumerate(comparison_values):
                    splits = [x for x in val.split("\t") if x != '']
                    if len(splits) != expected_column_count:
                        if column_count_mismatch:
                            logger.column_count_mismatch(result, query.values, original_expected_columns, row_wise)
                        logger.split_mismatch(i + 1, expected_column_count, len(splits))
                        self.fail_query(query)
                    for c, split_val in enumerate(splits):
                        lvalue_str = result_values_string[current_row * expected_column_count + c]
                        rvalue_str = split_val
                        success = compare_values(result, lvalue_str, split_val, c)
                        if not success:
                            logger.print_error_header("Wrong result in query!")
                            logger.print_line_sep()
                            logger.print_sql()
                            logger.print_line_sep()
                            print(f"Mismatch on row {current_row + 1}, column {c + 1}")
                            print(f"{lvalue_str} <> {rvalue_str}")
                            logger.print_line_sep()
                            logger.print_result_error(result_values_string, values, expected_column_count, row_wise)
                            self.fail_query(query)
                        # Increment the assertion counter
                        assert success
                    current_row += 1
            else:
                current_row, current_column = 0, 0
                for i, val in enumerate(comparison_values):
                    lvalue_str = result_values_string[current_row * expected_column_count + current_column]
                    rvalue_str = val
                    success = compare_values(result, lvalue_str, rvalue_str, current_column)
                    if not success:
                        logger.print_error_header("Wrong result in query!")
                        logger.print_line_sep()
                        logger.print_sql()
                        logger.print_line_sep()
                        print(f"Mismatch on row {current_row + 1}, column {current_column + 1}")
                        print(f"{lvalue_str} <> {rvalue_str}")
                        logger.print_line_sep()
                        logger.print_result_error(result_values_string, values, expected_column_count, row_wise)
                        self.fail_query(query)
                    # Increment the assertion counter
                    assert success

                    current_column += 1
                    if current_column == expected_column_count:
                        current_row += 1
                        current_column = 0

            if column_count_mismatch:
                logger.column_count_mismatch_correct_result(original_expected_columns, expected_column_count, result)
                self.fail_query(query)
        else:
            hash_compare_error = False
            if query_has_label:
                entry = self.hash_label_map.get(query_label)
                if entry is None:
                    self.hash_label_map[query_label] = hash_value
                    self.result_label_map[query_label] = result
                else:
                    hash_compare_error = entry != hash_value

            if is_hash:
                hash_compare_error = values[0] != hash_value

            if hash_compare_error:
                expected_result = self.result_label_map.get(query_label)
                logger.wrong_result_hash(expected_result, result)
                self.fail_query(query)

            assert not hash_compare_error
