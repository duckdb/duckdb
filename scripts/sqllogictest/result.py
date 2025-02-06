from hashlib import md5
import gc

from .base_statement import BaseStatement
from .test import SQLLogicTest
from .statement import (
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
    Unzip,
    SortStyle,
    Unskip,
)

from .expected_result import ExpectedResult
from typing import Optional, Any, Tuple, List, Dict, Generator
import typing

from .logger import SQLLogicTestLogger
import duckdb
import os
import math
import time
import threading

import re
from functools import cmp_to_key
from enum import Enum

### Helper structs


class RequireResult(Enum):
    MISSING = 0
    PRESENT = 1


class ExecuteResult:
    class Type(Enum):
        SUCCESS = 0
        ERROR = 1
        SKIPPED = 2

    def __init__(self, type: "ExecuteResult.Type"):
        self.type = type


### Exceptions

BUILTIN_EXTENSIONS = [
    'json',
    'fts',
    'tpcds',
    'tpch',
    'parquet',
    'icu',
]

from duckdb import DuckDBPyConnection

# def patch_execute(method):
#    def patched_execute(self, *args, **kwargs):
#        print(*args)
#        return method(self, *args, **kwargs)
#    return patched_execute

# patched_execute = patch_execute(getattr(DuckDBPyConnection, "execute"))
# setattr(DuckDBPyConnection, "execute", patched_execute)


class SQLLogicStatementData:
    # Context information about a statement
    def __init__(self, test: SQLLogicTest, statement: BaseStatement):
        self.test = test
        self.statement = statement

    def __str__(self) -> str:
        return f'{self.test.path}:{self.statement.get_query_line()}'

    __repr__ = __str__


class TestException(Exception):
    __slots__ = ['data', 'message', 'result']

    def __init__(self, data: SQLLogicStatementData, message: str, result: ExecuteResult):
        self.message = f'{str(data)} {message}'
        super().__init__(self.message)
        self.data = data
        self.result = result

    def handle_result(self) -> ExecuteResult:
        return self.result


class SkipException(TestException):
    def __init__(self, data: SQLLogicStatementData, message: str):
        super().__init__(data, message, ExecuteResult(ExecuteResult.Type.SKIPPED))


class FailException(TestException):
    def __init__(self, data: SQLLogicStatementData, message: str):
        super().__init__(data, message, ExecuteResult(ExecuteResult.Type.ERROR))


### Result primitive


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

    @property
    def column_count(self) -> int:
        assert self._column_count != 0
        return self._column_count

    def has_error(self) -> bool:
        return self.error != None

    def get_error(self) -> Optional[Exception]:
        return self.error

    def check(self, context, query: Query) -> None:
        expected_column_count = query.expected_result.get_expected_column_count()
        values = query.expected_result.lines
        sort_style = query.get_sortstyle()
        query_label = query.get_label()
        query_has_label = query_label != None
        runner = context.runner

        logger = SQLLogicTestLogger(context, query, runner.test.path)

        # If the result has an error, log it
        if self.has_error():
            logger.unexpected_failure(self)
            if runner.skip_error_message(self.get_error()):
                runner.finished_processing_file = True
                return
            context.fail(self.get_error())

        row_count = self.row_count()
        column_count = self.column_count
        total_value_count = row_count * column_count

        if len(values) == 1 and result_is_hash(values[0]):
            compare_hash = True
            is_hash = True
        else:
            compare_hash = query_has_label or (runner.hash_threshold > 0 and total_value_count > runner.hash_threshold)
            is_hash = False

        result_values_string = duck_db_convert_result(self, runner.original_sqlite_test)

        if runner.output_result_mode:
            logger.output_result(self, result_values_string)

        if sort_style == SortStyle.ROW_SORT:
            ncols = self.column_count
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
            fname = context.replace_keywords(values[0])
            try:
                comparison_values = load_result_from_file(fname, self)
                # FIXME this is kind of dumb
                # We concatenate it with tabs just so we can split it again later
                for x in range(len(comparison_values)):
                    comparison_values[x] = "\t".join(list(comparison_values[x]))
            except duckdb.Error as e:
                logger.print_error_header(str(e))
                context.fail(f"Failed to load result from {fname}")
        else:
            comparison_values = values

        hash_value = ""
        if runner.output_hash_mode or compare_hash:
            hash_context = md5()
            for val in result_values_string:
                hash_context.update(str(val).encode())
                hash_context.update("\n".encode())
            digest = hash_context.hexdigest()
            hash_value = f"{total_value_count} values hashing to {digest}"
            if runner.output_hash_mode:
                logger.output_hash(hash_value)
                return

        if not compare_hash:
            original_expected_columns = expected_column_count
            column_count_mismatch = False

            if expected_column_count != self.column_count:
                expected_column_count = self.column_count
                column_count_mismatch = True

            expected_rows = len(comparison_values) / expected_column_count
            row_wise = expected_column_count > 1 and len(comparison_values) == self.row_count()

            if not row_wise:
                all_tabs = all("\t" in val for val in comparison_values)
                row_wise = all_tabs

            if row_wise:
                expected_rows = len(comparison_values)
                row_wise = True
            elif len(comparison_values) % expected_column_count != 0:
                if column_count_mismatch:
                    logger.column_count_mismatch(self, values, original_expected_columns, row_wise)
                else:
                    logger.not_cleanly_divisible(expected_column_count, len(comparison_values))
                # FIXME: the logger should just create the strings to send to self.fail()/self.skip()
                context.fail("")

            if expected_rows != self.row_count():
                if column_count_mismatch:
                    logger.column_count_mismatch(self, values, original_expected_columns, row_wise)
                else:
                    logger.wrong_row_count(
                        expected_rows, result_values_string, comparison_values, expected_column_count, row_wise
                    )
                context.fail("")

            if row_wise:
                current_row = 0
                for i, val in enumerate(comparison_values):
                    splits = [x for x in val.split("\t") if x != '']
                    if len(splits) != expected_column_count:
                        if column_count_mismatch:
                            logger.column_count_mismatch(self, values, original_expected_columns, row_wise)
                        logger.split_mismatch(i + 1, expected_column_count, len(splits))
                        context.fail("")
                    for c, split_val in enumerate(splits):
                        lvalue_str = result_values_string[current_row * expected_column_count + c]
                        rvalue_str = split_val
                        success = compare_values(self, lvalue_str, split_val, c)
                        if not success:
                            logger.print_error_header("Wrong result in query!")
                            logger.print_line_sep()
                            logger.print_sql()
                            logger.print_line_sep()
                            print(f"Mismatch on row {current_row + 1}, column {c + 1}")
                            print(f"{lvalue_str} <> {rvalue_str}")
                            logger.print_line_sep()
                            logger.print_result_error(result_values_string, values, expected_column_count, row_wise)
                            context.fail("")
                        # Increment the assertion counter
                        assert success
                    current_row += 1
            else:
                current_row, current_column = 0, 0
                for i, val in enumerate(comparison_values):
                    lvalue_str = result_values_string[current_row * expected_column_count + current_column]
                    rvalue_str = val
                    success = compare_values(self, lvalue_str, rvalue_str, current_column)
                    if not success:
                        logger.print_error_header("Wrong result in query!")
                        logger.print_line_sep()
                        logger.print_sql()
                        logger.print_line_sep()
                        print(f"Mismatch on row {current_row + 1}, column {current_column + 1}")
                        print(f"{lvalue_str} <> {rvalue_str}")
                        logger.print_line_sep()
                        logger.print_result_error(result_values_string, values, expected_column_count, row_wise)
                        context.fail("")
                    # Increment the assertion counter
                    assert success

                    current_column += 1
                    if current_column == expected_column_count:
                        current_row += 1
                        current_column = 0

            if column_count_mismatch:
                logger.column_count_mismatch_correct_result(original_expected_columns, expected_column_count, self)
                context.fail("")
        else:
            hash_compare_error = False
            if query_has_label:
                entry = runner.hash_label_map.get(query_label)
                if entry is None:
                    runner.hash_label_map[query_label] = hash_value
                    runner.result_label_map[query_label] = self
                else:
                    hash_compare_error = entry != hash_value

            if is_hash:
                hash_compare_error = values[0] != hash_value

            if hash_compare_error:
                expected_result = runner.result_label_map.get(query_label)
                # logger.wrong_result_hash(expected_result, self)
                context.fail(query)

            assert not hash_compare_error


class SQLLogicConnectionPool:
    __slots__ = [
        'connection',
        'cursors',
    ]

    def __init__(self, con: duckdb.DuckDBPyConnection):
        assert con
        self.cursors = {}
        self.connection = con

    def initialize_connection(self, context: "SQLLogicContext", con):
        runner = context.runner
        if runner.test.is_sqlite_test():
            con.execute("SET integer_division=true")
        try:
            con.execute("SET timezone='UTC'")
        except duckdb.Error:
            pass
        env_var = os.getenv("LOCAL_EXTENSION_REPO")
        if env_var:
            con.execute("SET autoload_known_extensions=True")
            con.execute(f"SET autoinstall_extension_repository='{env_var}'")

    def get_connection(self, name: Optional[str] = None) -> duckdb.DuckDBPyConnection:
        """
        Either fetch the 'self.connection' object if name is None
        Or get-or-create the cursor identified by name
        """
        assert self.connection
        if name is None:
            return self.connection

        if name not in self.cursors:
            # TODO: do we need to run any set up on a new named connection ??
            self.cursors[name] = self.connection.cursor()
        return self.cursors[name]


class SQLLogicDatabase:
    __slots__ = ['path', 'database', 'config']

    def __init__(
        self, path: str, context: Optional["SQLLogicContext"] = None, additional_config: Optional[Dict[str, str]] = None
    ):
        """
        Connection Hierarchy:

        database
        └── connection
            └── cursor1
            └── cursor2
            └── cursor3

        'connection' is a cursor of 'database'.
        Every entry of 'cursors' is a cursor created from 'connection'.

        This is important to understand how ClientConfig settings affect each cursor.
        """
        self.reset()
        if additional_config:
            self.config.update(additional_config)
        self.path = path

        # Now re-open the current database
        read_only = 'access_mode' in self.config and self.config['access_mode'] == 'read_only'
        if 'access_mode' not in self.config:
            self.config['access_mode'] = 'automatic'
        self.database = duckdb.connect(path, read_only, self.config)

        # Load any previously loaded extensions again
        if context:
            for extension in context.runner.extensions:
                self.load_extension(context, extension)

    def reset(self):
        self.database: Optional[duckdb.DuckDBPyConnection] = None
        self.config: Dict[str, Any] = {
            'allow_unsigned_extensions': True,
            'allow_unredacted_secrets': True,
        }
        self.path = ''

    def load_extension(self, context: "SQLLogicContext", extension: str):
        if extension in BUILTIN_EXTENSIONS:
            # No need to load
            return
        path = context.get_extension_path(extension)
        # Serialize it as a POSIX compliant path
        query = f"LOAD '{path}'"
        self.database.execute(query)

    def connect(self) -> SQLLogicConnectionPool:
        return SQLLogicConnectionPool(self.database.cursor())


def is_regex(input: str) -> bool:
    return input.startswith("<REGEX>:") or input.startswith("<!REGEX>:")


def matches_regex(input: str, actual_str: str) -> bool:
    if input.startswith("<REGEX>:"):
        should_match = True
        regex_str = input.replace("<REGEX>:", "")
    else:
        should_match = False
        regex_str = input.replace("<!REGEX>:", "")
    # The exact match will never be the same, allow leading and trailing messages
    if regex_str[:2] != '.*':
        regex_str = ".*" + regex_str
    if regex_str[-2:] != '.*':
        regex_str = regex_str + '.*'

    re_options = re.DOTALL
    re_pattern = re.compile(regex_str, re_options)
    regex_matches = bool(re_pattern.fullmatch(actual_str))
    return regex_matches == should_match


def has_external_access(conn):
    # this is required for the python tester to work, as we make use of replacement scans
    try:
        res = conn.sql("select current_setting('enable_external_access')").fetchone()[0]
        return res
    except duckdb.TransactionException:
        return True
    except duckdb.BinderException:
        return True
    except duckdb.InvalidInputException:
        return True


def compare_values(result: QueryResult, actual_str, expected_str, current_column):
    error = False

    if actual_str == expected_str:
        return True

    if is_regex(expected_str):
        return matches_regex(expected_str, actual_str)

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


def load_result_from_file(fname, result: QueryResult):
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={os.cpu_count()}")
    column_count = result.column_count

    fname = fname.replace("<FILE>:", "")

    struct_definition = "STRUCT_PACK("
    for i in range(column_count):
        if i > 0:
            struct_definition += ", "
        struct_definition += f"c{i} := VARCHAR"
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
    column_count = result.column_count

    for r in range(row_count):
        for c in range(column_count):
            value = result.get_value(c, r)
            converted_value = sql_logic_test_convert_value(value, result.types[c], is_sqlite_test)
            out_result.append(converted_value)

    return out_result


class SQLLogicRunner:
    __slots__ = [
        'skipped',
        'error',
        'skip_level',
        'loaded_databases',
        'database',
        'extensions',
        'environment_variables',
        'test',
        'hash_threshold',
        'hash_label_map',
        'result_label_map',
        'required_requires',
        'output_hash_mode',
        'output_result_mode',
        'debug_mode',
        'finished_processing_file',
        'ignore_error_messages',
        'always_fail_error_messages',
        'original_sqlite_test',
        'build_directory',
        'skip_reload',  # <-- used for 'force_reload' and 'force_storage', unused for now
    ]

    def reset(self):
        self.skip_level: int = 0

        # The set of databases that have been loaded by this runner at any point
        # Used for cleanup
        self.loaded_databases: typing.Set[str] = set()
        self.database: Optional[SQLLogicDatabase] = None
        self.extensions = set(BUILTIN_EXTENSIONS)
        self.environment_variables: Dict[str, str] = {}
        self.test: Optional[SQLLogicTest] = None

        self.hash_threshold: int = 0
        self.hash_label_map: Dict[str, str] = {}
        self.result_label_map: Dict[str, Any] = {}

        # FIXME: create a CLI argument for this
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

    def __init__(self, build_directory: Optional[str] = None):
        self.reset()
        self.build_directory = build_directory

    def skip(self):
        self.skip_level += 1

    def unskip(self):
        self.skip_level -= 1

    def skip_active(self) -> bool:
        return self.skip_level > 0

    def is_required(self, param):
        return param in self.required_requires


class SQLLogicContext:
    __slots__ = [
        'iterator',
        'runner',
        'generator',
        'STATEMENTS',
        'pool',
        'statements',
        'current_statement',
        'keywords',
        'error',
        'is_loop',
        'is_parallel',
        'build_directory',
        'cached_config_settings',
    ]

    def reset(self):
        self.iterator = 0

    def replace_keywords(self, input: str):
        # Apply a replacement for every registered keyword
        if '__BUILD_DIRECTORY__' in input:
            self.skiptest("Test contains __BUILD_DIRECTORY__ which isnt supported")
        for key, value in self.keywords.items().__reversed__():
            input = input.replace(key, value)
        return input

    def get_extension_path(self, extension: str):
        if self.runner.build_directory is None:
            self.skiptest("Tried to load an extension, but --build-dir was not set!")
        root = self.runner.build_directory
        path = os.path.join(root, "extension", extension, f"{extension}.duckdb_extension")
        return path

    def __init__(
        self,
        pool: SQLLogicConnectionPool,
        runner: SQLLogicRunner,
        statements: List[BaseStatement],
        keywords: Dict[str, str],
        iteration_generator,
    ):
        self.statements = statements
        self.runner = runner
        self.is_loop = True
        self.is_parallel = False
        self.error: Optional[TestException] = None
        self.generator: Generator[Any] = iteration_generator
        self.keywords = keywords
        self.cached_config_settings: List[Tuple[str, str]] = []
        self.current_statement: Optional[SQLLogicStatementData] = None
        self.pool: Optional[SQLLogicConnectionPool] = pool
        self.STATEMENTS = {
            Query: self.execute_query,
            Statement: self.execute_statement,
            RequireEnv: self.execute_require_env,
            Require: self.execute_require,
            Load: self.execute_load,
            Skip: self.execute_skip,
            Unskip: self.execute_unskip,
            Mode: self.execute_mode,
            Sleep: self.execute_sleep,
            Reconnect: self.execute_reconnect,
            Halt: self.execute_halt,
            Restart: self.execute_restart,
            HashThreshold: self.execute_hash_threshold,
            Set: self.execute_set,
            Unzip: self.execute_unzip,
            Loop: self.execute_loop,
            Foreach: self.execute_foreach,
            Endloop: None,  # <-- should never be encountered outside of Loop/Foreach
        }

    def add_keyword(self, key, value):
        # Make sure that loop names can't silently collide
        key = f'${{{key}}}'
        assert key not in self.keywords
        self.keywords[key] = str(value)

    def remove_keyword(self, key):
        key = f'${{{key}}}'
        assert key in self.keywords
        self.keywords.pop(key)

    def fail(self, message):
        self.error = FailException(self.current_statement, message)
        raise self.error

    def skiptest(self, message: str):
        self.error = SkipException(self.current_statement, message)
        raise self.error

    def in_loop(self) -> bool:
        return self.is_loop

    def get_connection(self, name: Optional[str] = None) -> duckdb.DuckDBPyConnection:
        return self.pool.get_connection(name)

    def execute_load(self, load: Load):
        if self.in_loop():
            # FIXME: should add support for this, the CPP tester supports this
            self.skiptest("load cannot be called in a loop")
            # self.fail("load cannot be called in a loop")

        readonly = load.readonly

        if load.header.parameters:
            dbpath = load.header.parameters[0]
            dbpath = self.replace_keywords(dbpath)
            if not readonly:
                # delete the target database file, if it exists
                self.runner.delete_database(dbpath)
        else:
            dbpath = ""
        self.runner.loaded_databases.add(dbpath)

        # set up the config file
        additional_config = {}
        if readonly:
            additional_config['temp_directory'] = ""
            additional_config['access_mode'] = 'read_only'
        else:
            additional_config['access_mode'] = 'automatic'

        self.pool = None
        self.runner.database = None
        self.runner.database = SQLLogicDatabase(dbpath, self, additional_config)
        self.pool = self.runner.database.connect()

    def execute_query(self, query: Query):
        assert isinstance(query, Query)
        conn = self.get_connection(query.connection_name)
        if not has_external_access(conn):
            self.skiptest("enable_external_access is explicitly disabled by the test")
        sql_query = '\n'.join(query.lines)
        sql_query = self.replace_keywords(sql_query)

        expected_result = query.expected_result
        assert expected_result.type == ExpectedResult.Type.SUCCESS

        try:
            statements = conn.extract_statements(sql_query)
            statement = statements[-1]
            if 'pivot' in sql_query and len(statements) != 1:
                self.skiptest("Can not deal properly with a PIVOT statement")

            def is_query_result(sql_query, statement) -> bool:
                if duckdb.ExpectedResultType.QUERY_RESULT not in statement.expected_result_type:
                    return False
                if statement.type in [
                    duckdb.StatementType.DELETE,
                    duckdb.StatementType.UPDATE,
                    duckdb.StatementType.INSERT,
                ]:
                    if 'returning' not in sql_query.lower():
                        return False
                    return True
                if statement.type in [duckdb.StatementType.COPY]:
                    if 'return_files' not in sql_query.lower():
                        return False
                    return True
                return len(statement.expected_result_type) == 1

            if is_query_result(sql_query, statement):
                original_rel = conn.query(sql_query)
                if original_rel is None:
                    query_result = QueryResult([(0,)], ['BIGINT'])
                else:
                    original_types = original_rel.types
                    # We create new names for the columns, because they might be duplicated
                    aliased_columns = [f'c{i}' for i in range(len(original_types))]

                    expressions = [f'"{name}"::VARCHAR' for name, sql_type in zip(aliased_columns, original_types)]
                    aliased_table = ", ".join(aliased_columns)
                    expression_list = ", ".join(expressions)
                    try:
                        # Select from the result, converting the Values to the right type for comparison
                        transformed_query = (
                            f"select {expression_list} from original_rel unnamed_subquery_blabla({aliased_table})"
                        )
                        stringified_rel = conn.query(transformed_query)
                    except duckdb.Error as e:
                        self.fail(f"Could not select from the ValueRelation: {str(e)}")
                    result = stringified_rel.fetchall()
                    query_result = QueryResult(result, original_types)
            elif duckdb.ExpectedResultType.CHANGED_ROWS in statement.expected_result_type:
                conn.execute(sql_query)
                result = conn.fetchall()
                query_result = QueryResult(result, [duckdb.typing.BIGINT])
            else:
                conn.execute(sql_query)
                result = conn.fetchall()
                query_result = QueryResult(result, [])
            if expected_result.lines == None:
                return
        except duckdb.Error as e:
            print(e)
            query_result = QueryResult([], [], e)

        query_result.check(self, query)

    def execute_skip(self, statement: Skip):
        self.runner.skip()

    def execute_unzip(self, statement: Unzip):
        import gzip
        import shutil

        source = self.replace_keywords(statement.source)
        destination = self.replace_keywords(statement.destination)

        with gzip.open(source, 'rb') as f_in:
            with open(destination, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Extracted to '{destination}'")

    def execute_unskip(self, statement: Unskip):
        self.runner.unskip()

    def execute_halt(self, statement: Halt):
        self.skiptest("HALT was encountered in file")

    def execute_restart(self, statement: Restart):
        if self.is_parallel:
            self.fail("Cannot restart database in parallel")

        old_settings = self.cached_config_settings

        path = self.runner.database.path
        self.pool = None
        self.runner.database = None
        gc.collect()
        self.runner.database = SQLLogicDatabase(path, self)
        self.pool = self.runner.database.connect()
        con = self.pool.get_connection()
        for setting in old_settings:
            name, value = setting
            if name in [
                'access_mode',
                'enable_external_access',
                'allow_unsigned_extensions',
                'allow_unredacted_secrets',
                'duckdb_api',
            ]:
                # Can not be set after initialization
                continue
            if name in ['profiling_mode', 'enable_profiling']:
                # FIXME: 'profiling_mode' becomes "standard" when requested, but that's not actually the default setting
                continue
            query = f"set {name}='{value}'"
            con.execute(query)

    def execute_set(self, statement: Set):
        option = statement.header.parameters[0]
        string_set = (
            self.runner.ignore_error_messages
            if option == "ignore_error_messages"
            else self.runner.always_fail_error_messages
        )
        string_set.clear()
        string_set = statement.error_messages

    def execute_hash_threshold(self, statement: HashThreshold):
        self.runner.hash_threshold = statement.threshold

    def execute_reconnect(self, statement: Reconnect):
        if self.is_parallel:
            self.fail("reconnect can not be used inside a parallel loop")
        self.pool = None
        self.pool = self.runner.database.connect()
        con = self.pool.get_connection()
        self.pool.initialize_connection(self, con)

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
            self.runner.output_hash_mode = True
        elif parameter == "output_result":
            self.runner.output_result_mode = True
        elif parameter == "no_output":
            self.runner.output_hash_mode = False
            self.runner.output_result_mode = False
        elif parameter == "debug":
            self.runner.debug_mode = True
        else:
            raise RuntimeError("unrecognized mode: " + parameter)

    def execute_statement(self, statement: Statement):
        assert isinstance(statement, Statement)
        conn = self.get_connection(statement.connection_name)
        if not has_external_access(conn):
            self.skiptest("enable_external_access is explicitly disabled by the test")

        sql_query = '\n'.join(statement.lines)
        sql_query = self.replace_keywords(sql_query)

        expected_result = statement.expected_result
        try:
            conn.execute(sql_query)
            result = conn.fetchall()
            if expected_result.type == ExpectedResult.Type.ERROR:
                self.fail(f"Query unexpectedly succeeded")
            if expected_result.type != ExpectedResult.Type.UNKNOWN:
                assert expected_result.lines == None
        except duckdb.Error as e:
            if expected_result.type == ExpectedResult.Type.SUCCESS:
                self.fail(f"Query unexpectedly failed: {str(e)}")
            if expected_result.lines == None:
                return
            expected = '\n'.join(expected_result.lines)
            if is_regex(expected):
                if not matches_regex(expected, str(e)):
                    self.fail(
                        f"Query failed, but did not produce the right error: {expected}\nInstead it produced: {str(e)}"
                    )
            else:
                # Sanitize the expected error
                if expected.startswith('Dependency Error: '):
                    expected = expected.split('Dependency Error: ')[1]
                if expected not in str(e):
                    self.fail(
                        f"Query failed, but did not produce the right error: {expected}\nInstead it produced: {str(e)}"
                    )

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
            "no_alternative_verify",
        ]
        param = statement.header.parameters[0].lower()
        if param in not_an_extension:
            if param == 'vector_size':
                required_vector_size = int(statement.header.parameters[1])
                if duckdb.__standard_vector_size__ < required_vector_size:
                    return RequireResult.MISSING
                return RequireResult.PRESENT
            if param == 'exact_vector_size':
                required_vector_size = int(statement.header.parameters[1])
                if duckdb.__standard_vector_size__ == required_vector_size:
                    return RequireResult.PRESENT
                return RequireResult.MISSING
            if param == 'skip_reload':
                self.runner.skip_reload = True
                return RequireResult.PRESENT
            return RequireResult.MISSING

        # Already loaded
        if param in self.runner.extensions:
            return RequireResult.PRESENT

        if self.runner.build_directory is None:
            return RequireResult.MISSING

        connection = self.pool.get_connection()
        autoload_known_extensions = connection.execute(
            "select value::BOOLEAN from duckdb_settings() where name == 'autoload_known_extensions'"
        ).fetchone()[0]
        if param == "no_extension_autoloading":
            if autoload_known_extensions:
                # If autoloading is on, we skip this test
                return RequireResult.MISSING
            return RequireResult.PRESENT

        excluded_from_autoloading = True
        for ext in self.runner.AUTOLOADABLE_EXTENSIONS:
            if ext == param:
                excluded_from_autoloading = False
                break

        if autoload_known_extensions == False:
            try:
                self.runner.database.load_extension(self, param)
                self.runner.extensions.add(param)
            except duckdb.Error:
                return RequireResult.MISSING
        elif excluded_from_autoloading:
            return RequireResult.MISSING

        return RequireResult.PRESENT

    def execute_require(self, statement: Require):
        require_result = self.check_require(statement)
        if require_result != RequireResult.MISSING:
            return
        param = statement.header.parameters[0].lower()
        if self.runner.is_required(param):
            # This extension / setting was explicitly required
            self.fail("require {}: FAILED".format(param))
        self.skiptest(f"require {param}: Missing")

    def execute_require_env(self, statement: RequireEnv):
        key = statement.header.parameters[0]
        res = os.getenv(key)
        if self.in_loop():
            # FIXME: we can just remove the keyword at the end of the loop
            # I think we should support this
            # ... actually the way we set up keywords here, this is already the behavior
            # inside the python sqllogic runner, since contexts are created and destroyed at loop start and end
            self.skiptest(f"require-env can not be called in a loop")
        if res is None:
            self.skiptest(f"require-env {key} failed, not set")
        if len(statement.header.parameters) != 1:
            expected = statement.header.parameters[1]
            if res != expected:
                self.skiptest(f"require-env {key} failed, expected '{expected}', but found '{res}'")
        self.add_keyword(key, res)

    def get_loop_statements(self):
        saved_iterator = self.iterator
        # Loop until EndLoop is found
        statement = None
        depth = 0
        while self.iterator < len(self.statements):
            statement = self.next_statement()
            if statement.__class__ in [Foreach, Loop]:
                depth += 1
            if statement.__class__ == Endloop:
                if depth == 0:
                    break
                depth -= 1
        if not statement or statement.__class__ != Endloop:
            raise Exception("no corresponding 'endloop' found before the end of the file!")
        statements = self.statements[saved_iterator : self.iterator - 1]
        return statements

    def execute_parallel(self, context: "SQLLogicContext", key, value):
        context.is_parallel = True
        try:
            # For some reason the lambda won't capture the 'value' when created outside of 'execute_parallel'
            def update_value(context: SQLLogicContext) -> Generator[Any, Any, Any]:
                context.add_keyword(key, value)
                yield None
                context.remove_keyword(key)

            context.generator = update_value
            context.execute()
        except TestException:
            assert context.error is not None

    def execute_loop(self, loop: Loop):
        statements = self.get_loop_statements()

        if not loop.parallel:
            # Every iteration the 'value' of the loop key needs to change
            def update_value(context: SQLLogicContext) -> Generator[Any, Any, Any]:
                key = loop.name
                for val in range(loop.start, loop.end):
                    context.add_keyword(key, val)
                    yield None
                    context.remove_keyword(key)

            loop_context = SQLLogicContext(self.pool, self.runner, statements, self.keywords.copy(), update_value)
            try:
                loop_context.execute()
            except TestException:
                self.error = loop_context.error
        else:
            contexts: Dict[Tuple[str, int], Any] = {}
            for val in range(loop.start, loop.end):
                # FIXME: these connections are expected to have the same settings
                # So we need to apply the cached settings to them
                contexts[(loop.name, val)] = SQLLogicContext(
                    self.runner.database.connect(),
                    self.runner,
                    statements,
                    self.keywords.copy(),
                    None,  # generator, can't be created yet
                )

            threads = []
            for keyval, context in contexts.items():
                key, value = keyval
                t = threading.Thread(target=self.execute_parallel, args=(context, key, value))
                threads.append(t)
                t.start()

            for thread in threads:
                thread.join()

            for _, context in contexts.items():
                if context.error is not None:
                    # Propagate the exception
                    self.error = context.error
                    raise self.error

    def execute_foreach(self, foreach: Foreach):
        statements = self.get_loop_statements()

        if not foreach.parallel:
            # Every iteration the 'value' of the loop key needs to change
            def update_value(context: SQLLogicContext) -> Generator[Any, Any, Any]:
                loop_keys = foreach.name.split(',')

                for val in foreach.values:
                    if len(loop_keys) != 1:
                        values = val.split(',')
                    else:
                        values = [val]
                    assert len(values) == len(loop_keys)
                    for i, key in enumerate(loop_keys):
                        context.add_keyword(key, values[i])
                    yield None
                    for key in loop_keys:
                        context.remove_keyword(key)

            loop_context = SQLLogicContext(self.pool, self.runner, statements, self.keywords.copy(), update_value)
            loop_context.execute()
        else:
            # parallel loop: launch threads
            contexts: List[Tuple[str, int, Any]] = []
            loop_keys = foreach.name.split(',')
            for val in foreach.values:
                if len(loop_keys) != 1:
                    values = val.split(',')
                else:
                    values = [val]

                assert len(values) == len(loop_keys)
                for i, key in enumerate(loop_keys):
                    contexts.append(
                        (
                            foreach.name,
                            values[i],
                            SQLLogicContext(
                                self.runner.database.connect(),
                                self.runner,
                                statements,
                                self.keywords.copy(),
                                None,  # generator, can't be created yet
                            ),
                        )
                    )

            threads = []
            for x in contexts:
                key, value, context = x
                t = threading.Thread(target=self.execute_parallel, args=(context, key, value))
                threads.append(t)
                t.start()

            for thread in threads:
                thread.join()

            for x in contexts:
                _, _, context = x
                if context.error is not None:
                    self.error = context.error
                    raise self.error

    def next_statement(self):
        if self.iterator >= len(self.statements):
            raise Exception("'next_statement' out of range, statements already consumed")
        statement = self.statements[self.iterator]
        self.iterator += 1
        return statement

    def verify_statements(self) -> None:
        unsupported_statements = [
            statement for statement in self.statements if statement.__class__ not in self.STATEMENTS.keys()
        ]
        if unsupported_statements == []:
            return
        types = set([x.__class__ for x in unsupported_statements])
        error = f'skipped because the following statement types are not supported: {str(list([x for x in types]))}'
        self.skiptest(error)

    def update_settings(self):
        # Because we need to fire a query to get the settings required for 'restart'
        # we do this preemptively before executing a statement
        con = self.pool.get_connection()
        try:
            self.cached_config_settings = con.execute(
                "select name, value from duckdb_settings() where value != 'NULL' and value != ''"
            ).fetchall()
        except duckdb.Error:
            pass

    def execute(self):
        try:
            for _ in self.generator(self):
                self.reset()
                while self.iterator < len(self.statements):
                    statement = self.next_statement()
                    self.current_statement = SQLLogicStatementData(self.runner.test, statement)
                    if self.runner.skip_active() and statement.__class__ != Unskip:
                        # Keep skipping until Unskip is found
                        continue
                    if statement.get_decorators() != []:
                        self.skiptest("Decorators are not supported yet")
                    method = self.STATEMENTS.get(statement.__class__)
                    if not method:
                        self.skiptest("Not supported by the runner")
                    self.update_settings()
                    method(statement)
        except TestException as e:
            raise (e)
        return ExecuteResult(ExecuteResult.Type.SUCCESS)
