from typing import Any, Generator, List
import json
import os
import time
from zipfile import Path
from os.path import basename, dirname
import shutil

import duckdb

from duckdb_sqllogictest import SQLLogicParser
from duckdb_sqllogictest.result import (
    ExecuteResult,
    Foreach,
    Load,
    Loop,
    Query,
    Require,
    RequireResult,
    Restart,
    Set,
    SQLLogicContext,
    SQLLogicRunner,
    Statement,
    Unzip,
)

from duckdb_sqllogictest.expected_result import ExpectedResult
from duckdb_sqllogictest.statement.query import SortStyle

from utils.query_exclusions import format_exclusion_tags
from utils.logger import make_logger

BWC_TAG_PREFIX = '-- bwc_tag:'
BWC_OUTPUT_DIR_NAME = 'output'

logger = make_logger(__name__)


def make_tag(tag_key: str, tag_value: int | str | list | set) -> str:
    if isinstance(tag_value, list) or isinstance(tag_value, set):
        if len(tag_value) == 0:
            raise ValueError("Tag value list/set cannot be empty")
        tag_value = ';'.join(str(x) for x in tag_value)
    elif not isinstance(tag_value, str):
        tag_value = str(tag_value)
    if len(tag_value) > 0:
        return f"{BWC_TAG_PREFIX}{tag_key}={tag_value}\n"
    else:
        return f"{BWC_TAG_PREFIX}{tag_key}\n"


## List tests files
def parse_excluded_tests(path):
    exclusion_list = {}
    with open(path) as f:
        for line in f:
            stripped = line.strip()
            if len(stripped) > 0 and line[0] != '#':
                exclusion_list[stripped] = True
    return exclusion_list


def find_tests_recursive(dir):
    test_list = []
    for f in os.listdir(dir):
        path = os.path.join(dir, f)
        if os.path.isdir(path):
            test_list += find_tests_recursive(path)
        elif path.endswith('.test'):
            test_list.append(path)
    return test_list


def list_test_files(root_dir: str, test_pattern: str = None) -> List[str]:
    test_dir = os.path.join(root_dir, 'test', 'sql')
    files = find_tests_recursive(test_dir)
    files.sort()
    files = files if test_pattern is None else [x for x in files if test_pattern in x]
    len_root_dir = len(root_dir) + 1
    return [f[len_root_dir:] for f in files]


class SerializedTest:
    class TestStep:
        def __init__(self, step_type: str, step_data: Any = None):
            self.step_type = step_type
            self.step_data = step_data

    def __init__(self, duckdb_bwc_base_dir: str, duckdb_version: str, test_filename: str, test_idx: int):
        # Provided metadata
        self.duckdb_bwc_base_dir = duckdb_bwc_base_dir
        self.duckdb_version = duckdb_version
        self.test_spec_relative_path = test_filename
        self.test_idx = test_idx

        # Computed paths
        test_basename = basename(test_filename)  # eg. attach_encryption_fallback_readonly.test
        if len(test_basename) == 0:
            raise ValueError(f"Test spec relative path '{test_filename}' is not valid (basename is empty)")

        self.test_specs_base_dir = f"{duckdb_bwc_base_dir}/specs/{duckdb_version}"

        is_spec_file = test_filename.startswith('test/sql/')
        if is_spec_file:
            self.test_absolute_filename = f"{duckdb_bwc_base_dir}/specs/{duckdb_version}/{test_filename}"
            self.test_runtime_directory = f"{duckdb_bwc_base_dir}/runtime/{duckdb_version}/{dirname(test_filename)}/{test_basename.replace('.test', '')}"
        else:
            self.test_absolute_filename = os.path.join(os.getcwd(), test_filename)
            self.test_runtime_directory = (
                f"{duckdb_bwc_base_dir}/runtime/{duckdb_version}/{test_basename.replace('.test', '')}"
            )

        self.test_output_directory = f"{self.test_runtime_directory}/{BWC_OUTPUT_DIR_NAME}"
        self.queries_file_name = f"{test_basename}.sql"

        self.serialized_plans_file_name = f"{test_basename}.plan.bin"
        self.results_old_file_name = f"{test_basename}.{duckdb_version}.result.bin"
        self.results_new_file_name = f"{test_basename}.new.result.bin"  # TODO - use actual version?

        # Log all paths
        logger.debug(f"SerializedTest#{self.test_idx} initialized:")
        logger.debug(f"  test_absolute_filename: {self.test_absolute_filename}")
        logger.debug(f"  test_runtime_directory: {self.test_runtime_directory}")
        logger.debug(f"  queries_file_name: {self.queries_file_name}")
        logger.debug(f"  serialized_plans_file_name: {self.serialized_plans_file_name}")
        logger.debug(f"  results_old_file_name: {self.results_old_file_name}")
        logger.debug(f"  results_new_file_name: {self.results_new_file_name}")

        # Used during parsing
        self.loaded_from_disk = False
        self.has_cached_serialized_plans = False
        self.ignore_error_messages = False
        self.no_extension_autoloading = False
        self.nb_skipped_statements_in_loop = 0
        self.steps = []

        # Used during execution
        self.skip_reasons = []
        self.fixtures = set()
        self.nb_steps = 0
        self.needed_extensions = set()

    def add(self, step_type: str, step_data: Any = None):
        self.steps.append(self.TestStep(step_type, step_data))

    def __str__(self):
        return (
            f"SerializedTest#{self.test_idx}(test_filename={self.test_absolute_filename}, steps={self.get_nb_steps()})"
        )

    def steps_str(self):
        return '\n- '.join([f"{step.step_type}: {step.step_data}" for step in self.steps])

    def create_runtime_directories(self):
        # Create runtime directory (base + output since they are nested)
        assert self.test_runtime_directory in self.test_output_directory
        os.makedirs(self.test_output_directory, exist_ok=True)

        # Create symlink to data directory so tests can access it via 'data/'
        data_symlink = f"{self.test_runtime_directory}/data"
        if not os.path.exists(data_symlink):
            os.symlink(f"{self.test_specs_base_dir}/data", data_symlink)

        test_symlink = f"{self.test_runtime_directory}/test"
        if not os.path.exists(test_symlink):
            os.symlink(f"{self.test_specs_base_dir}/test", test_symlink)

    def reset_output_directory(self):
        """
        When generating the SQL files, we also create some output fixtures (eg. during unzip)
        At the same time, during runtime the tests will also generate output files.
        So we before each run we copy all reference output directory.
        """
        output_files = self.list_output_directory(True)
        files_to_move = set()
        for output_file in output_files:
            if output_file in self.fixtures:
                continue

            # Only move the top level folder
            f = output_file.split('/')[0] if '/' in output_file else output_file
            files_to_move.add(f)

        # Ensure output directory exists
        os.makedirs(self.test_output_directory, exist_ok=True)

        # Restore fixtures
        for fixture in self.fixtures:
            shutil.copy2(f"{self.test_runtime_directory}/fixtures/{fixture}", self.test_output_directory)

        if len(files_to_move) == 0:
            return

        src = self.test_output_directory
        target_dir = f"{src}_{time.strftime('%Y%m%d_%H%M%S')}"

        # Target dir can exist if run with old version moved it just before
        if os.path.exists(target_dir):
            target_dir = f"{target_dir}_run2"

        os.mkdir(target_dir)
        for f in files_to_move:
            shutil.move(f"{src}/{f}", f"{target_dir}/{f}")

        logger.debug(f"Moved output directory from '{src}' to '{target_dir}'")

    def reload_from_disk(self) -> bool:
        """Reload a test from disk if it already exists."""
        absolute_queries_file = f"{self.test_runtime_directory}/{self.queries_file_name}"
        if not os.path.exists(absolute_queries_file):
            return False

        logger.debug(
            f"Reloading test '{self.test_absolute_filename}' from cached queries file '{absolute_queries_file}'"
        )
        with open(absolute_queries_file, 'r') as f:
            for full_line in f.readlines():
                line = full_line.strip()
                if not line.startswith(BWC_TAG_PREFIX):
                    continue

                [tag_key, _, tag_value] = line[len(BWC_TAG_PREFIX) :].partition('=')
                if tag_key == 'skip_reasons':
                    reasons = tag_value.split(';')
                    self.skip_reasons.extend(reasons)
                    break  # No need to parse further
                elif tag_key == 'needed_extensions':
                    extensions = tag_value.split(';')
                    self.needed_extensions.update(extensions)
                elif tag_key == 'nb_steps':
                    self.nb_steps = int(tag_value)
                elif tag_key == 'fixtures':
                    self.fixtures = set(tag_value.split(';'))

        self.loaded_from_disk = True
        self.has_cached_serialized_plans = self.exists_in_runtime_dir(
            self.serialized_plans_file_name
        ) and self.exists_in_runtime_dir(self.results_old_file_name)

        # Ensure symlinks exist (they are not included in the cache archive)
        self.create_runtime_directories()

        return True

    def exists_in_runtime_dir(self, file_name) -> bool:
        return os.path.exists(f"{self.test_runtime_directory}/{file_name}")

    def get_nb_steps(self) -> int:
        return self.nb_steps if self.loaded_from_disk else len(self.steps)

    def list_output_directory(self, allow_nested_path=False) -> List[str]:
        files = []
        folder = self.test_output_directory
        for root, _, filenames in os.walk(folder):
            for filename in filenames:
                f = os.path.relpath(os.path.join(root, filename), folder)
                if not allow_nested_path:
                    assert '/' not in f, f"Nested output files are not supported for now: {f}"
                files.append(f)

        return files

    def write_test_queries(self):
        if len(self.steps) == 0 and len(self.skip_reasons) == 0:
            logger.warning(f"No step found in test file '{self.queries_file_name}'")
            return

        gen_files = self.list_output_directory()
        self.fixtures.update(gen_files)

        if len(gen_files) > 0:
            os.mkdir(f"{self.test_runtime_directory}/fixtures")
            for gen_file in gen_files:
                shutil.copy2(f"{self.test_output_directory}/{gen_file}", f"{self.test_runtime_directory}/fixtures/")

        with open(f"{self.test_runtime_directory}/{self.queries_file_name}", 'w') as f:
            if len(self.skip_reasons) > 0:
                f.write(make_tag("skip_reasons", self.skip_reasons))
                return

            if len(gen_files) > 0:
                f.write(make_tag("fixtures", ';'.join(gen_files)))

            if len(self.needed_extensions) > 0:
                f.write(make_tag("needed_extensions", self.needed_extensions))

            f.write(make_tag("nb_steps", len(self.steps)))
            for step in self.steps:
                if step.step_type == "query":
                    f.write(step.step_data)
                else:
                    raise ValueError(f"Unsupported step type: {step.step_type}")


class SerializerSQLLogicContext(SQLLogicContext):
    def __init__(self, pool, runner, statements, keywords, update_value):
        super().__init__(pool, runner, statements, keywords, update_value)
        self.current_test = runner.current_test

    def update_settings(self):
        pass  # Nothing to do

    def _normalize_query(self, statement: Statement) -> str:
        # Normalize the query by removing comments and extra whitespace
        tags = None
        if statement.expected_result is not None and statement.expected_result.type != ExpectedResult.Type.SUCCESS:
            if statement.expected_result.type == ExpectedResult.Type.ERROR:
                tags = make_tag("expected_result", "error")
            elif statement.expected_result.type == ExpectedResult.Type.UNKNOWN:
                tags = make_tag("expected_result", "unknown")
            else:
                raise ValueError(f"Unknown expected result type: {statement.expected_result.type}")

        sql_query = '\n'.join(statement.lines)
        sql_query = self.replace_keywords(sql_query)
        return sql_query if not tags else f"{tags}\n{sql_query}"

    def execute_statement(self, statement: Statement):
        if len(statement.header.parameters) > 1:
            self.skiptest("Multiple connections not supported for now")
            return

        sql_query = self._normalize_query(statement)
        sql_query_lc = sql_query.lower()
        if 'set enable_external_access=false' in sql_query_lc:
            # Tester uses DuckDB API to write files...
            self.skiptest("Can't disable external access for now")
            return
        self.add_query(sql_query)

    def execute_query(self, query: Query):
        sql_query = ""

        # Handle sortstyle
        sort_style = query.get_sortstyle()
        if sort_style is not None and sort_style != SortStyle.NO_SORT:
            sql_query = make_tag("sort", SerializerSQLLogicContext.sort_style_to_str(sort_style))

        sql_query += self._normalize_query(query)

        self.add_query(sql_query)

    @staticmethod
    def sort_style_to_str(sort_style):
        if sort_style == SortStyle.NO_SORT:
            return "no_sort"
        elif sort_style == SortStyle.ROW_SORT:
            return "row_sort"
        elif sort_style == SortStyle.VALUE_SORT:
            return "value_sort"
        else:
            raise ValueError(f"Unknown sort style: {sort_style}")

    def add_query(self, query: str):
        if self.in_loop() and self.is_parallel:
            return  # Skip queries in parallel loops

        # Add exclusion tags based on query analysis
        exclusion_tags = format_exclusion_tags(query)
        if exclusion_tags:
            query = f"{exclusion_tags}\n{query}"

        self.current_test.add('query', f"{query}\n{make_tag('end_query', '')}\n")

    def execute_load(self, load: Load):
        options = []

        if not load.header.parameters:
            self.add_query(f"USE memory;")
            return

        dbpath = self.replace_keywords(load.header.parameters[0])
        db_name = basename(dbpath).split('.')[0]
        if load.readonly:
            options.append('READ_ONLY')
        if load.version:
            options.append(f"STORAGE_VERSION {load.version}")

        opt_str = f" ({', '.join(options)})" if options and len(options) > 1 else ''
        self.add_query(f"{make_tag('load_db', db_name)}\nATTACH '{dbpath}' AS {db_name}{opt_str};")

    def execute_restart(self, statement: Restart):
        # FIXME - skipping >200 tests because of this
        self.skiptest("Restart not supported for now")

    def execute_unzip(self, statement: Unzip):
        self.do_execute_unzip(statement)

    def execute_set(self, statement: Set):
        option = statement.header.parameters[0]
        if option == 'ignore_error_messages':
            self.current_test.ignore_error_messages = True
        elif option == 'seed':
            self.add_query(f"SELECT SETSEED({statement.header.parameters[1]});")
            # TODO - self.runner.skip_reload = True
        else:
            raise ValueError(f"SET '{option}' is not implemented!")

    def execute_foreach(self, foreach: Foreach):
        if not foreach.parallel:
            super().execute_foreach(foreach)
        else:
            # Get loop statements will skip over the inner statements
            skipped_stmts = self.get_loop_statements()
            self.current_test.nb_skipped_statements_in_loop += len(skipped_stmts)

    def execute_loop(self, loop: Loop):
        if not loop.parallel:
            super().execute_loop(loop)
        else:
            # Get loop statements will skip over the inner statements
            skipped_stmts = self.get_loop_statements()
            self.current_test.nb_skipped_statements_in_loop += len(skipped_stmts)

    def skiptest(self, reason):
        if reason not in self.current_test.skip_reasons:
            self.current_test.skip_reasons.append(reason)

    def check_require(self, statement: Require) -> RequireResult:
        not_supported = set(
            [
                "no_alternative_verify",
                "noalternativeverify",
                "fts",  # TODO - need to support non-linked extensions
            ]
        )
        not_an_extension = [
            # Copied over from scripts/sqllogictest/result.py
            "64bit",
            "block_size",
            "exact_vector_size",
            "longdouble",
            "mingw",
            "no_alternative_verify",
            "noalternativeverify",
            "noforcestorage",
            "nothreadsan",
            "notmingw",
            "notwindows",
            "skip_reload",
            "strinline",
            "vector_size",
            "windows",
            # Added here:
            "no_latest_storage",
            "no_vector_verification",
            "notmusl",
            "ram",
        ]

        param = statement.header.parameters[0].lower()
        if param in not_supported:
            # self.skiptest(f"Requirement '{param}' is not supported")
            return RequireResult.MISSING
        elif param in not_an_extension:
            # TODO - skip_reload?
            return RequireResult.PRESENT
        elif param == "no_extension_autoloading":
            # This is a special case, we do not want to load extensions automatically
            self.add_query("SET autoload_known_extensions=false;")
            self.current_test.no_extension_autoloading = True
            return RequireResult.PRESENT
        elif param == "allow_unsigned_extensions":
            # We're running with this parameter for now
            return RequireResult.PRESENT

        if not self.current_test.no_extension_autoloading:
            self.add_query(f"LOAD '{param}';")
            self.current_test.needed_extensions.add(param)
        return RequireResult.PRESENT


class SQLLogicTestSerializer(SQLLogicRunner):
    def __init__(self, duckdb_bwc_base_dir: str, duckdb_version: str, test_filename: str, test_idx: int):
        super().__init__(None)
        self.current_test = SerializedTest(duckdb_bwc_base_dir, duckdb_version, test_filename, test_idx)

    def execute_test(self) -> None:
        if self.current_test.reload_from_disk():
            return

        self.current_test.create_runtime_directories()

        # Set current working directory
        os.chdir(self.current_test.test_runtime_directory)

        self.reset()
        sql_parser = SQLLogicParser()
        test_absolute_filename = self.current_test.test_absolute_filename
        try:
            test = sql_parser.parse(str(test_absolute_filename))
            if test is None:
                raise ValueError(f"Failed to parse test file (unknown reason)")
        except Exception as e:
            # Throw if exception is file not found
            if not os.path.exists(test_absolute_filename):
                raise e

            self.current_test.skip_reasons.append("parsing_error")
            self.current_test.write_test_queries()
            logger.error(f"Exception while parsing test file '{test_absolute_filename}': {e}")
            return

        self.test = test
        self.extensions = [
            "httpfs",
            "parquet",
            "tpch",
            "json",
            "icu",
            "tpcds",
            "sqlite_scanner",
            "autocomplete",
            "inet",
        ]
        self.original_sqlite_test = self.test.is_sqlite_test()

        # Top level keywords
        keywords = {
            '__TEST_DIR__': BWC_OUTPUT_DIR_NAME,
            '__WORKING_DIRECTORY__': '.',
            '{DATA_DIR}': 'data',
            '{TEMP_DIR}': BWC_OUTPUT_DIR_NAME,
            '{TEST_DIR}': BWC_OUTPUT_DIR_NAME,
        }

        def update_value(_: SQLLogicContext) -> Generator[Any, Any, Any]:
            # Yield once to represent one iteration, do not touch the keywords
            yield None

        context = SerializerSQLLogicContext(None, self, test.statements, keywords, update_value)
        context.is_loop = False  # The outer context is not a loop
        context.verify_statements()
        res = context.execute()
        assert res.type == ExecuteResult.Type.SUCCESS

        if self.skip_active() and len(self.current_test.skip_reasons) == 0:
            self.current_test.skip_reasons.append("explicitly skipped")

        if (
            len(self.current_test.skip_reasons) == 0
            and len(self.current_test.steps) == 0
            and self.current_test.nb_skipped_statements_in_loop > 0
        ):
            self.current_test.skip_reasons.append("entirely_skipped_because_of_parallel_loops")

        self.current_test.write_test_queries()


def load_skipped_tests_from_file(json_file: str) -> set[str]:
    if not os.path.exists(json_file):
        return set()

    with open(json_file, 'r') as f:
        data = json.load(f)

    skipped_tests = set()
    for entry in data.get('skip_tests', []):
        skipped_tests.update(entry.get('paths', []))

    return skipped_tests


def list_skipped_tests(duckdb_test_config_dir: str, duckdb_version: str) -> set[str]:
    base_skip_file_path = f"{duckdb_test_config_dir}/test/configs/serialization_bwc_base.json"
    version_skip_file_path = f"{duckdb_test_config_dir}/test/configs/serialization_bwc_{duckdb_version}.json"
    skipped_tests = load_skipped_tests_from_file(base_skip_file_path)
    skipped_tests.update(load_skipped_tests_from_file(version_skip_file_path))
    logger.debug(
        f"Found {len(skipped_tests)} tests to skip in version {duckdb_version} (base skip file: {base_skip_file_path}, version skip file: {version_skip_file_path})"
    )
    return skipped_tests


class LoadingStats:
    def __init__(self):
        self.skipped_count = 0
        self.skipped_count_per_reason = {}
        self.start_time = time.time()
        self.nb_steps = 0
        self.nb_tests = 0
        self.nb_cached_queries = 0
        self.nb_cached_plans = 0

    def update(self, test: SerializedTest):
        if len(test.skip_reasons) > 0:
            self.update_skipped(test.skip_reasons)
            return

        self.nb_tests += 1
        self.nb_steps += test.get_nb_steps()

        if test.loaded_from_disk:
            self.nb_cached_queries += 1

        if test.has_cached_serialized_plans:
            self.nb_cached_plans += 1

    def update_skipped(self, reasons: List[str]):
        if len(reasons) == 0:
            return

        self.skipped_count += 1
        for reason in reasons:
            if reason not in self.skipped_count_per_reason:
                self.skipped_count_per_reason[reason] = 0
            self.skipped_count_per_reason[reason] += 1

    def log_stats(self):
        logger.info(
            f"Loaded {self.nb_tests} test files (with {self.nb_steps} steps) in {time.time() - self.start_time:.2f}s"
        )
        logger.info(
            f"  Cached queries: {self.nb_cached_queries} ({(self.nb_cached_queries/self.nb_tests*100) if self.nb_tests > 0 else 0:.2f}%)"
        )
        logger.info(
            f"  Cached plans: {self.nb_cached_plans} ({(self.nb_cached_plans/self.nb_tests*100) if self.nb_tests > 0 else 0:.2f}%)"
        )
        logger.info(
            f"  Skipped {self.skipped_count} ({(self.skipped_count/self.nb_tests*100) if self.nb_tests > 0 else 0:.2f}%) test files:"
        )

        # Sort self.skipped_count_per_reason by count descending
        sorted_skipped = sorted(self.skipped_count_per_reason.items(), key=lambda x: x[1], reverse=True)
        for reason, count in sorted_skipped:
            logger.info(
                f"    {count} ({(count/self.skipped_count*100) if self.skipped_count > 0 else 0:.2f}%): {reason}"
            )


def load_test_files(
    duckdb_root_dir: str,
    duckdb_bwc_base_dir: str,
    duckdb_version: str,
    test_pattern: str = None,
    single_test_file: str = None,
) -> List:
    loading_stats = LoadingStats()

    spec_files_dir = f"{duckdb_bwc_base_dir}/specs/{duckdb_version}"
    logger.info(f"Using Python DuckDB version {duckdb.__version__} to load test files from {spec_files_dir}")

    # Normalize (remove trailing ./ etc)
    single_test_file = os.path.normpath(single_test_file) if single_test_file is not None else None

    files = [single_test_file] if single_test_file is not None else list_test_files(spec_files_dir, test_pattern)
    logger.info(f"Found {len(files)} test files")

    loaded_tests = []
    skipped_tests = (
        list_skipped_tests(duckdb_root_dir, duckdb_version)
        if test_pattern is None and single_test_file is None
        else set()
    )
    needed_extensions = set()
    for test_filename in files:
        if test_filename in skipped_tests:
            loading_stats.update_skipped([f"in skipped file"])
            continue

        try:
            serializer = SQLLogicTestSerializer(
                duckdb_bwc_base_dir, duckdb_version, test_filename, len(loaded_tests) + 1
            )
            serializer.execute_test()
            test = serializer.current_test

            loading_stats.update(test)
            if len(test.skip_reasons) == 0:
                loaded_tests.append(test)
                needed_extensions.update(test.needed_extensions)
        except Exception as e:
            logger.error(f"Failed to load test file '{test_filename}'")
            raise e

    loading_stats.log_stats()

    return {'tests': loaded_tests, 'needed_extensions': needed_extensions}
