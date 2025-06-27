import itertools
import pathlib
import pytest
import random
import re
import typing
import warnings
import glob
from .skipped_tests import SKIPPED_TESTS

SQLLOGIC_TEST_CASE_NAME = "test_sqllogic"
SQLLOGIC_TEST_PARAMETER = "test_script_path"
DUCKDB_ROOT_DIR = pathlib.Path(__file__).parent.joinpath("../../..").resolve()


def pytest_addoption(parser: pytest.Parser):
    parser.addoption(
        "--test-dir",
        action="extend",
        nargs="*",
        type=pathlib.Path,
        default=[],  # We handle default in pytest_generate_tests
        dest="test_dirs",
        help="Path to one or more directories containing SQLLogic test scripts",
    )
    parser.addoption(
        "--path",
        type=str,
        default=None,
        dest="path",
        help="Path (or glob) of the tests to run",
    )
    parser.addoption(
        "--build-dir",
        type=str,
        dest="build_dir",
        help="Path to the build directory, used for loading extensions",
    )
    parser.addoption("--start-offset", type=int, dest="start_offset", help="Index of the first test to run")
    parser.addoption("--end-offset", type=int, dest="end_offset", help="Index of the last test to run")
    parser.addoption(
        "--order",
        choices=["decl", "lex", "rand"],
        default="decl",
        dest="order",
        help="Specifies the execution order of tests",
    )
    parser.addoption("--rng-seed", type=int, dest="rng_seed", help="Random integer seed")


@pytest.hookimpl(hookwrapper=True)
def pytest_keyboard_interrupt(excinfo: pytest.ExceptionInfo):
    # Ensure all tests are properly cleaned up on keyboard interrupt
    from .test_sqllogic import test_sqllogic

    if hasattr(test_sqllogic, 'executor') and test_sqllogic.executor:
        if test_sqllogic.executor.database and hasattr(test_sqllogic.executor.database, 'connection'):
            test_sqllogic.executor.database.connection.interrupt()
        test_sqllogic.executor.cleanup()
        test_sqllogic.executor = None
    yield


def pytest_configure(config: pytest.Config):
    rng_seed = config.getoption("rng_seed")
    if rng_seed is not None:
        random.seed(rng_seed)

    # Custom marker used to run all tests
    config.addinivalue_line("markers", "all")
    # These markers are used for .test_slow and .test_coverage files
    config.addinivalue_line("markers", "slow")
    config.addinivalue_line("markers", "coverage")


def get_test_id(path: pathlib.Path, root_dir: pathlib.Path, config: pytest.Config) -> str:
    # Test IDs are the path of the script starting from the test/ directory.
    return str(path.relative_to(root_dir.parent))


def get_test_marks(path: pathlib.Path, root_dir: pathlib.Path, config: pytest.Config) -> typing.List[typing.Any]:
    # Tests are tagged with the their category (i.e., name of their parent directory)
    category = path.parent.name

    for mark in config.getini("markers"):
        # Look for MarkDecorator object with the same name as the category
        if mark == category or (hasattr(mark, "markname") and mark.markname.startswith(category)):
            break
    else:
        # If the category is not in the markers, add it
        config.addinivalue_line("markers", category)

    marks = [pytest.mark.all, pytest.mark.__getattr__(category)]

    test_id = get_test_id(path, root_dir, config)
    if test_id in SKIPPED_TESTS:
        marks.append(pytest.mark.skip(reason="Test is on SKIPPED_TESTS list"))

    if test_id.endswith(".test_slow"):
        marks.append(pytest.mark.slow)
    if test_id.endswith(".test_coverage"):
        marks.append(pytest.mark.coverage)

    return marks


def create_parameters_from_paths(paths, root_dir: pathlib.Path, config: pytest.Config) -> typing.Iterator[typing.Any]:
    return map(
        lambda path: pytest.param(
            path.absolute(), id=get_test_id(path, root_dir, config), marks=get_test_marks(path, root_dir, config)
        ),
        paths,
    )


def scan_for_test_scripts(root_dir: pathlib.Path, config: pytest.Config) -> typing.Iterator[typing.Any]:
    """
    Scans for .test files in the given directory and its subdirectories.
    Returns an iterator of pytest parameters (argument, id and marks).
    """

    # TODO: Add tests from extensions
    test_script_extensions = [".test", ".test_slow", ".test_coverage"]
    it = itertools.chain.from_iterable(root_dir.rglob(f"*{ext}") for ext in test_script_extensions)
    return create_parameters_from_paths(it, root_dir, config)


def pytest_generate_tests(metafunc: pytest.Metafunc):
    # test_sqllogic (a.k.a SQLLOGIC_TEST_CASE_NAME) is defined in test_sqllogic.py
    if metafunc.definition.name != SQLLOGIC_TEST_CASE_NAME:
        return

    test_dirs: typing.List[pathlib.Path] = metafunc.config.getoption("test_dirs")
    test_glob: typing.Optional[pathlib.Path] = metafunc.config.getoption("path")

    parameters = []

    if test_glob:
        test_paths = DUCKDB_ROOT_DIR.rglob(test_glob)
        parameters.extend(create_parameters_from_paths(test_paths, DUCKDB_ROOT_DIR, metafunc.config))

    for test_dir in test_dirs:
        # Create absolute & normalized path
        test_dir = test_dir.resolve()
        assert test_dir.is_dir()
        parameters.extend(scan_for_test_scripts(test_dir, metafunc.config))

    if parameters == []:
        if len(test_dirs) == 0:
            # Use DuckDB's test directory as the default when no paths are provided
            parameters.extend(scan_for_test_scripts(DUCKDB_ROOT_DIR / "test", metafunc.config))

    metafunc.parametrize(SQLLOGIC_TEST_PARAMETER, parameters)


# Execute last, after pytest has already deselected tests based on -k and -m parameters
@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(session: pytest.Session, config: pytest.Config, items: list[pytest.Item]):
    if len(items) == 0:
        warnings.warn("No tests were found. Check that you passed the correct directory via --tests-dir.")
        return

    # Check if specific test cases to run were passed as arguments, if an expression to match test casees was specified with -k,
    # or if markers were passed with -m.
    # If none of these are true, we run all .test files, but not .test_slow or .test_coverage, and no tests that are on the SKIPPED_TESTS list.
    specific_test_args_pattern = re.compile(r"test_sqllogic\[.*\]")
    is_default_run = (
        not config.option.markexpr.strip()
        and not config.option.keyword.strip()
        and not any(specific_test_args_pattern.search(arg) for arg in config.args)
    )
    if is_default_run:
        selected_items = []
        deselected_items = []
        for test_case in items:
            # Extract the name of the SQLLogic script which is between the brackets in the test case name.
            # The test case name looks something like this: test_sqllogic[test/extension/autoloading_reset_setting.test]
            sqllogic_test_name = test_case.name[test_case.name.find("[") + 1 : test_case.name.find("]")]
            if sqllogic_test_name.endswith(".test"):
                selected_items.append(test_case)
            else:
                deselected_items.append(test_case)

        config.hook.pytest_deselected(items=deselected_items)
        items[:] = selected_items

    start_offset = config.getoption("start_offset")
    if start_offset is None:
        start_offset = 0

    end_offset = config.getoption("end_offset")
    if end_offset is None:
        end_offset = len(items) - 1

    if start_offset < 0:
        raise ValueError("--start-offset must be a non-negative integer")
    elif end_offset < start_offset:
        raise ValueError(f"--end-offset ({end_offset}) must be greater than or equal to --start-offset")

    max_end_offset = len(items) - 1
    if end_offset > max_end_offset:
        end_offset = max_end_offset

    # Order tests based on --order option. Take as is if order is "decl".
    if config.getoption("order") == "rand":
        random.shuffle(items)
    elif config.getoption("order") == "lex":
        items.sort(key=lambda item: item.name)

    for index, item in enumerate(items):
        # Store some information that are later used in pytest_runtest_logreport.
        # We store the test index after sorting but before deselecting to match start and end offset.
        item.user_properties.append(("test_index", index))
        item.user_properties.append(("total_num_tests", len(items)))
        item.user_properties.append(
            ("should_print_progress", config.get_verbosity() > 0 and config.getoption("capture") == "no")
        )

    deselected_items = items[:start_offset] + items[end_offset + 1 :]
    config.hook.pytest_deselected(items=deselected_items)
    items[:] = items[start_offset : end_offset + 1]


def pytest_runtest_setup(item: pytest.Item):
    """
    Show the test index after the test name
    """

    def get_from_tuple_list(tuples, key):
        for t in tuples:
            if t[0] == key:
                return t[1]
        return None

    if get_from_tuple_list(item.user_properties, "should_print_progress"):
        idx = get_from_tuple_list(item.user_properties, "test_index")
        # index is 0-based, but total_num_tests 1-based
        max_idx = get_from_tuple_list(item.user_properties, "total_num_tests") - 1
        print(f"[{idx}/{max_idx}]", end=" ", flush=True)
