import itertools
import pathlib
import pytest
import random
import typing
import warnings
from .skipped_tests import SKIPPED_TESTS

SQLLOGIC_TEST_CASE_NAME = "test_sqllogic"
SQLLOGIC_TEST_PARAMETER = "test_script_path"


def pytest_addoption(parser: pytest.Parser):
    parser.addoption(
        "--test-dir",
        required=True,
        action="extend",
        nargs="+",
        type=pathlib.Path,
        dest="test_dirs",
        help="Path to one or more directories containing SQLLogic test scripts",
    )
    parser.addoption("--start-offset", type=int, dest="start_offset", help="Index of the first test to run")
    parser.addoption("--end-offset", type=int, dest="end_offset", help="Index of the last test to run")
    parser.addoption("--order", choices=["decl", "lex", "rand"], default="decl", dest="order", help="Specifies the execution order of tests")
    parser.addoption("--rng-seed", type=int, dest="rng_seed", help="Random integer seed")


@pytest.hookimpl(tryfirst=True)
def pytest_keyboard_interrupt(excinfo: pytest.ExceptionInfo):
    # TODO: CTRL+C does not immediately interrupt pytest. You sometimes have to press it multiple times.
    pytestmark = pytest.mark.skip(reason="Keyboard interrupt")


def pytest_configure(config: pytest.Config):
    rng_seed = config.getoption("rng_seed")
    if rng_seed is not None:
        random.seed(rng_seed)
    
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

    marks = [pytest.mark.__getattr__(category)]

    test_id = get_test_id(path, root_dir, config)
    if test_id in SKIPPED_TESTS:
        marks.append(pytest.mark.skip(reason="Test is on SKIPPED_TESTS list"))

    if test_id.endswith(".test_slow"):
        marks.append(pytest.mark.slow)
    if test_id.endswith(".test_coverage"):
        marks.append(pytest.mark.coverage)
    
    
    return marks

def scan_for_test_scripts(root_dir: pathlib.Path, config: pytest.Config) -> typing.Iterator[typing.Any]:
    """
    Scans for .test files in the given directory and its subdirectories.
    Returns an iterator of pytest parameters (argument, id and marks).
    """

    # TODO: Add tests from extensions
    # TODO: Do we have to handle --ignore and --ignore-glob and --deselect options?
    test_script_extensions = [".test", ".test_slow", ".test_coverage"]
    it = itertools.chain.from_iterable(root_dir.rglob(f"*{ext}") for ext in test_script_extensions)
    return map(lambda path: pytest.param(path.absolute(), id=get_test_id(path, root_dir, config), marks=get_test_marks(path, root_dir, config)), it)

def pytest_generate_tests(metafunc: pytest.Metafunc):
    # test_sqllogic (a.k.a SQLLOGIC_TEST_CASE_NAME) is defined in test_sqllogic.py
    if metafunc.definition.name == SQLLOGIC_TEST_CASE_NAME:
        test_dirs: typing.List[pathlib.Path] = metafunc.config.getoption("test_dirs")
        assert len(test_dirs) > 0

        parameters = []
        for test_dir in test_dirs:
            # Create absolute & normalized path
            test_dir = test_dir.resolve()
            assert test_dir.is_dir()
            parameters.extend(scan_for_test_scripts(test_dir, metafunc.config))
        
        metafunc.parametrize(SQLLOGIC_TEST_PARAMETER, parameters)


# Execute last, after pytest has already deselected tests based on -k and -m parameters
@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(session: pytest.Session, config: pytest.Config, items: list[pytest.Item]):
    if len(items) == 0:
        warnings.warn("No tests were found. Check that you passed the correct directory via --tests-dir.")
        return

    start_offset = config.getoption("start_offset")
    if start_offset is None:
        start_offset = 0

    end_offset = config.getoption("end_offset")
    if end_offset is None:
        end_offset = len(items) - 1

    if start_offset < 0:
        raise ValueError("--start-offset must be a non-negative integer")
    elif end_offset < start_offset:
        raise ValueError("--end-offset must be greater than or equal to --start-offset")
    
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
        item.user_properties.append(("report_verbosity", config.get_verbosity()))

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

    if get_from_tuple_list(item.user_properties, "report_verbosity") > 0:
        idx = get_from_tuple_list(item.user_properties, "test_index")
        # index is 0-based, but total_num_tests 1-based
        max_idx = get_from_tuple_list(item.user_properties, "total_num_tests") - 1
        print(f"[{idx}/{max_idx}]", end=" ", flush=True)