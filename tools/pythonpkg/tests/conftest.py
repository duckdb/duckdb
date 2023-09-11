import os
import pytest
import shutil
from os.path import abspath, join, dirname, normpath
import glob
import duckdb

try:
    import pandas

    pyarrow_dtype = pandas.ArrowDtype
except:
    pyarrow_dtype = None

# Check if pandas has arrow dtypes enabled
try:
    from pandas.compat import pa_version_under7p0

    pyarrow_dtypes_enabled = not pa_version_under7p0
except:
    pyarrow_dtypes_enabled = False


# https://docs.pytest.org/en/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option
# https://stackoverflow.com/a/47700320
def pytest_addoption(parser):
    parser.addoption("--skiplist", action="append", nargs="+", type=str, help="skip listed tests")


def pytest_collection_modifyitems(config, items):
    tests_to_skip = config.getoption("--skiplist")
    if not tests_to_skip:
        # --skiplist not given in cli, therefore move on
        return

    # Combine all the lists into one
    skipped_tests = []
    for item in tests_to_skip:
        skipped_tests.extend(item)

    skip_listed = pytest.mark.skip(reason="included in --skiplist")
    for item in items:
        if item.name in skipped_tests:
            # test is named specifically
            item.add_marker(skip_listed)
        elif item.parent != None and item.parent.name in skipped_tests:
            # the class is named specifically
            item.add_marker(skip_listed)


@pytest.fixture(scope="function")
def duckdb_empty_cursor(request):
    connection = duckdb.connect('')
    cursor = connection.cursor()
    return cursor


def pandas_2_or_higher():
    from packaging.version import Version
    import pandas as pd

    return Version(pd.__version__) >= Version('2.0.0')


def pandas_supports_arrow_backend():
    try:
        from pandas.compat import pa_version_under7p0

        if pa_version_under7p0 == True:
            return False
    except:
        return False
    return pandas_2_or_higher()


def numpy_pandas_df(*args, **kwargs):
    pandas = pytest.importorskip("pandas")
    return pandas.DataFrame(*args, **kwargs)


def arrow_pandas_df(*args, **kwargs):
    df = numpy_pandas_df(*args, **kwargs)
    return df.convert_dtypes(dtype_backend="pyarrow")


class NumpyPandas:
    def __init__(self):
        self.backend = 'numpy_nullable'
        self.DataFrame = numpy_pandas_df
        self.pandas = pytest.importorskip("pandas")

    def __getattr__(self, __name: str):
        item = eval(f'self.pandas.{__name}')
        return item


def convert_arrow_to_numpy_backend(df):
    pandas = pytest.importorskip("pandas")
    names = df.columns
    df_content = {}
    for name in names:
        df_content[name] = df[name].array.__arrow_array__()
    # This should convert the pyarrow chunked arrays into numpy arrays
    return pandas.DataFrame(df_content)


def convert_to_numpy(df):
    if (
        pyarrow_dtypes_enabled
        and pyarrow_dtype != None
        and any([True for x in df.dtypes if isinstance(x, pyarrow_dtype)])
    ):
        return convert_arrow_to_numpy_backend(df)
    return df


def convert_and_equal(df1, df2, **kwargs):
    df1 = convert_to_numpy(df1)
    df2 = convert_to_numpy(df2)
    pytest.importorskip("pandas").testing.assert_frame_equal(df1, df2, **kwargs)


class ArrowMockTesting:
    def __init__(self):
        self.testing = pytest.importorskip("pandas").testing
        self.assert_frame_equal = convert_and_equal

    def __getattr__(self, __name: str):
        item = eval(f'self.testing.{__name}')
        return item


# This converts dataframes constructed with 'DataFrame(...)' to pyarrow backed dataframes
# Assert equal does the opposite, turning all pyarrow backed dataframes into numpy backed ones
# this is done because we don't produce pyarrow backed dataframes yet
class ArrowPandas:
    def __init__(self):
        self.pandas = pytest.importorskip("pandas")
        if pandas_2_or_higher() and pyarrow_dtypes_enabled:
            self.backend = 'pyarrow'
            self.DataFrame = arrow_pandas_df
        else:
            # For backwards compatible reasons, just mock regular pandas
            self.backend = 'numpy_nullable'
            self.DataFrame = self.pandas.DataFrame
        self.testing = ArrowMockTesting()

    def __getattr__(self, __name: str):
        item = eval(f'self.pandas.{__name}')
        return item


@pytest.fixture(scope="function")
def require():
    def _require(extension_name, db_name=''):
        # Paths to search for extensions

        build = normpath(join(dirname(__file__), "../../../build/"))
        extension = "extension/*/*.duckdb_extension"

        extension_search_patterns = [
            join(build, "release", extension),
            join(build, "debug", extension),
        ]

        # DUCKDB_PYTHON_TEST_EXTENSION_PATH can be used to add a path for the extension test to search for extensions
        if 'DUCKDB_PYTHON_TEST_EXTENSION_PATH' in os.environ:
            env_extension_path = os.getenv('DUCKDB_PYTHON_TEST_EXTENSION_PATH')
            env_extension_path = env_extension_path.rstrip('/')
            extension_search_patterns.append(env_extension_path + '/*/*.duckdb_extension')
            extension_search_patterns.append(env_extension_path + '/*.duckdb_extension')

        extension_paths_found = []
        for pattern in extension_search_patterns:
            extension_pattern_abs = abspath(pattern)
            print(f"Searching path: {extension_pattern_abs}")
            for path in glob.glob(extension_pattern_abs):
                extension_paths_found.append(path)

        for path in extension_paths_found:
            print(path)
            if path.endswith(extension_name + ".duckdb_extension"):
                conn = duckdb.connect(db_name, config={'allow_unsigned_extensions': 'true'})
                conn.execute(f"LOAD '{path}'")
                return conn
        pytest.skip(f'could not load {extension_name}')

    return _require


# By making the scope 'function' we ensure that a new connection gets created for every function that uses the fixture
@pytest.fixture(scope='function', autouse=True)
def spark():
    if not hasattr(spark, 'session'):
        # Cache the import
        from pyduckdb.spark.sql import SparkSession as session

        spark.session = session
    return spark.session.builder.master(':memory:').appName('pyspark').getOrCreate()


@pytest.fixture(scope='session', autouse=True)
def duckdb_cursor(request):
    connection = duckdb.connect('')
    cursor = connection.cursor()
    cursor.execute('CREATE TABLE integers (i integer)')
    cursor.execute('INSERT INTO integers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)')
    cursor.execute('CREATE TABLE timestamps (t timestamp)')
    cursor.execute("INSERT INTO timestamps VALUES ('1992-10-03 18:34:45'), ('2010-01-01 00:00:01'), (NULL)")
    cursor.execute("CALL dbgen(sf=0.01)")
    return cursor


@pytest.fixture(scope="function")
def duckdb_cursor_autocommit(request, tmp_path):
    test_dbfarm = tmp_path.resolve().as_posix()

    def finalizer():
        duckdb.shutdown()
        if tmp_path.is_dir():
            shutil.rmtree(test_dbfarm)

    request.addfinalizer(finalizer)

    connection = duckdb.connect(test_dbfarm)
    connection.set_autocommit(True)
    cursor = connection.cursor()
    return (cursor, connection, test_dbfarm)


@pytest.fixture(scope="function")
def initialize_duckdb(request, tmp_path):
    test_dbfarm = tmp_path.resolve().as_posix()

    def finalizer():
        duckdb.shutdown()
        if tmp_path.is_dir():
            shutil.rmtree(test_dbfarm)

    request.addfinalizer(finalizer)

    duckdb.connect(test_dbfarm)
    return test_dbfarm
