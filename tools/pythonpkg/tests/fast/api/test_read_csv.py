from multiprocessing.sharedctypes import Value
import numpy
import datetime
import pandas
import pytest
import duckdb
from io import StringIO, BytesIO
from duckdb.typing import BIGINT, VARCHAR, INTEGER


def TestFile(name):
    import os

    filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'data', name)
    return filename


@pytest.fixture
def create_temp_csv(tmp_path):
    # Create temporary CSV files
    file1_content = """1
2
3"""
    file2_content = """4
5
6"""
    file1_path = tmp_path / "file1.csv"
    file2_path = tmp_path / "file2.csv"

    file1_path.write_text(file1_content)
    file2_path.write_text(file2_content)

    return file1_path, file2_path


class TestReadCSV(object):
    def test_using_connection_wrapper(self):
        rel = duckdb.read_csv(TestFile('category.csv'))
        res = rel.fetchone()
        print(res)
        assert res == (1, 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))

    def test_using_connection_wrapper_with_keyword(self):
        rel = duckdb.read_csv(TestFile('category.csv'), dtype={'category_id': 'string'})
        res = rel.fetchone()
        print(res)
        assert res == ('1', 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))

    def test_no_options(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'))
        res = rel.fetchone()
        print(res)
        assert res == (1, 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))

    def test_dtype(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'), dtype={'category_id': 'string'})
        res = rel.fetchone()
        print(res)
        assert res == ('1', 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))

    def test_dtype_as_list(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'), dtype=['string'])
        res = rel.fetchone()
        print(res)
        assert res == ('1', 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))

        rel = duckdb_cursor.read_csv(TestFile('category.csv'), dtype=['double'])
        res = rel.fetchone()
        print(res)
        assert res == (1.0, 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))

    def test_sep(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'), sep=" ")
        res = rel.fetchone()
        print(res)
        assert res == ('1|Action|2006-02-15', datetime.time(4, 46, 27))

    def test_delimiter(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'), delimiter=" ")
        res = rel.fetchone()
        print(res)
        assert res == ('1|Action|2006-02-15', datetime.time(4, 46, 27))

    def test_delimiter_and_sep(self, duckdb_cursor):
        with pytest.raises(duckdb.InvalidInputException, match="read_csv takes either 'delimiter' or 'sep', not both"):
            rel = duckdb_cursor.read_csv(TestFile('category.csv'), delimiter=" ", sep=" ")

    def test_header_true(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'), header=True)
        res = rel.fetchone()
        print(res)
        assert res == (1, 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))

    @pytest.mark.skip(reason="Issue #6011 needs to be fixed first, header=False doesn't work correctly")
    def test_header_false(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'), header=False)

    def test_na_values(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'), na_values='Action')
        res = rel.fetchone()
        print(res)
        assert res == (1, None, datetime.datetime(2006, 2, 15, 4, 46, 27))

    def test_skiprows(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'), skiprows=1)
        res = rel.fetchone()
        print(res)
        assert res == (1, 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))

    # We want to detect this at bind time
    def test_compression_wrong(self, duckdb_cursor):
        with pytest.raises(duckdb.Error, match="Input is not a GZIP stream"):
            rel = duckdb_cursor.read_csv(TestFile('category.csv'), compression='gzip')

    def test_quotechar(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('unquote_without_delimiter.csv'), quotechar="")
        res = rel.fetchone()
        print(res)
        assert res == ('"AAA"BB',)

    def test_escapechar(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('quote_escape.csv'), escapechar=";")
        res = rel.limit(1, 1).fetchone()
        print(res)
        assert res == ('345', 'TEST6', '"text""2""text"')

    def test_encoding_wrong(self, duckdb_cursor):
        with pytest.raises(
            duckdb.BinderException, match="Copy is only supported for UTF-8 encoded files, ENCODING 'UTF-8'"
        ):
            rel = duckdb_cursor.read_csv(TestFile('quote_escape.csv'), encoding=";")

    def test_encoding_correct(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('quote_escape.csv'), encoding="UTF-8")
        res = rel.limit(1, 1).fetchone()
        print(res)
        assert res == (345, 'TEST6', 'text"2"text')

    def test_parallel_true(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'), parallel=True)
        res = rel.fetchone()
        print(res)
        assert res == (1, 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))

    def test_parallel_true(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'), parallel=False)
        res = rel.fetchone()
        print(res)
        assert res == (1, 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))

    def test_date_format_as_datetime(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('datetime.csv'))
        res = rel.fetchone()
        print(res)
        assert res == (
            123,
            'TEST2',
            datetime.time(12, 12, 12),
            datetime.date(2000, 1, 1),
            datetime.datetime(2000, 1, 1, 12, 12),
        )

    def test_date_format_as_date(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('datetime.csv'), date_format='%Y-%m-%d')
        res = rel.fetchone()
        print(res)
        assert res == (
            123,
            'TEST2',
            datetime.time(12, 12, 12),
            datetime.date(2000, 1, 1),
            datetime.datetime(2000, 1, 1, 12, 12),
        )

    def test_timestamp_format(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('datetime.csv'), timestamp_format='%m/%d/%Y')
        res = rel.fetchone()
        print(res)
        assert res == (123, 'TEST2', datetime.time(12, 12, 12), datetime.date(2000, 1, 1), '2000-01-01 12:12:00')

    def test_sample_size_correct(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('problematic.csv'), header=True, sample_size=-1)
        res = rel.fetchone()
        print(res)
        assert res == ('1', '1', '1')

    def test_all_varchar(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'), all_varchar=True)
        res = rel.fetchone()
        print(res)
        assert res == ('1', 'Action', '2006-02-15 04:46:27')

    def test_null_padding(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('nullpadding.csv'), null_padding=False)
        res = rel.fetchall()
        assert res == [
            ('# this file has a bunch of gunk at the top',),
            ('one,two,three,four',),
            ('1,a,alice',),
            ('2,b,bob',),
        ]

        rel = duckdb_cursor.read_csv(TestFile('nullpadding.csv'), null_padding=True)
        res = rel.fetchall()
        assert res == [
            ('# this file has a bunch of gunk at the top', None, None, None),
            ('one', 'two', 'three', 'four'),
            ('1', 'a', 'alice', None),
            ('2', 'b', 'bob', None),
        ]

        rel = duckdb.read_csv(TestFile('nullpadding.csv'), null_padding=False)
        res = rel.fetchall()
        assert res == [
            ('# this file has a bunch of gunk at the top',),
            ('one,two,three,four',),
            ('1,a,alice',),
            ('2,b,bob',),
        ]

        rel = duckdb.read_csv(TestFile('nullpadding.csv'), null_padding=True)
        res = rel.fetchall()
        assert res == [
            ('# this file has a bunch of gunk at the top', None, None, None),
            ('one', 'two', 'three', 'four'),
            ('1', 'a', 'alice', None),
            ('2', 'b', 'bob', None),
        ]

        rel = duckdb_cursor.from_csv_auto(TestFile('nullpadding.csv'), null_padding=False)
        res = rel.fetchall()
        assert res == [
            ('# this file has a bunch of gunk at the top',),
            ('one,two,three,four',),
            ('1,a,alice',),
            ('2,b,bob',),
        ]

        rel = duckdb_cursor.from_csv_auto(TestFile('nullpadding.csv'), null_padding=True)
        res = rel.fetchall()
        assert res == [
            ('# this file has a bunch of gunk at the top', None, None, None),
            ('one', 'two', 'three', 'four'),
            ('1', 'a', 'alice', None),
            ('2', 'b', 'bob', None),
        ]

        rel = duckdb.from_csv_auto(TestFile('nullpadding.csv'), null_padding=False)
        res = rel.fetchall()
        assert res == [
            ('# this file has a bunch of gunk at the top',),
            ('one,two,three,four',),
            ('1,a,alice',),
            ('2,b,bob',),
        ]

        rel = duckdb.from_csv_auto(TestFile('nullpadding.csv'), null_padding=True)
        res = rel.fetchall()
        assert res == [
            ('# this file has a bunch of gunk at the top', None, None, None),
            ('one', 'two', 'three', 'four'),
            ('1', 'a', 'alice', None),
            ('2', 'b', 'bob', None),
        ]

    def test_normalize_names(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'), normalize_names=False)
        df = rel.df()
        column_names = list(df.columns.values)
        # The names are not normalized, so they are capitalized
        assert 'CATEGORY_ID' in column_names

        rel = duckdb_cursor.read_csv(TestFile('category.csv'), normalize_names=True)
        df = rel.df()
        column_names = list(df.columns.values)
        # The capitalized names are normalized to lowercase instead
        assert 'CATEGORY_ID' not in column_names

    def test_filename(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'), filename=False)
        df = rel.df()
        column_names = list(df.columns.values)
        # The filename is not included in the returned columns
        assert 'filename' not in column_names

        rel = duckdb_cursor.read_csv(TestFile('category.csv'), filename=True)
        df = rel.df()
        column_names = list(df.columns.values)
        # The filename is included in the returned columns
        assert 'filename' in column_names

    def test_read_pathlib_path(self, duckdb_cursor):
        pathlib = pytest.importorskip("pathlib")
        path = pathlib.Path(TestFile('category.csv'))
        rel = duckdb_cursor.read_csv(path)
        res = rel.fetchone()
        print(res)
        assert res == (1, 'Action', datetime.datetime(2006, 2, 15, 4, 46, 27))

    def test_read_filelike(self, duckdb_cursor):
        _ = pytest.importorskip("fsspec")
        string = StringIO("c1,c2,c3\na,b,c")
        res = duckdb_cursor.read_csv(string, header=True).fetchall()
        assert res == [('a', 'b', 'c')]

    def test_read_filelike_rel_out_of_scope(self, duckdb_cursor):
        _ = pytest.importorskip("fsspec")

        def keep_in_scope():
            string = StringIO("c1,c2,c3\na,b,c")
            # Create a ReadCSVRelation on a file-like object
            # this will add the object to our internal object filesystem
            rel = duckdb_cursor.read_csv(string, header=True)
            # The file-like object will still exist, so we can execute this later
            return rel

        def close_scope():
            string = StringIO("c1,c2,c3\na,b,c")
            # Create a ReadCSVRelation on a file-like object
            # this will add the object to our internal object filesystem
            res = duckdb_cursor.read_csv(string, header=True).fetchall()
            # When the relation goes out of scope - we delete the file-like object from our filesystem
            return res

        relation = keep_in_scope()
        res = relation.fetchall()

        res2 = close_scope()
        assert res == res2

    def test_filelike_bytesio(self, duckdb_cursor):
        _ = pytest.importorskip("fsspec")
        string = BytesIO(b"c1,c2,c3\na,b,c")
        res = duckdb_cursor.read_csv(string, header=True).fetchall()
        assert res == [('a', 'b', 'c')]

    def test_filelike_exception(self, duckdb_cursor):
        _ = pytest.importorskip("fsspec")

        class ReadError:
            def __init__(self):
                pass

            def read(self, amount):
                raise ValueError(amount)

            def seek(self, loc):
                return 0

        class SeekError:
            def __init__(self):
                pass

            def read(self, amount):
                return b'test'

            def seek(self, loc):
                raise ValueError(loc)

        obj = ReadError()
        with pytest.raises(ValueError):
            res = duckdb_cursor.read_csv(obj, header=True).fetchall()

        obj = SeekError()
        with pytest.raises(ValueError):
            res = duckdb_cursor.read_csv(obj, header=True).fetchall()

    def test_filelike_custom(self, duckdb_cursor):
        _ = pytest.importorskip("fsspec")

        class CustomIO:
            def __init__(self):
                self.loc = 0
                pass

            def seek(self, loc):
                self.loc = loc
                return loc

            def read(self, amount):
                out = b"c1,c2,c3\na,b,c"[self.loc : self.loc + amount : 1]
                self.loc += amount
                return out

        obj = CustomIO()
        res = duckdb_cursor.read_csv(obj, header=True).fetchall()
        assert res == [('a', 'b', 'c')]

    def test_filelike_non_readable(self, duckdb_cursor):
        _ = pytest.importorskip("fsspec")
        obj = 5
        with pytest.raises(ValueError, match="Can not read from a non file-like object"):
            res = duckdb_cursor.read_csv(obj, header=True).fetchall()

    def test_filelike_none(self, duckdb_cursor):
        _ = pytest.importorskip("fsspec")
        obj = None
        with pytest.raises(ValueError, match="Can not read from a non file-like object"):
            res = duckdb_cursor.read_csv(obj, header=True).fetchall()

    @pytest.mark.skip(reason="depends on garbage collector behaviour, and sporadically breaks in CI")
    def test_internal_object_filesystem_cleanup(self, duckdb_cursor):
        _ = pytest.importorskip("fsspec")

        class CountedObject(StringIO):
            instance_count = 0

            def __init__(self, str):
                CountedObject.instance_count += 1
                super().__init__(str)

            def __del__(self):
                CountedObject.instance_count -= 1

        def scoped_objects(duckdb_cursor):
            obj = CountedObject("a,b,c")
            rel1 = duckdb_cursor.read_csv(obj)
            assert rel1.fetchall() == [
                (
                    'a',
                    'b',
                    'c',
                )
            ]
            assert CountedObject.instance_count == 1

            obj = CountedObject("a,b,c")
            rel2 = duckdb_cursor.read_csv(obj)
            assert rel2.fetchall() == [
                (
                    'a',
                    'b',
                    'c',
                )
            ]
            assert CountedObject.instance_count == 2

            obj = CountedObject("a,b,c")
            rel3 = duckdb_cursor.read_csv(obj)
            assert rel3.fetchall() == [
                (
                    'a',
                    'b',
                    'c',
                )
            ]
            assert CountedObject.instance_count == 3

        assert CountedObject.instance_count == 0
        scoped_objects(duckdb_cursor)
        assert CountedObject.instance_count == 0

    def test_read_csv_glob(self, tmp_path, create_temp_csv):
        file1_path, file2_path = create_temp_csv

        # Use the temporary file paths to read CSV files
        con = duckdb.connect()
        rel = con.read_csv(f'{tmp_path}/file*.csv')
        res = con.sql("select * from rel order by all").fetchall()
        assert res == [(1,), (2,), (3,), (4,), (5,), (6,)]

    def test_read_csv_combined(self):
        CSV_FILE = TestFile('stress_test.csv')
        COLUMNS = {
            'result': 'VARCHAR',
            'table': 'BIGINT',
            '_time': 'TIMESTAMPTZ',
            '_measurement': 'VARCHAR',
            'bench_test': 'VARCHAR',
            'flight_id': 'VARCHAR',
            'flight_status': 'VARCHAR',
            'log_level': 'VARCHAR',
            'sys_uuid': 'VARCHAR',
            'message': 'VARCHAR',
        }

        rel = duckdb.read_csv(
            CSV_FILE, header=True, skiprows=1, delimiter=",", quotechar='"', escapechar="\\", dtype=COLUMNS
        )
        res = rel.fetchall()

        rel2 = duckdb.sql(rel.sql_query())
        res2 = rel2.fetchall()

        # Assert that the results are the same
        assert res == res2

        # And assert that the columns and types of the relations are the same
        assert rel.columns == rel2.columns
        assert rel.types == rel2.types

    def test_read_csv_names(self):
        con = duckdb.connect()
        file = StringIO('one,two,three,four\n1,2,3,4\n1,2,3,4\n1,2,3,4')
        rel = con.read_csv(file, names=['a', 'b', 'c'])
        assert rel.columns == ['a', 'b', 'c', 'four']

        with pytest.raises(duckdb.InvalidInputException, match="read_csv only accepts 'names' as a list of strings"):
            rel = con.read_csv(file, names=True)

        # Excessive columns is fine, just doesn't have any effect past the number of provided columns
        rel = con.read_csv(file, names=['a', 'b', 'c', 'd', 'e'])
        assert rel.columns == ['a', 'b', 'c', 'd']

        # Duplicates are not okay
        with pytest.raises(duckdb.BinderException, match="has duplicate column name"):
            rel = con.read_csv(file, names=['a', 'b', 'a', 'b'])
            assert rel.columns == ['a', 'b', 'a', 'b']

    def test_read_csv_names_mixed_with_dtypes(self):
        con = duckdb.connect()
        file = StringIO('one,two,three,four\n1,2,3,4\n1,2,3,4\n1,2,3,4')
        rel = con.read_csv(
            file,
            names=['a', 'b', 'c'],
            dtype={
                'a': int,
                'b': bool,
                'c': str,
            },
        )
        assert rel.columns == ['a', 'b', 'c', 'four']
        assert rel.types == ['BIGINT', 'BOOLEAN', 'VARCHAR', 'BIGINT']

        # dtypes and names dont match
        # FIXME: seems the order columns are named in this error is non-deterministic
        # so for now I'm excluding the list of columns from the expected error
        expected_error = """do not exist in the CSV File"""
        with pytest.raises(duckdb.BinderException, match=expected_error):
            rel = con.read_csv(
                file,
                names=['a', 'b', 'c'],
                dtype={
                    'd': int,
                    'e': bool,
                    'f': str,
                },
            )
