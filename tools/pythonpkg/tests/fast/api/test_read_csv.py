from multiprocessing.sharedctypes import Value
import datetime
import pytest
import platform
import duckdb
from io import StringIO, BytesIO
from duckdb import CSVLineTerminator


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
        rel = duckdb_cursor.read_csv(TestFile('category.csv'))
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

    def test_na_values_list(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'), na_values=['Action', 'Animation'])
        res = rel.fetchone()
        assert res == (1, None, datetime.datetime(2006, 2, 15, 4, 46, 27))
        res = rel.fetchone()
        assert res == (2, None, datetime.datetime(2006, 2, 15, 4, 46, 27))

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
        rel = duckdb_cursor.read_csv(TestFile('unquote_without_delimiter.csv'), quotechar="", header=False)
        res = rel.fetchone()
        print(res)
        assert res == ('"AAA"BB',)

    def test_quote(self, duckdb_cursor):
        with pytest.raises(
            duckdb.Error, match="The methods read_csv and read_csv_auto do not have the \"quote\" argument."
        ):
            rel = duckdb_cursor.read_csv(TestFile('unquote_without_delimiter.csv'), quote="", header=False)

    def test_escapechar(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('quote_escape.csv'), escapechar=";", header=False)
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
        rel = duckdb_cursor.read_csv(TestFile('datetime.csv'), timestamp_format='%Y-%m-%d %H:%M:%S')
        res = rel.fetchone()
        assert res == (
            123,
            'TEST2',
            datetime.time(12, 12, 12),
            datetime.date(2000, 1, 1),
            datetime.datetime(2000, 1, 1, 12, 12),
        )

    def test_sample_size_correct(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('problematic.csv'), sample_size=-1)
        res = rel.fetchone()
        print(res)
        assert res == ('1', '1', '1')

    def test_all_varchar(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('category.csv'), all_varchar=True)
        res = rel.fetchone()
        print(res)
        assert res == ('1', 'Action', '2006-02-15 04:46:27')

    def test_null_padding(self, duckdb_cursor):
        rel = duckdb_cursor.read_csv(TestFile('nullpadding.csv'), null_padding=False, header=False)
        res = rel.fetchall()
        assert res == [
            ('# this file has a bunch of gunk at the top',),
            ('one,two,three,four',),
            ('1,a,alice',),
            ('2,b,bob',),
        ]

        rel = duckdb_cursor.read_csv(TestFile('nullpadding.csv'), null_padding=True, header=False)
        res = rel.fetchall()
        assert res == [
            ('# this file has a bunch of gunk at the top', None, None, None),
            ('one', 'two', 'three', 'four'),
            ('1', 'a', 'alice', None),
            ('2', 'b', 'bob', None),
        ]

        rel = duckdb.read_csv(TestFile('nullpadding.csv'), null_padding=False, header=False)
        res = rel.fetchall()
        assert res == [
            ('# this file has a bunch of gunk at the top',),
            ('one,two,three,four',),
            ('1,a,alice',),
            ('2,b,bob',),
        ]

        rel = duckdb.read_csv(TestFile('nullpadding.csv'), null_padding=True, header=False)
        res = rel.fetchall()
        assert res == [
            ('# this file has a bunch of gunk at the top', None, None, None),
            ('one', 'two', 'three', 'four'),
            ('1', 'a', 'alice', None),
            ('2', 'b', 'bob', None),
        ]

        rel = duckdb_cursor.from_csv_auto(TestFile('nullpadding.csv'), null_padding=False, header=False)
        res = rel.fetchall()
        assert res == [
            ('# this file has a bunch of gunk at the top',),
            ('one,two,three,four',),
            ('1,a,alice',),
            ('2,b,bob',),
        ]

        rel = duckdb_cursor.from_csv_auto(TestFile('nullpadding.csv'), null_padding=True, header=False)
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
        pytest.importorskip("fsspec")

        string = StringIO("c1,c2,c3\na,b,c")
        res = duckdb_cursor.read_csv(string).fetchall()
        assert res == [('a', 'b', 'c')]

    def test_read_filelike_rel_out_of_scope(self, duckdb_cursor):
        _ = pytest.importorskip("fsspec")

        def keep_in_scope():
            string = StringIO("c1,c2,c3\na,b,c")
            # Create a ReadCSVRelation on a file-like object
            # this will add the object to our internal object filesystem
            rel = duckdb_cursor.read_csv(string)
            # The file-like object will still exist, so we can execute this later
            return rel

        def close_scope():
            string = StringIO("c1,c2,c3\na,b,c")
            # Create a ReadCSVRelation on a file-like object
            # this will add the object to our internal object filesystem
            res = duckdb_cursor.read_csv(string).fetchall()
            # When the relation goes out of scope - we delete the file-like object from our filesystem
            return res

        relation = keep_in_scope()
        res = relation.fetchall()

        res2 = close_scope()
        assert res == res2

    def test_filelike_bytesio(self, duckdb_cursor):
        _ = pytest.importorskip("fsspec")
        string = BytesIO(b"c1,c2,c3\na,b,c")
        res = duckdb_cursor.read_csv(string).fetchall()
        assert res == [('a', 'b', 'c')]

    def test_filelike_exception(self, duckdb_cursor):
        _ = pytest.importorskip("fsspec")

        class ReadError:
            def __init__(self):
                pass

            def read(self, amount=-1):
                raise ValueError(amount)

            def seek(self, loc):
                return 0

        class SeekError:
            def __init__(self):
                pass

            def read(self, amount=-1):
                return b'test'

            def seek(self, loc):
                raise ValueError(loc)

        # The MemoryFileSystem reads the content into another object, so this fails instantly
        obj = ReadError()
        with pytest.raises(ValueError):
            res = duckdb_cursor.read_csv(obj).fetchall()

        # For that same reason, this will not error, because the data is retrieved with 'read' and then SeekError is never used again
        obj = SeekError()
        res = duckdb_cursor.read_csv(obj).fetchall()

    def test_filelike_custom(self, duckdb_cursor):
        _ = pytest.importorskip("fsspec")

        class CustomIO:
            def __init__(self):
                self.loc = 0
                pass

            def seek(self, loc):
                self.loc = loc
                return loc

            def read(self, amount=-1):
                file = b"c1,c2,c3\na,b,c"
                if amount == -1:
                    return file
                out = file[self.loc : self.loc + amount : 1]
                self.loc += amount
                return out

        obj = CustomIO()
        res = duckdb_cursor.read_csv(obj).fetchall()
        assert res == [('a', 'b', 'c')]

    def test_filelike_non_readable(self, duckdb_cursor):
        _ = pytest.importorskip("fsspec")
        obj = 5
        with pytest.raises(ValueError, match="Can not read from a non file-like object"):
            res = duckdb_cursor.read_csv(obj).fetchall()

    def test_filelike_none(self, duckdb_cursor):
        _ = pytest.importorskip("fsspec")
        obj = None
        with pytest.raises(ValueError, match="Can not read from a non file-like object"):
            res = duckdb_cursor.read_csv(obj).fetchall()

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

    @pytest.mark.xfail(condition=platform.system() == "Emscripten", reason="time zones not working")
    def test_read_csv_combined(self, duckdb_cursor):
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

        rel = duckdb.read_csv(CSV_FILE, skiprows=1, delimiter=",", quotechar='"', escapechar="\\", dtype=COLUMNS)
        res = rel.fetchall()

        rel2 = duckdb_cursor.sql(rel.sql_query())
        res2 = rel2.fetchall()

        # Assert that the results are the same
        assert res == res2

        # And assert that the columns and types of the relations are the same
        assert rel.columns == rel2.columns
        assert rel.types == rel2.types

    def test_read_csv_names(self, tmp_path):
        file = tmp_path / "file.csv"
        file.write_text('one,two,three,four\n1,2,3,4\n1,2,3,4\n1,2,3,4')

        con = duckdb.connect()
        rel = con.read_csv(str(file), names=['a', 'b', 'c'])
        assert rel.columns == ['a', 'b', 'c', 'four']

        with pytest.raises(duckdb.InvalidInputException, match="read_csv only accepts 'names' as a list of strings"):
            rel = con.read_csv(file, names=True)

        # Excessive columns is fine, just doesn't have any effect past the number of provided columns
        rel = con.read_csv(file, names=['a', 'b', 'c', 'd', 'e'])
        assert rel.columns == ['a', 'b', 'c', 'd']

        # Duplicates are not okay
        with pytest.raises(duckdb.BinderException, match="names must have unique values"):
            rel = con.read_csv(file, names=['a', 'b', 'a', 'b'])
            assert rel.columns == ['a', 'b', 'a', 'b']

    def test_read_csv_names_mixed_with_dtypes(self, tmp_path):
        file = tmp_path / "file.csv"
        file.write_text('one,two,three,four\n1,2,3,4\n1,2,3,4\n1,2,3,4')

        con = duckdb.connect()
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

    def test_read_csv_multi_file(self, tmp_path):
        file1 = tmp_path / "file1.csv"
        file1.write_text('one,two,three,four\n1,2,3,4\n1,2,3,4\n1,2,3,4')

        file2 = tmp_path / "file2.csv"
        file2.write_text('one,two,three,four\n5,6,7,8\n5,6,7,8\n5,6,7,8')

        file3 = tmp_path / "file3.csv"
        file3.write_text('one,two,three,four\n9,10,11,12\n9,10,11,12\n9,10,11,12')

        con = duckdb.connect()
        files = [str(file1), str(file2), str(file3)]
        rel = con.read_csv(files)
        res = rel.fetchall()
        assert res == [
            (1, 2, 3, 4),
            (1, 2, 3, 4),
            (1, 2, 3, 4),
            (5, 6, 7, 8),
            (5, 6, 7, 8),
            (5, 6, 7, 8),
            (9, 10, 11, 12),
            (9, 10, 11, 12),
            (9, 10, 11, 12),
        ]

    def test_read_csv_empty_list(self):
        con = duckdb.connect()
        files = []
        with pytest.raises(
            duckdb.InvalidInputException, match='Please provide a non-empty list of paths or file-like objects'
        ):
            rel = con.read_csv(files)
            res = rel.fetchall()

    def test_read_csv_list_invalid_path(self, tmp_path):
        con = duckdb.connect()

        file1 = tmp_path / "file1.csv"
        file1.write_text('one,two,three,four\n1,2,3,4\n1,2,3,4\n1,2,3,4')

        file3 = tmp_path / "file3.csv"
        file3.write_text('one,two,three,four\n9,10,11,12\n9,10,11,12\n9,10,11,12')

        files = [str(file1), 'not_valid_path', str(file3)]
        with pytest.raises(duckdb.IOException, match='No files found that match the pattern "not_valid_path"'):
            rel = con.read_csv(files)
            res = rel.fetchall()

    @pytest.mark.parametrize(
        'options',
        [
            {'lineterminator': '\\n'},
            {'lineterminator': 'LINE_FEED'},
            {'lineterminator': CSVLineTerminator.LINE_FEED},
            {'columns': {'id': 'INTEGER', 'name': 'INTEGER', 'c': 'integer', 'd': 'INTEGER'}},
            {'auto_type_candidates': ['INTEGER', 'INTEGER']},
            {'max_line_size': 10000},
            {'ignore_errors': True},
            {'ignore_errors': False},
            {'store_rejects': True},
            {'store_rejects': False},
            {'rejects_table': 'my_rejects_table'},
            {'rejects_scan': 'my_rejects_scan'},
            {'rejects_table': 'my_rejects_table', 'rejects_limit': 50},
            {'force_not_null': ['one', 'two']},
            {'buffer_size': 420000},
            {'decimal': '.'},
            {'allow_quoted_nulls': True},
            {'allow_quoted_nulls': False},
            {'filename': True},
            {'filename': 'test'},
            {'hive_partitioning': True},
            {'hive_partitioning': False},
            # {'union_by_name': True},
            {'union_by_name': False},
            {'hive_types_autocast': False},
            {'hive_types_autocast': True},
            {'hive_types': {'one': 'INTEGER', 'two': 'VARCHAR'}},
        ],
    )
    def test_read_csv_options(self, duckdb_cursor, options, tmp_path):
        file = tmp_path / "file.csv"
        file.write_text('one,two,three,four\n1,2,3,4\n1,2,3,4\n1,2,3,4')
        print(options)
        if 'hive_types' in options:
            with pytest.raises(duckdb.InvalidInputException, match=r'Unknown hive_type:'):
                rel = duckdb_cursor.read_csv(file, **options)
        else:
            rel = duckdb_cursor.read_csv(file, **options)
            res = rel.fetchall()
