import numpy
import datetime
import pandas
import pytest
import duckdb
import re
from io import StringIO


def TestFile(name):
    import os

    filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data', name)
    return filename


class TestReadJSON(object):
    def test_read_json_columns(self):
        rel = duckdb.read_json(TestFile('example.json'), columns={'id': 'integer', 'name': 'varchar'})
        res = rel.fetchone()
        print(res)
        assert res == (1, 'O Brother, Where Art Thou?')

    def test_read_json_auto(self):
        rel = duckdb.read_json(TestFile('example.json'))
        res = rel.fetchone()
        print(res)
        assert res == (1, 'O Brother, Where Art Thou?')

    def test_read_json_maximum_depth(self):
        rel = duckdb.read_json(TestFile('example.json'), maximum_depth=4)
        res = rel.fetchone()
        print(res)
        assert res == (1, 'O Brother, Where Art Thou?')

    def test_read_json_sample_size(self):
        rel = duckdb.read_json(TestFile('example.json'), sample_size=2)
        res = rel.fetchone()
        print(res)
        assert res == (1, 'O Brother, Where Art Thou?')

    def test_read_json_format(self):
        # Wrong option
        with pytest.raises(duckdb.BinderException, match="format must be one of .* not 'test'"):
            rel = duckdb.read_json(TestFile('example.json'), format='test')

        rel = duckdb.read_json(TestFile('example.json'), format='unstructured')
        res = rel.fetchone()
        print(res)
        assert res == (
            [
                {'id': 1, 'name': 'O Brother, Where Art Thou?'},
                {'id': 2, 'name': 'Home for the Holidays'},
                {'id': 3, 'name': 'The Firm'},
                {'id': 4, 'name': 'Broadcast News'},
                {'id': 5, 'name': 'Raising Arizona'},
            ],
        )

    def test_read_filelike(self, duckdb_cursor):
        pytest.importorskip("fsspec")

        duckdb_cursor.execute("set threads=1")
        string = StringIO("""{"id":1,"name":"O Brother, Where Art Thou?"}\n{"id":2,"name":"Home for the Holidays"}""")
        res = duckdb_cursor.read_json(string).fetchall()
        assert res == [(1, 'O Brother, Where Art Thou?'), (2, 'Home for the Holidays')]

        string1 = StringIO("""{"id":1,"name":"O Brother, Where Art Thou?"}""")
        string2 = StringIO("""{"id":2,"name":"Home for the Holidays"}""")
        res = duckdb_cursor.read_json([string1, string2], filename=True).fetchall()
        assert res[0][1] == 'O Brother, Where Art Thou?'
        assert res[1][1] == 'Home for the Holidays'

        # filenames are different
        assert res[0][2] != res[1][2]

    def test_read_json_records(self):
        # Wrong option
        with pytest.raises(duckdb.BinderException, match="""read_json requires "records" to be one of"""):
            rel = duckdb.read_json(TestFile('example.json'), records='none')

        rel = duckdb.read_json(TestFile('example.json'), records='true')
        res = rel.fetchone()
        print(res)
        assert res == (1, 'O Brother, Where Art Thou?')

    @pytest.mark.parametrize(
        'option',
        [
            ('filename', True),
            ('filename', 'test'),
            ('date_format', '%m-%d-%Y'),
            ('date_format', '%m-%d-%y'),
            ('date_format', '%d-%m-%Y'),
            ('date_format', '%d-%m-%y'),
            ('date_format', '%Y-%m-%d'),
            ('date_format', '%y-%m-%d'),
            ('timestamp_format', '%H:%M:%S%y-%m-%d'),
            ('compression', 'AUTO_DETECT'),
            ('compression', 'UNCOMPRESSED'),
            ('maximum_object_size', 5),
            ('ignore_errors', False),
            ('ignore_errors', True),
            ('convert_strings_to_integers', False),
            ('convert_strings_to_integers', True),
            ('field_appearance_threshold', 0.534),
            ('map_inference_threshold', 34234),
            ('maximum_sample_files', 5),
            ('hive_partitioning', True),
            ('hive_partitioning', False),
            ('union_by_name', True),
            ('union_by_name', False),
            ('hive_types_autocast', False),
            ('hive_types_autocast', True),
            ('hive_types', {'id': 'INTEGER', 'name': 'VARCHAR'}),
        ],
    )
    def test_read_json_options(self, duckdb_cursor, option):
        keyword_arguments = dict()
        option_name, option_value = option
        keyword_arguments[option_name] = option_value
        if option_name == 'hive_types':
            with pytest.raises(duckdb.InvalidInputException, match=r'Unknown hive_type:'):
                rel = duckdb_cursor.read_json(TestFile('example.json'), **keyword_arguments)
        else:
            rel = duckdb_cursor.read_json(TestFile('example.json'), **keyword_arguments)
            res = rel.fetchall()
