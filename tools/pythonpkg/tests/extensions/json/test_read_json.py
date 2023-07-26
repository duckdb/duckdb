import numpy
import datetime
import pandas
import pytest
import duckdb
import re


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

    def test_read_json_records(self):
        # Wrong option
        with pytest.raises(duckdb.BinderException, match="""read_json requires "records" to be one of"""):
            rel = duckdb.read_json(TestFile('example.json'), records='none')

        rel = duckdb.read_json(TestFile('example.json'), records='true')
        res = rel.fetchone()
        print(res)
        assert res == (1, 'O Brother, Where Art Thou?')
