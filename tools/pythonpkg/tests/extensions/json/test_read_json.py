import os
from pathlib import Path

import duckdb
import pytest
from pytest import mark


def build_test_file(name: str) -> Path:
    return Path(__file__).absolute().parent / 'data' / name


def in_memory_file(name: str):
    return build_test_file(name).open('rb')


parametrize = lambda: mark.parametrize('name', [build_test_file('example.json'), in_memory_file('example.json')])


class TestReadJSON(object):
    @parametrize()
    def test_read_json_columns(self, name):
        rel = duckdb.read_json(name, columns={'id': 'integer', 'name': 'varchar'})
        res = rel.fetchone()
        print(res)
        assert res == (1, 'O Brother, Where Art Thou?')

    @parametrize()
    def test_read_json_auto(self, name):
        rel = duckdb.read_json(name)
        res = rel.fetchone()
        print(res)
        assert res == (1, 'O Brother, Where Art Thou?')

    @parametrize()
    def test_read_json_maximum_depth(self, name):
        rel = duckdb.read_json(name, maximum_depth=4)
        res = rel.fetchone()
        print(res)
        assert res == (1, 'O Brother, Where Art Thou?')

    @parametrize()
    def test_read_json_sample_size(self, name):
        rel = duckdb.read_json(name, sample_size=2)
        res = rel.fetchone()
        print(res)
        assert res == (1, 'O Brother, Where Art Thou?')

    @parametrize()
    def test_read_json_format(self, name):
        # Wrong option
        with pytest.raises(duckdb.BinderException, match="format must be one of .* not 'test'"):
            rel = duckdb.read_json(name, format='test')

        rel = duckdb.read_json(name, format='unstructured')
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

    @parametrize()
    def test_read_json_records(self, name):
        # Wrong option
        with pytest.raises(duckdb.BinderException, match="""read_json requires "records" to be one of"""):
            rel = duckdb.read_json(name, records='none')

        rel = duckdb.read_json(name, records='true')
        res = rel.fetchone()
        print(res)
        assert res == (1, 'O Brother, Where Art Thou?')
