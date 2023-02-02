import numpy
import datetime
import pandas
import pytest
import duckdb

def TestFile(name):
    import os
    filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),'data',name)
    return filename

@pytest.mark.skipif(duckdb.extension_loaded('json') == False, reason="JSON extension is not loaded")
class TestReadJSON(object):
    def test_read_json_columns(self):
        rel = duckdb.read_json(TestFile('example.json'), columns={'id':'integer', 'name':'varchar'})
        res = rel.fetchone()
        print(res)
        assert res == (1, 'O Brother, Where Art Thou?')

    def test_read_json_auto(self):
        rel = duckdb.read_json(TestFile('example.json'))
        res = rel.fetchone()
        print(res)
        assert res == (1, 'O Brother, Where Art Thou?')
