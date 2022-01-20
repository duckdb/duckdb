import duckdb
import pytest
import pandas as pd

class TestPandasBoolean(object):
    def test_boolean_types_roundtrip(self, duckdb_cursor):
        return


df2 = pd.DataFrame({"foo": [True, None, False]}, dtype="boolean")
print(df2)