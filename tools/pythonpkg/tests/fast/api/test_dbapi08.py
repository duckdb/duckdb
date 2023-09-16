# test fetchdf with various types
import numpy
import pytest
import duckdb
from conftest import NumpyPandas


class TestType(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas()])
    def test_fetchdf(self, pandas):
        con = duckdb.connect()
        con.execute("CREATE TABLE items(item VARCHAR)")
        con.execute("INSERT INTO items VALUES ('jeans'), (''), (NULL)")
        res = con.execute("SELECT item FROM items").fetchdf()
        assert isinstance(res, pandas.core.frame.DataFrame)

        df = pandas.DataFrame({'item': ['jeans', '', None]})

        print(res)
        print(df)
        pandas.testing.assert_frame_equal(res, df)
