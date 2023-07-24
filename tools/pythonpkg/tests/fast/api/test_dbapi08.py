# test fetchdf with various types
import numpy
import pytest
import duckdb
from conftest import NumpyPandas, ArrowPandas


class TestType(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_fetchdf(self, pandas):
        con = duckdb.connect()
        con.execute("CREATE TABLE items(item VARCHAR)")
        con.execute("INSERT INTO items VALUES ('jeans'), (''), (NULL)")
        res = con.execute("SELECT item FROM items").fetchdf()
        assert isinstance(res, pandas.core.frame.DataFrame)

        arr = numpy.ma.masked_array(['jeans', '', None])
        arr.mask = [False, False, True]
        arr = {'item': arr}
        df = pandas.DataFrame(arr)

        pandas.testing.assert_frame_equal(res, df)
