# test fetchdf with various types
import pandas
import numpy

class TestType(object):
    def test_fetchdf(self, duckdb_cursor):
        duckdb_cursor.execute("CREATE TABLE items(item VARCHAR)")
        duckdb_cursor.execute("INSERT INTO items VALUES ('jeans'), (''), (NULL)")
        res = duckdb_cursor.execute("SELECT item FROM items").fetchdf()
        assert isinstance(res, pandas.DataFrame)

        arr = numpy.ma.masked_array(['jeans', '', None])
        arr.mask = [False, False, True]
        arr = {'item': arr}
        df = pandas.DataFrame.from_dict(arr)

        pandas.testing.assert_frame_equal(res, df)
