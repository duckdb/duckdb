# test fetchdf with various types
import pandas

class TestType(object):
    def test_fetchdf(self, duckdb_cursor):
        duckdb_cursor.execute("CREATE TABLE items(item VARCHAR, value DECIMAL(10,2), count INTEGER)")
        duckdb_cursor.execute("INSERT INTO items VALUES ('jeans', 20.0, 1)")
        res = duckdb_cursor.execute("SELECT * FROM items").fetchdf()
        assert isinstance(res, pandas.DataFrame)
