import pandas as pd
import duckdb

class Test3654(object):
    def test_3654(self, duckdb_cursor):
        return
df1 = pd.DataFrame({
    'id': [1, 1, 2],
    'id_1': [1, 1, 2],
    'id_3': [1, 1, 2],
})
con = duckdb.connect()
con.register("df1",df1)
rel = con.view("df1")
