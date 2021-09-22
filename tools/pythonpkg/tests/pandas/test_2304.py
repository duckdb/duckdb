import duckdb
import pandas as pd
import numpy as np

class TestPandasMergeSameName(object):
    def test_2304(self, duckdb_cursor):
        df1 = pd.DataFrame({
            'id_1': [1, 1, 1, 2, 2],
            'agedate': np.array(['2010-01-01','2010-02-01','2010-03-01','2020-02-01', '2020-03-01']).astype('datetime64[D]'),
            'age': [1, 2, 3, 1, 2],
            'v': [1.1, 1.2, 1.3, 2.1, 2.2]
        })

        df2 = pd.DataFrame({
            'id_1': [1, 1, 2],
            'agedate': np.array(['2010-01-01','2010-02-01', '2020-03-01']).astype('datetime64[D]'),
            'v2': [11.1, 11.2, 21.2]
        })

        con = duckdb.connect()
        con.register('df1', df1)
        con.register('df2', df2)
        query = """SELECT * from df1
        LEFT  OUTER JOIN df2
        ON (df1.id_1=df2.id_1 and df1.agedate=df2.agedate)  order by df1.id_1, df1.agedate"""

        result_df = con.execute(query).fetchdf()
        expected_result = con.execute(query).fetchall()
        con.register('result_df', result_df)
        result = con.execute('select * from result_df').fetchall()

        assert result == expected_result

    def test_pd_names(self, duckdb_cursor):

        df1 = pd.DataFrame({
            'id': [1, 1, 2],
            'id_1': [1, 1, 2],
            'id_3': [1, 1, 2],
        })

        df2 = pd.DataFrame({
            'id': [1, 1, 2],
            'id_1': [1, 1, 2],
            'id_2': [1, 1, 1]
        })

        exp_result = pd.DataFrame({
            'id': [1, 1, 2, 1, 1],
            'id_1': [1, 1, 2, 1, 1],
            'id_3': [1, 1, 2, 1, 1],
            'id_2': [1, 1, 1, 1, 1],
            'id_1_2': [1, 1, 2, 1, 1]
        })

        con = duckdb.connect()
        con.register('df1', df1)
        con.register('df2', df2)
        query = """SELECT * from df1
        LEFT OUTER JOIN df2
        ON (df1.id_1=df2.id_1)"""

        result_df = con.execute(query).fetchdf()
        assert(exp_result.equals(result_df))