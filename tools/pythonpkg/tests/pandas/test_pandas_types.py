import duckdb
import pandas as pd
import numpy

def round_trip(data,pandas_type):
    df_in = pd.DataFrame({
        'object': pd.Series(data, dtype=pandas_type),
    })

    df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
    assert numpy.all(df_out['object'] == data)

class TestPandasTypes(object):
    def test_pandas_unsigned(self, duckdb_cursor):
        unsigned_types = ['uint8','uint16','uint32','uint64']
        data = numpy.array([0,1,2,3])
        for u_type in unsigned_types:
            round_trip(data,u_type)

    def test_pandas_bool(self, duckdb_cursor):
        data = numpy.array([True,False,False,True])
        round_trip(data,'bool')
        
    def test_pandas_float32(self, duckdb_cursor):
        data = numpy.array([0.1,0.32,0.78, numpy.nan])
        df_in = pd.DataFrame({
        'object': pd.Series(data, dtype='float32'),
        })

        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        
        assert df_out['object'][0] == df_in['object'][0]
        assert df_out['object'][1] == df_in['object'][1]
        assert df_out['object'][2] == df_in['object'][2]
        assert np.isnan(df_out['object'][3])