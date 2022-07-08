import duckdb
import pandas as pd
import numpy

def round_trip(data,pandas_type):
    df_in = pd.DataFrame({
        'object': pd.Series(data, dtype=pandas_type),
    })

    df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
    print (df_out)
    print (df_in)
    assert df_out.equals(df_in)

class TestPandasTypes(object):
    def test_pandas_unsigned(self, duckdb_cursor):
        unsigned_types = ['uint8','uint16','uint32','uint64']
        data = numpy.array([0,1,2,3])
        for u_type in unsigned_types:
            round_trip(data,u_type)

    def test_pandas_bool(self, duckdb_cursor):
        data = numpy.array([True,False,False,True])
        round_trip(data,'bool')
    
    def test_pandas_boolean(self, duckdb_cursor):
        data = numpy.array([True,None,pd.NA,numpy.nan,True])
        df_in = pd.DataFrame({
            'object': pd.Series(data, dtype='boolean'),
        })

        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        assert df_out['object'][0] == df_in['object'][0]
        assert numpy.isnan(df_out['object'][1])
        assert numpy.isnan(df_out['object'][2])
        assert numpy.isnan(df_out['object'][3])
        assert df_out['object'][4] == df_in['object'][4]

    def test_pandas_float32(self, duckdb_cursor):
        data = numpy.array([0.1,0.32,0.78, numpy.nan])
        df_in = pd.DataFrame({
        'object': pd.Series(data, dtype='float32'),
        })

        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        
        assert df_out['object'][0] == df_in['object'][0]
        assert df_out['object'][1] == df_in['object'][1]
        assert df_out['object'][2] == df_in['object'][2]
        assert numpy.isnan(df_out['object'][3])

    def test_pandas_interval(self, duckdb_cursor):
        if pd. __version__ != '1.2.4':
            return
            
        data = numpy.array([2069211000000000,numpy.datetime64("NaT")])
        df_in = pd.DataFrame({
        'object': pd.Series(data, dtype='timedelta64[ns]'),
        })

        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        
        assert df_out['object'][0] == df_in['object'][0]
        assert  pd.isnull(df_out['object'][1])

    def test_pandas_encoded_utf8(self, duckdb_cursor):
        data = u'\u00c3'            # Unicode data
        data = [data.encode('utf8')]
        expected_result = data[0]
        df_in = pd.DataFrame({'object': pd.Series(data, dtype='object')})
        result = duckdb.query_df(df_in, "data", "SELECT * FROM data").fetchone()[0]
        assert result == expected_result


