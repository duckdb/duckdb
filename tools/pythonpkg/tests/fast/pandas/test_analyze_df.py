import pandas as pd
import duckdb
import datetime
import numpy as np
import pytest

def check_analyze_result(df, output_type):
    assert df[0].dtype.char == np.dtype('O')
    duckdb.analyze_df(df)
    assert df[0].dtype.char == np.dtype(output_type).char
    duckdb.default_connection.execute("select * from df").fetchall()

def create_generic_dataframe(data):
    return pd.DataFrame({0: pd.Series(data=data, dtype='object')})

class TestAnalyzeDF(object):

    def test_empty_dataframe(self, duckdb_cursor):
        data = []
        df = create_generic_dataframe(data)
        with pytest.raises(Exception, match="Empty dataframe can not be analyzed"):
            duckdb.analyze_df(df)

    def test_analyze_date(self, duckdb_cursor):
        data = [datetime.date(1992, 7, 30), datetime.date(1992, 7, 31)]
        df = create_generic_dataframe(data)
        check_analyze_result(df,'<M8[ns]')

    def test_analyze_string(self, duckdb_cursor):
        data = ['hello', 'these', 'are', 'all', 'strings', 'also bigger strings that span multiple words']
        df = create_generic_dataframe(data)
        #max_string_size = max(data, key=len)
        check_analyze_result(df,'O')

    def test_analyze_int(self, duckdb_cursor):
        data = [5, -12, -256, 255, 123]
        df = create_generic_dataframe(data)
        check_analyze_result(df, 'i')

    def test_analyze_hugeint(self, duckdb_cursor):
        data = [12345123451234512345]
        with pytest.raises(Exception):
            df = create_generic_dataframe(data)
            # Too big to cast to int
            check_analyze_result(df, 'i')

    def test_analyze_bool(self, duckdb_cursor):
        data = [True, False, True, True]
        df = create_generic_dataframe(data)
        check_analyze_result(df, '?')

    def test_analyze_float(self, duckdb_cursor):
        data = [4.2, 3.1, 1.0, 5.0, 234234.01, -234234.01]
        df = create_generic_dataframe(data)
        check_analyze_result(df, 'f')

    def test_analyze_complex(self, duckdb_cursor):
        data = [complex(0.5, 2.5)]
        df = create_generic_dataframe(data)
        with pytest.raises(Exception):
            # Unsupported python type 'complex128'
            check_analyze_result(df, 'D')

    def test_analyze_byte_unsigned(self, duckdb_cursor):
        data = bytearray([5,4,3])
        df = create_generic_dataframe(data)
        # As far as python is concerned, these are ints
        check_analyze_result(df, 'i')

    def test_analyze_bytes(self, duckdb_cursor):
        data = [bytes('test', 'utf-8')]
        df = create_generic_dataframe(data)
        # These can just be treated as strings
        check_analyze_result(df, 'O')

    def test_analyze_clongdouble(self, duckdb_cursor):
        data = [np.clongdouble(234234234.2341234)]
        df = create_generic_dataframe(data)
        with pytest.raises(Exception):
            # Unsupported python type 'complex128'
            check_analyze_result(df, 'G')

    def test_analyze_longdouble(self, duckdb_cursor):
        data = [np.longdouble(234234234.2341234)]
        df = create_generic_dataframe(data)
        check_analyze_result(df, 'g')

    def test_analyze_timedelta(self, duckdb_cursor):
        data = [datetime.timedelta(days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0)]
        df = create_generic_dataframe(data)
        with pytest.raises(Exception):
            # TypeError: Cannot cast datetime.timedelta object from metadata [W] to  according to the rule 'same_kind'
            check_analyze_result(df, 'm')

    def test_analyze_object(self, duckdb_cursor):
        data = [datetime.date(1992, 7, 30), "bla"]
        df = create_generic_dataframe(data)
        check_analyze_result(df,'O')
        
    # if (col_type == "bool") {
    #     duckdb_col_type = LogicalType::BOOLEAN;
    #     pandas_type = PandasType::BOOL;
    # } else if (col_type == "boolean") {
    #     duckdb_col_type = LogicalType::BOOLEAN;
    #     pandas_type = PandasType::BOOLEAN;
    # } else if (col_type == "uint8" || col_type == "Uint8") {
    #     duckdb_col_type = LogicalType::UTINYINT;
    #     pandas_type = PandasType::UTINYINT;
    # } else if (col_type == "uint16" || col_type == "Uint16") {
    #     duckdb_col_type = LogicalType::USMALLINT;
    #     pandas_type = PandasType::USMALLINT;
    # } else if (col_type == "uint32" || col_type == "Uint32") {
    #     duckdb_col_type = LogicalType::UINTEGER;
    #     pandas_type = PandasType::UINTEGER;
    # } else if (col_type == "uint64" || col_type == "Uint64") {
    #     duckdb_col_type = LogicalType::UBIGINT;
    #     pandas_type = PandasType::UBIGINT;
    # } else if (col_type == "int8" || col_type == "Int8") {
    #     duckdb_col_type = LogicalType::TINYINT;
    #     pandas_type = PandasType::TINYINT;
    # } else if (col_type == "int16" || col_type == "Int16") {
    #     duckdb_col_type = LogicalType::SMALLINT;
    #     pandas_type = PandasType::SMALLINT;
    # } else if (col_type == "int32" || col_type == "Int32") {
    #     duckdb_col_type = LogicalType::INTEGER;
    #     pandas_type = PandasType::INTEGER;
    # } else if (col_type == "int64" || col_type == "Int64") {
    #     duckdb_col_type = LogicalType::BIGINT;
    #     pandas_type = PandasType::BIGINT;
    # } else if (col_type == "float32") {
    #     duckdb_col_type = LogicalType::FLOAT;
    #     pandas_type = PandasType::FLOAT;
    # } else if (col_type == "float64") {
    #     duckdb_col_type = LogicalType::DOUBLE;
    #     pandas_type = PandasType::DOUBLE;
    # } else if (col_type == "object") {
    #     //! this better be castable to strings
    #     duckdb_col_type = LogicalType::VARCHAR;
    #     pandas_type = PandasType::OBJECT;
    # } else if (col_type == "string") {
    #     duckdb_col_type = LogicalType::VARCHAR;
    #     pandas_type = PandasType::VARCHAR;
    # } else if (col_type == "timedelta64[ns]") {
    #     duckdb_col_type = LogicalType::INTERVAL;
    #     pandas_type = PandasType::INTERVAL;