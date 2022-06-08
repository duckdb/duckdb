import pandas as pd
import duckdb
import datetime
import numpy as np
import pytest

def check_analyze_result(df, output_type):
    assert df[0].dtype == np.dtype('O')
    duckdb.analyze_df(df)
    assert df[0].dtype == np.dtype(output_type)
    duckdb.default_connection.execute("select * from df").fetchall()

class TestAnalyzeDF(object):

    def test_empty_dataframe(self, duckdb_cursor):
        df = pd.DataFrame({'A' : []})
        with pytest.raises(Exception, match="Empty dataframe can not be analyzed"):
            duckdb.analyze_df(df)

    def test_analyze_date(self, duckdb_cursor):
        df = pd.DataFrame({0: [datetime.date(1992, 7, 30), datetime.date(1992, 7, 31)]})
        check_analyze_result(df,'<M8[ns]')

    def test_analyze_object(self, duckdb_cursor):
        df = pd.DataFrame({0: [datetime.date(1992, 7, 30), "bla"]})
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