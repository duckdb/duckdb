import duckdb
import pytest
import tempfile
import numpy
import pandas
import datetime
try:
    import pyarrow as pa
    can_run = True
except:
    can_run = False

def parquet_types_test(type_list):
    temp = tempfile.NamedTemporaryFile()
    temp_name = temp.name
    for type_pair in type_list:
        value_list = type_pair[0]
        numpy_type = type_pair[1]
        sql_type = type_pair[2]
        add_cast = len(type_pair) > 3 and type_pair[3]
        add_sql_cast = len(type_pair) > 4 and type_pair[4]
        df = pandas.DataFrame.from_dict({
            'val': numpy.array(value_list, dtype=numpy_type)
        })
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute(f"CREATE TABLE tmp AS SELECT val::{sql_type} val FROM df")
        duckdb_cursor.execute(f"COPY tmp TO '{temp_name}' (FORMAT PARQUET)")
        read_df = pandas.read_parquet(temp_name)
        if add_cast:
            read_df['val'] = read_df['val'].astype(numpy_type)
        assert df.equals(read_df)

        read_from_duckdb = duckdb_cursor.execute(f"SELECT * FROM parquet_scan('{temp_name}')").df()
        assert read_df.equals(read_from_duckdb)

        df.to_parquet(temp_name)
        if add_sql_cast:
            read_from_arrow = duckdb_cursor.execute(f"SELECT val::{sql_type} val FROM parquet_scan('{temp_name}')").df()
        else:
            read_from_arrow = duckdb_cursor.execute(f"SELECT * FROM parquet_scan('{temp_name}')").df()
        assert read_df.equals(read_from_arrow)


class TestParquetRoundtrip(object):
    def test_roundtrip_numeric(self, duckdb_cursor):
        if not can_run:
            return
        type_list = [
            ([-2**7, 0, 2**7-1], numpy.int8, 'TINYINT'),
            ([-2**15, 0, 2**15-1], numpy.int16, 'SMALLINT'),
            ([-2**31, 0, 2**31-1], numpy.int32, 'INTEGER'),
            ([-2**63, 0, 2**63-1], numpy.int64, 'BIGINT'),
            ([0, 42, 2**8-1], numpy.uint8, 'UTINYINT'),
            ([0, 42, 2**16-1], numpy.uint16, 'USMALLINT'),
            ([0, 42, 2**32-1], numpy.uint32, 'UINTEGER', False, True),
            ([0, 42, 2**64-1], numpy.uint64, 'UBIGINT'),
            ([0, 0.5, -0.5], numpy.float32, 'REAL'),
            ([0, 0.5, -0.5], numpy.float64, 'DOUBLE'),
        ]
        parquet_types_test(type_list)

    def test_roundtrip_timestamp(self, duckdb_cursor):
        if not can_run:
            return
        date_time_list = [
            datetime.datetime(2018, 3, 10, 11, 17, 54),
            datetime.datetime(1900, 12, 12, 23, 48, 42),
            None,
            datetime.datetime(1992, 7, 9, 7, 5, 33)
        ]
        type_list = [
            (date_time_list, 'datetime64[ns]', 'TIMESTAMP_NS'),
            (date_time_list, 'datetime64[us]', 'TIMESTAMP'),
            (date_time_list, 'datetime64[ms]', 'TIMESTAMP_MS'),
            (date_time_list, 'datetime64[s]', 'TIMESTAMP_S'),
            (date_time_list, 'datetime64[D]', 'DATE', True)
        ]
        parquet_types_test(type_list)

    def test_roundtrip_varchar(self, duckdb_cursor):
        if not can_run:
            return
        varchar_list = [
            'hello',
            'this is a very long string',
            'hello',
            None
        ]
        type_list = [
            (varchar_list, object, 'VARCHAR')
        ]
        parquet_types_test(type_list)

