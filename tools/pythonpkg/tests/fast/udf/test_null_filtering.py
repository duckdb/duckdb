import duckdb
import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
from typing import Union
import pyarrow.compute as pc
import uuid
import datetime
import numpy as np
import cmath
from typing import NamedTuple, Any

from duckdb.typing import *


class Candidate(NamedTuple):
    type: duckdb.typing.DuckDBPyType
    variant_one: Any
    variant_two: Any


def get_layout():
    return (
        [
            ['x', None, 'y'],
            [None, 'y', None],
            ['x', None, None],
            [None, None, 'y'],
            [None, None, None],
        ],
    )


def get_types():
    return [
        Candidate(TINYINT, -42, -21),
        Candidate(SMALLINT, -512, -256),
        Candidate(INTEGER, -131072, -65536),
        Candidate(
            BIGINT,
            -17179869184,
            -8589934592,
        ),
        Candidate(
            UTINYINT,
            254,
            127,
        ),
        Candidate(
            USMALLINT,
            65535,
            32767,
        ),
        Candidate(
            UINTEGER,
            4294967295,
            2147483647,
        ),
        Candidate(UBIGINT, 18446744073709551615, 9223372036854776000),
        Candidate(HUGEINT, 18446744073709551616, 9223372036854776000),
        Candidate(VARCHAR, 'long_string_test', 'smallstring'),
        Candidate(
            UUID, uuid.UUID('ffffffff-ffff-ffff-ffff-ffffffffffff'), uuid.UUID('ffffffff-ffff-ffff-ffff-000000000000')
        ),
        Candidate(
            FLOAT,
            0.12246409803628922,
            0.24492819607257843,
        ),
        Candidate(
            DOUBLE,
            123142.12312416293784721232344,
            246284.2462483259,
        ),
        Candidate(DATE, datetime.date(2005, 3, 11), datetime.date(1989, 1, 7)),
        Candidate(TIMESTAMP, datetime.datetime(2009, 2, 13, 11, 5, 53), datetime.datetime(1989, 11, 20, 5, 3, 25)),
        Candidate(
            TIME,
            datetime.time(14, 1, 12),
            datetime.time(6, 3, 6),
        ),
        Candidate(
            BLOB,
            b'\xF6\x96\xB0\x85',
            b'\x85\xB0\x96\xF6',
        ),
        Candidate(
            INTERVAL,
            datetime.timedelta(days=30969, seconds=999, microseconds=999999),
            datetime.timedelta(days=786, seconds=53, microseconds=651),
        ),
        Candidate(
            BOOLEAN,
            True,
            False,
        ),
        Candidate(
            duckdb.struct_type(['BIGINT[]', 'VARCHAR[]']),
            {'v1': [1, 2, 3], 'v2': ['a', 'non-inlined string', 'duckdb']},
            {'v1': [5, 4, 3, 2, 1], 'v2': ['non-inlined-string', 'a', 'b', 'c', 'duckdb']},
        ),
        Candidate(duckdb.list_type('VARCHAR'), ['the', 'duck', 'non-inlined string'], ['non-inlined-string', 'test']),
    ]


class TestUDFNullFiltering(object):
    @pytest.mark.parametrize(
        'table_data',
        get_layout(),
    )
    @pytest.mark.parametrize(
        'test_type',
        get_types(),
    )
    @pytest.mark.parametrize('udf_type', ['arrow', 'native'])
    def test_null_filtering(self, duckdb_cursor, table_data, test_type: Candidate, udf_type):
        null_count = sum([1 for x in table_data if not x])
        row_count = len(table_data)
        table_data = [
            None if not x else test_type.variant_one if x == 'x' else test_type.variant_two for x in table_data
        ]

        df = pd.DataFrame({'a': table_data})
        duckdb_cursor.execute("create table tbl as select * FROM df")

        def my_func(x):
            if udf_type == 'arrow':
                my_func.count += len(x)
            else:
                my_func.count += 1
            return x

        my_func.count = 0
        duckdb_cursor.create_function('test', my_func, [test_type.type], test_type.type, type=udf_type)
        result = duckdb_cursor.sql(f"select test(a::{test_type.type}) from tbl").fetchall()
        assert result == [(x,) for x in table_data]
        assert len(result) == row_count
        # Only the non-null tuples should have been seen by the UDF
        assert my_func.count == row_count - null_count

    @pytest.mark.parametrize(
        'table_data',
        [
            [1, 2, 3, 4],
            [1, 2, None, 4],
        ],
    )
    def test_nulls_from_default_null_handling_native(self, duckdb_cursor, table_data):
        def returns_null(x):
            return None

        df = pd.DataFrame({'a': table_data})
        duckdb_cursor.execute("create table tbl as select * from df")
        duckdb_cursor.create_function('test', returns_null, [str], int, type='native')
        with pytest.raises(duckdb.InvalidInputException, match='The UDF is not expected to return NULL values'):
            result = duckdb_cursor.sql("select test(a::VARCHAR) from tbl").fetchall()

    @pytest.mark.parametrize(
        'table_data',
        [
            [1, 2, 3, 4],
            [1, 2, None, 4],
        ],
    )
    def test_nulls_from_default_null_handling_arrow(self, duckdb_cursor, table_data):
        def returns_null(x):
            l = x.to_pylist()
            return pa.array([None for _ in l], type=pa.int64())

        df = pd.DataFrame({'a': table_data})
        duckdb_cursor.execute("create table tbl as select * from df")
        duckdb_cursor.create_function('test', returns_null, [str], int, type='arrow')
        with pytest.raises(duckdb.InvalidInputException, match='The UDF is not expected to return NULL values'):
            result = duckdb_cursor.sql("select test(a::VARCHAR) from tbl").fetchall()
            print(result)
