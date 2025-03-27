import duckdb
import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip('pyarrow', '18.0.0')
from typing import Union
import pyarrow.compute as pc
import uuid
import datetime
import numpy as np
import cmath
from typing import NamedTuple, Any, List

from duckdb.typing import *


class Candidate(NamedTuple):
    type: duckdb.typing.DuckDBPyType
    variant_one: Any
    variant_two: Any


def layout(index: int):
    return [
        ['x', 'x', 'y'],
        ['x', None, 'y'],
        [None, 'y', None],
        ['x', None, None],
        [None, None, 'y'],
        [None, None, None],
    ][index]


def get_table_data():
    def add_variations(data, index: int):
        data.extend(
            [
                {
                    'a': layout(index),
                    'b': layout(0),
                    'c': layout(0),
                },
                {
                    'a': layout(0),
                    'b': layout(0),
                    'c': layout(index),
                },
            ]
        )

    data = []
    add_variations(data, 1)
    add_variations(data, 2)
    add_variations(data, 3)
    add_variations(data, 4)
    add_variations(data, 5)
    return data


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
            b'\xf6\x96\xb0\x85',
            b'\x85\xb0\x96\xf6',
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


def construct_query(tuples) -> str:
    def construct_values_list(row, start_param_idx):
        parameter_count = len(row)
        parameters = [f'${x+start_param_idx}' for x in range(parameter_count)]
        parameters = '(' + ', '.join(parameters) + ')'
        return parameters

    row_size = len(tuples[0])
    values_list = [construct_values_list(x, 1 + (i * row_size)) for i, x in enumerate(tuples)]
    values_list = ', '.join(values_list)

    query = f"""
        select * from (values {values_list})
    """
    return query


def construct_parameters(tuples, dbtype):
    parameters = []
    for row in tuples:
        parameters.extend(list([duckdb.Value(x, dbtype) for x in row]))
    return parameters


class TestUDFNullFiltering(object):
    @pytest.mark.parametrize(
        'table_data',
        get_table_data(),
    )
    @pytest.mark.parametrize(
        'test_type',
        get_types(),
    )
    @pytest.mark.parametrize('udf_type', ['arrow', 'native'])
    def test_null_filtering(self, duckdb_cursor, table_data: dict, test_type: Candidate, udf_type):
        null_count = sum([1 for x in list(zip(*table_data.values())) if any([y == None for y in x])])
        row_count = len(table_data)
        table_data = {
            key: [None if not x else test_type.variant_one if x == 'x' else test_type.variant_two for x in value]
            for key, value in table_data.items()
        }

        tuples = list(zip(*table_data.values()))
        query = construct_query(tuples)
        parameters = construct_parameters(tuples, test_type.type)
        rel = duckdb_cursor.sql(query + " t(a, b, c)", params=parameters)
        rel.to_table('tbl')
        rel.show()

        def my_func(*args):
            if udf_type == 'arrow':
                my_func.count += len(args[0])
            else:
                my_func.count += 1
            return args[0]

        def create_parameters(table_data, dbtype):
            return ", ".join(f'{key}::{dbtype}' for key in list(table_data.keys()))

        my_func.count = 0
        duckdb_cursor.create_function('test', my_func, None, test_type.type, type=udf_type)
        query = f"select test({create_parameters(table_data, test_type.type)}) from tbl"
        result = duckdb_cursor.sql(query).fetchall()

        expected_output = [
            (t[0],) if not any(x == None for x in t) else (None,) for t in list(zip(*table_data.values()))
        ]
        assert result == expected_output
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
