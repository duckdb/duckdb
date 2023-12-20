import duckdb
import os
import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
from typing import Union
import pyarrow.compute as pc
import uuid
import datetime
import numpy as np
import cmath

from duckdb.typing import *


def make_annotated_function(type):
    # Create a function that returns its input
    def test_base(x):
        return x

    import types

    test_function = types.FunctionType(
        test_base.__code__, test_base.__globals__, test_base.__name__, test_base.__defaults__, test_base.__closure__
    )
    # Add annotations for the return type and 'x'
    test_function.__annotations__ = {'return': type, 'x': type}
    return test_function


class TestScalarUDF(object):
    @pytest.mark.parametrize('function_type', ['native', 'arrow'])
    @pytest.mark.parametrize(
        'test_type',
        [
            (TINYINT, -42),
            (SMALLINT, -512),
            (INTEGER, -131072),
            (BIGINT, -17179869184),
            (UTINYINT, 254),
            (USMALLINT, 65535),
            (UINTEGER, 4294967295),
            (UBIGINT, 18446744073709551615),
            (HUGEINT, 18446744073709551616),
            (VARCHAR, 'long_string_test'),
            (UUID, uuid.UUID('ffffffff-ffff-ffff-ffff-ffffffffffff')),
            (FLOAT, 0.12246409803628922),
            (DOUBLE, 123142.12312416293784721232344),
            (DATE, datetime.date(2005, 3, 11)),
            (TIMESTAMP, datetime.datetime(2009, 2, 13, 11, 5, 53)),
            (TIME, datetime.time(14, 1, 12)),
            (BLOB, b'\xF6\x96\xB0\x85'),
            (INTERVAL, datetime.timedelta(days=30969, seconds=999, microseconds=999999)),
            (BOOLEAN, True),
            (
                duckdb.struct_type(['BIGINT[]', 'VARCHAR[]']),
                {'v1': [1, 2, 3], 'v2': ['a', 'non-inlined string', 'duckdb']},
            ),
            (duckdb.list_type('VARCHAR'), ['the', 'duck', 'non-inlined string']),
        ],
    )
    def test_type_coverage(self, test_type, function_type):
        type = test_type[0]
        value = test_type[1]

        test_function = make_annotated_function(type)

        con = duckdb.connect()
        con.create_function('test', test_function, type=function_type)

        # Single value
        res = con.execute(f"select test(?::{str(type)})", [value]).fetchall()
        assert res[0][0] == value

        # NULLs
        res = con.execute(f"select res from (select ?, test(NULL::{str(type)}) as res)", [value]).fetchall()
        assert res[0][0] == None

        # Multiple chunks
        size = duckdb.__standard_vector_size__ * 3
        res = con.execute(f"select test(x) from repeat(?::{str(type)}, {size}) as tbl(x)", [value]).fetchall()
        assert len(res) == size

        # Mixed NULL/NON-NULL
        size = duckdb.__standard_vector_size__ * 3
        con.execute("select setseed(0.1337)").fetchall()
        actual = con.execute(
            f"""
            select test(
                case when (x > 0.5) then
                    ?::{str(type)}
                else
                    NULL
                end
            ) from (select random() as x from range({size}))
        """,
            [value],
        ).fetchall()

        con.execute("select setseed(0.1337)").fetchall()
        expected = con.execute(
            f"""
            select
                case when (x > 0.5) then
                    ?::{str(type)}
                else
                    NULL
                end
            from (select random() as x from range({size}))
        """,
            [value],
        ).fetchall()
        assert expected == actual

        # Using 'relation.project'
        con.execute(f"create table tbl as select ?::{str(type)} as x", [value])
        table_rel = con.table('tbl')
        res = table_rel.project('test(x)').fetchall()
        assert res[0][0] == value

    @pytest.mark.parametrize('udf_type', ['arrow', 'native'])
    def test_map_coverage(self, udf_type):
        def no_op(x):
            return x

        con = duckdb.connect()
        map_type = con.map_type('VARCHAR', 'BIGINT')
        con.create_function('test_map', no_op, [map_type], map_type, type=udf_type)
        rel = con.sql("select test_map(map(['non-inlined string', 'test', 'duckdb'], [42, 1337, 123]))")
        res = rel.fetchall()
        assert res == [({'key': ['non-inlined string', 'test', 'duckdb'], 'value': [42, 1337, 123]},)]

    @pytest.mark.parametrize('udf_type', ['arrow', 'native'])
    def test_exceptions(self, udf_type):
        def raises_exception(x):
            raise AttributeError("error")

        con = duckdb.connect()
        con.create_function('raises', raises_exception, [BIGINT], BIGINT, type=udf_type)
        with pytest.raises(
            duckdb.InvalidInputException,
            match=' Python exception occurred while executing the UDF: AttributeError: error',
        ):
            res = con.sql('select raises(3)').fetchall()

        con.remove_function('raises')
        con.create_function(
            'raises', raises_exception, [BIGINT], BIGINT, exception_handling='return_null', type=udf_type
        )
        res = con.sql('select raises(3) from range(5)').fetchall()
        assert res == [(None,), (None,), (None,), (None,), (None,)]

    def test_non_callable(self):
        con = duckdb.connect()
        with pytest.raises(TypeError):
            con.create_function('func', 5, [BIGINT], BIGINT, type='arrow')

        class MyCallable:
            def __init__(self):
                pass

            def __call__(self, x):
                return x

        my_callable = MyCallable()
        con.create_function('func', my_callable, [BIGINT], BIGINT, type='arrow')
        res = con.sql('select func(5)').fetchall()
        assert res == [(5,)]

    # pyarrow does not support creating an array filled with pd.NA values
    @pytest.mark.parametrize('udf_type', ['native'])
    @pytest.mark.parametrize('duckdb_type', [FLOAT, DOUBLE])
    def test_pd_nan(self, duckdb_type, udf_type):
        def return_pd_nan():
            if udf_type == 'native':
                return pd.NA

        con = duckdb.connect()
        con.create_function('return_pd_nan', return_pd_nan, None, duckdb_type, null_handling='SPECIAL', type=udf_type)

        res = con.sql('select return_pd_nan()').fetchall()
        assert res[0][0] == None

    def test_side_effects(self):
        def count() -> int:
            old = count.counter
            count.counter += 1
            return old

        count.counter = 0

        con = duckdb.connect()
        con.create_function('my_counter', count, side_effects=False)
        res = con.sql('select my_counter() from range(10)').fetchall()
        assert res == [(0,), (0,), (0,), (0,), (0,), (0,), (0,), (0,), (0,), (0,)]

        count.counter = 0
        con.remove_function('my_counter')
        con.create_function('my_counter', count, side_effects=True)
        res = con.sql('select my_counter() from range(10)').fetchall()
        assert res == [(0,), (1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,)]

    @pytest.mark.parametrize('udf_type', ['arrow', 'native'])
    @pytest.mark.parametrize('duckdb_type', [FLOAT, DOUBLE])
    def test_np_nan(self, duckdb_type, udf_type):
        def return_np_nan():
            if udf_type == 'native':
                return np.nan
            else:
                import pyarrow as pa

                return pa.chunked_array([[np.nan]], type=pa.float64())

        con = duckdb.connect()
        con.create_function('return_np_nan', return_np_nan, None, duckdb_type, null_handling='SPECIAL', type=udf_type)

        res = con.sql('select return_np_nan()').fetchall()
        assert pd.isnull(res[0][0])

    @pytest.mark.parametrize('udf_type', ['arrow', 'native'])
    @pytest.mark.parametrize('duckdb_type', [FLOAT, DOUBLE])
    def test_math_nan(self, duckdb_type, udf_type):
        def return_math_nan():
            import cmath

            if udf_type == 'native':
                return cmath.nan
            else:
                import pyarrow as pa

                return pa.chunked_array([[cmath.nan]], type=pa.float64())

        con = duckdb.connect()
        con.create_function(
            'return_math_nan', return_math_nan, None, duckdb_type, null_handling='SPECIAL', type=udf_type
        )

        res = con.sql('select return_math_nan()').fetchall()
        assert pd.isnull(res[0][0])

    @pytest.mark.parametrize('udf_type', ['arrow', 'native'])
    @pytest.mark.parametrize(
        'data_type',
        [
            TINYINT,
            SMALLINT,
            INTEGER,
            BIGINT,
            UTINYINT,
            USMALLINT,
            UINTEGER,
            UBIGINT,
            HUGEINT,
            UHUGEINT,
            VARCHAR,
            UUID,
            FLOAT,
            DOUBLE,
            DATE,
            TIMESTAMP,
            TIME,
            BLOB,
            INTERVAL,
            BOOLEAN,
            duckdb.struct_type(['BIGINT[]', 'VARCHAR[]']),
            duckdb.list_type('VARCHAR'),
        ],
    )
    def test_return_null(self, data_type, udf_type):
        def return_null():
            if udf_type == 'native':
                return None
            else:
                import pyarrow as pa

                return pa.nulls(1)

        con = duckdb.connect()
        con.create_function('return_null', return_null, None, data_type, null_handling='special', type=udf_type)
        rel = con.sql('select return_null() as x')
        assert rel.types[0] == data_type
        assert rel.fetchall()[0][0] == None

    def test_udf_transaction_interaction(self):
        def func(x: int) -> int:
            return x

        con = duckdb.connect()
        rel = con.sql('select 42')
        # Using fetchone keeps the result open, with a transaction
        rel.fetchone()

        # If we would allow a UDF to be created when a transaction is active
        # then starting a new result-fetch would cancel the transaction
        # which would corrupt our internal mechanism used to check if a UDF is already registered
        # because that isn't transaction-aware
        with pytest.raises(
            duckdb.InvalidInputException,
            match='This function can not be called with an active transaction!, commit or abort the existing one first',
        ):
            con.create_function('func', func)

        # This would cancel the previous transaction, causing the function to no longer exist
        rel.fetchall()

        con.create_function('func', func)
        res = con.sql('select func(5)').fetchall()
        assert res == [(5,)]
