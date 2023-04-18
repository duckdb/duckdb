import duckdb
import os
import pandas as pd
import pytest
from typing import Union
import pyarrow.compute as pc
import pyarrow as pa
import pyarrow.compute as pc
even_filter = (pc.bit_wise_and(pc.field("nums"), pc.scalar(1)) == pc.scalar(0))

from duckdb.typing import *

# sort an entire chunk of data

class TestPyArrowUDF(object):

    def test_basic_use(self):
        def plus_one(x):
            column = x['c0']
            return x

        con = duckdb.connect()
        con.register_vectorized('plus_one', plus_one, [BIGINT], BIGINT)
        assert [(5,)] == con.sql('select plus_one(5)').fetchall()

        range_table = con.table_function('range', [5000])
        res = con.sql('select plus_one(i) from range_table tbl(i)').fetchall()
        assert len(res) == 5000

        # FIXME: this is dependent on the duckdb vector size
        # which we can get through `duckdb.__standard_vector_size__`
        res = con.sql('select i, plus_one(i) from test_vector_types(NULL::BIGINT, false) t(i), range(2000)')
        assert len(res) == 22000

    # This only works up to duckdb.__standard_vector_size__
    def test_sort_table(self):
        def sort_table(table):
            sorted_table = table.sort_by([("c0", "ascending")])
            return sorted_table

        con = duckdb.connect()
        con.register_vectorized('sort_table', sort_table, [BIGINT], BIGINT)
        res = con.sql("select 100-i as original, sort_table(original) from range(100) tbl(i)").fetchall()
        assert res[0] == (100, 1)

    def test_predicates_pyarrow(self):
        def is_round(table):
            res = table.compute(even_filter)
            return res

        con = duckdb.connect()
        con.register_vectorized('is_round', is_round, [BIGINT], BIGINT)
        res = con.sql("select 100-i as original, is_round(original) from range(100) tbl(i)").fetchall()
        assert res[0] == (100, 1)

    #def test_detected_parameters(self):
    #    def concatenate(a: str, b: str):
    #        return a + b
        
    #    con = duckdb.connect()
    #    con.register_scalar('py_concatenate', concatenate, None, VARCHAR)
    #    res = con.sql("""
    #        select py_concatenate('5','3');
    #    """).fetchall()
    #    assert res[0][0] == '53'

    #def test_detected_return_type(self):
    #    def add_nums(*args) -> int:
    #        sum = 0;
    #        for arg in args:
    #            sum += arg
    #        return sum

    #    con = duckdb.connect()
    #    con.register_scalar('add_nums', add_nums)
    #    res = con.sql("""
    #        select add_nums(5,3,2,1);
    #    """).fetchall()
    #    assert res[0][0] == 11

    #def test_varargs(self):
    #    def variable_args(*args):
    #        amount = len(args)
    #        return amount
        
    #    con = duckdb.connect()
    #    con.register_scalar('varargs', variable_args, None, BIGINT, varargs=True)
    #    res = con.sql("""select varargs('5', '3', '2', 1, 0.12345)""").fetchall()
    #    assert res == [(5,)]

    #def test_overwrite_name(self):
    #    # TODO: test proper behavior when you register two functions with the same name
        
    #    # create first version of the function

    #    # create relation that uses the function

    #    # create second version of the function

    #    # create relation that uses the new version

    #    # execute both relations
    #    pass

    #def test_nulls(self):
    #    # TODO: provide 'null_handling' option?
    #    pass
    
    #def test_exceptions(self):
    #    # TODO: we likely want an enum to define how exceptions should be handled
    #    # - propagate:
    #    #    throw the exception as a duckdb exception
    #    # - ignore:
    #    #    return NULL instead
    #    pass

    #def test_binding(self):
    #    # TODO: add a way to do extra binding for the UDF
    #    pass
    
    #def test_structs(self):
    #    def add_extra_column(original):
    #        original['a'] = 200
    #        original['bb'] = 0
    #        return original

    #    con = duckdb.connect()
    #    range_table = con.table_function('range', [5000])
    #    con.register_scalar("append_field", add_extra_column, [duckdb.struct_type({'a': BIGINT, 'b': BIGINT})], duckdb.struct_type({'a': BIGINT, 'b': BIGINT, 'c': BIGINT}))

    #    res = con.sql("""
    #        select append_field({'a': i::BIGINT, 'b': 3::BIGINT}) from range_table tbl(i)
    #    """)
    #    # added extra column to the struct
    #    assert len(res.fetchone()[0].keys()) == 3
    #    # FIXME: this is needed, otherwise the transaction is still active in 'register_scalar' and we can't begin a new one
    #    res.fetchall()

    #    def swap_keys(dict):
    #        result = {}
    #        reversed_keys = list(dict.keys())
    #        reversed_keys.reverse()
    #        for item in reversed_keys:
    #            result[item] = dict[item]
    #        return result
        
    #    con.register_scalar('swap_keys', swap_keys, [con.struct_type({'a': BIGINT, 'b': VARCHAR})], con.struct_type({'a': VARCHAR, 'b': BIGINT}))
    #    res = con.sql("""
    #    select swap_keys({'a': 42, 'b': 'answer_to_life'})
    #    """).fetchall()
    #    assert res == [({'a': 'answer_to_life', 'b': 42},)]
