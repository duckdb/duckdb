import duckdb
import datetime
import numpy as np
import pytest
import copy
from conftest import NumpyPandas, ArrowPandas

NULL = None


def check_equal(conn, df, reference_query):
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute(reference_query)
    res = duckdb_conn.query('SELECT * FROM tbl').fetchall()
    df_res = duckdb_conn.query('SELECT * FROM tbl').df()
    out = conn.sql("SELECT * FROM df").fetchall()
    assert res == out


def create_reference_query(data):
    query = "CREATE TABLE tbl AS SELECT " + str(data).replace("None", "NULL")
    return query


class TestDFRecursiveNested(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_list_of_structs(self, duckdb_cursor, pandas):
        data = [[{'a': 5}, NULL, {'a': NULL}], NULL, [{'a': 5}, NULL, {'a': NULL}]]
        reference_query = create_reference_query(data)
        df = pandas.DataFrame([{'a': data}])
        check_equal(duckdb_cursor, df, reference_query)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_list_of_map(self, duckdb_cursor, pandas):
        # LIST(MAP(VARCHAR, VARCHAR))
        data = [
            [{'key': [5], 'value': [NULL]}, NULL, {'key': [], 'value': []}],
            NULL,
            [NULL, {'key': [3, 2, 4], 'value': [NULL, 'a', NULL]}, {'key': ['a', 'b', 'c'], 'value': [1, 2, 3]}],
        ]
        reference_data = [
            [{'key': ['5'], 'value': [NULL]}, NULL, {'key': [], 'value': []}],
            NULL,
            [
                NULL,
                {'key': ['3', '2', '4'], 'value': [NULL, 'a', NULL]},
                {'key': ['a', 'b', 'c'], 'value': ['1', '2', '3']},
            ],
        ]
        reference_query = create_reference_query(reference_data)
        df = pandas.DataFrame([{'a': data}])
        check_equal(duckdb_cursor, df, reference_query)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_recursive_list(self, duckdb_cursor, pandas):
        # LIST(LIST(LIST(LIST(INTEGER))))
        data = [[[[3, NULL, 5], NULL], NULL, [[5, -20, NULL]]], NULL, [[[NULL]], [[]], NULL]]
        reference_query = create_reference_query(data)
        df = pandas.DataFrame([{'a': data}])
        check_equal(duckdb_cursor, df, reference_query)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_recursive_struct(self, duckdb_cursor, pandas):
        # STRUCT(STRUCT(STRUCT(LIST)))
        data = {
            'A': {'a': {'1': [1, 2, 3]}, 'b': NULL, 'c': {'1': NULL}},
            'B': {'a': {'1': [1, NULL, 3]}, 'b': NULL, 'c': {'1': NULL}},
        }
        reference_query = create_reference_query(data)
        df = pandas.DataFrame([{'a': data}])
        check_equal(duckdb_cursor, df, reference_query)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_recursive_map(self, duckdb_cursor, pandas):
        # MAP(
        # 	MAP(
        # 		INTEGER,
        # 		MAP(INTEGER)
        # 	),
        # 	INTEGER
        # )
        data = {
            'key': [
                {'key': [5, 6, 7], 'value': [{'key': [8], 'value': [NULL]}, NULL, {'key': [9], 'value': ['a']}]},
                {'key': [], 'value': []},
            ],
            'value': [1, 2],
        }
        reference_query = create_reference_query(data)
        df = pandas.DataFrame([{'a': data}])
        check_equal(duckdb_cursor, df, reference_query)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_recursive_stresstest(self, duckdb_cursor, pandas):
        # LIST(
        # 	STRUCT(
        # 		MAP(
        # 			STRUCT(
        # 				LIST(
        # 					INTEGER
        # 				)
        # 			)
        # 			LIST(
        # 				STRUCT(
        # 					VARCHAR
        # 				)
        # 			)
        # 		)
        # 	)
        # )
        data = [
            {
                'a': {
                    'key': [
                        {'1': [5, 4, 3], '2': [8, 7, 6], '3': [1, 2, 3]},
                        {'1': [], '2': NULL, '3': [NULL, 0, NULL]},
                    ],
                    'value': [[{'A': 'abc', 'B': 'def', 'C': NULL}], [NULL]],
                },
                'b': NULL,
                'c': {'key': [], 'value': []},
            }
        ]
        reference_query = create_reference_query(data)
        df = pandas.DataFrame([{'a': data}])
        check_equal(duckdb_cursor, df, reference_query)
