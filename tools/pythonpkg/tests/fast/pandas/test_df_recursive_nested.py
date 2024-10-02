import duckdb
import datetime
import numpy as np
import pytest
import copy
from conftest import NumpyPandas, ArrowPandas
from duckdb import Value

NULL = None


def check_equal(conn, df, reference_query, data):
    duckdb_conn = duckdb.connect()
    duckdb_conn.execute(reference_query, parameters=[data])
    res = duckdb_conn.query('SELECT * FROM tbl').fetchall()
    df_res = duckdb_conn.query('SELECT * FROM tbl').df()
    out = conn.sql("SELECT * FROM df").fetchall()
    assert res == out


def create_reference_query():
    query = "CREATE TABLE tbl AS SELECT $1"
    return query


class TestDFRecursiveNested(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_list_of_structs(self, duckdb_cursor, pandas):
        data = [[{'a': 5}, NULL, {'a': NULL}], NULL, [{'a': 5}, NULL, {'a': NULL}]]
        reference_query = create_reference_query()
        df = pandas.DataFrame([{'a': data}])
        check_equal(duckdb_cursor, df, reference_query, Value(data, 'STRUCT(a INTEGER)[]'))

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_list_of_map(self, duckdb_cursor, pandas):
        # LIST(MAP(VARCHAR, VARCHAR))
        data = [[{5: NULL}, NULL, {}], NULL, [NULL, {3: NULL, 2: 'a', 4: NULL}, {'a': 1, 'b': 2, 'c': 3}]]
        reference_query = create_reference_query()
        print(reference_query)
        df = pandas.DataFrame([{'a': data}])
        check_equal(duckdb_cursor, df, reference_query, Value(data, 'MAP(VARCHAR, VARCHAR)[][]'))

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_recursive_list(self, duckdb_cursor, pandas):
        # LIST(LIST(LIST(LIST(INTEGER))))
        data = [[[[3, NULL, 5], NULL], NULL, [[5, -20, NULL]]], NULL, [[[NULL]], [[]], NULL]]
        reference_query = create_reference_query()
        df = pandas.DataFrame([{'a': data}])
        check_equal(duckdb_cursor, df, reference_query, Value(data, 'INTEGER[][][][]'))

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_recursive_struct(self, duckdb_cursor, pandas):
        # STRUCT(STRUCT(STRUCT(LIST)))
        data = {
            'A': {'a': {'1': [1, 2, 3]}, 'b': NULL, 'c': {'1': NULL}},
            'B': {'a': {'1': [1, NULL, 3]}, 'b': NULL, 'c': {'1': NULL}},
        }
        reference_query = create_reference_query()
        df = pandas.DataFrame([{'a': data}])
        check_equal(
            duckdb_cursor,
            df,
            reference_query,
            Value(
                data,
                """
            STRUCT(
                A STRUCT(
                    a STRUCT(
                        "1" INTEGER[]
                    ),
                    b STRUCT(
                        "1" INTEGER[]
                    ),
                    c STRUCT(
                        "1" INTEGER[]
                    )
                ),
                B STRUCT(
                    a STRUCT(
                        "1" INTEGER[]
                    ),
                    b STRUCT(
                        "1" INTEGER[]
                    ),
                    c STRUCT(
                        "1" INTEGER[]
                    )
                )
            )
        """,
            ),
        )

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
        reference_query = create_reference_query()
        df = pandas.DataFrame([{'a': data}])
        check_equal(
            duckdb_cursor, df, reference_query, Value(data, 'MAP(MAP(INTEGER, MAP(INTEGER, VARCHAR)), INTEGER)')
        )

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_recursive_stresstest(self, duckdb_cursor, pandas):
        data = [
            {
                'a': {
                    'key': [
                        # key 1
                        {'1': [5, 4, 3], '2': [8, 7, 6], '3': [1, 2, 3]},
                        # key 2
                        {'1': [], '2': NULL, '3': [NULL, 0, NULL]},
                    ],
                    'value': [
                        # value 1
                        [{'A': 'abc', 'B': 'def', 'C': NULL}],
                        # value 2
                        [NULL],
                    ],
                },
                'b': NULL,
                'c': {'key': [], 'value': []},
            }
        ]
        reference_query = create_reference_query()
        df = pandas.DataFrame([{'a': data}])
        duckdb_type = """
            STRUCT(
                a MAP(
                    STRUCT(
                        "1" INTEGER[],
                        "2" INTEGER[],
                        "3" INTEGER[]
                    ),
                    STRUCT(
                        A VARCHAR,
                        B VARCHAR,
                        C VARCHAR
                    )[]
                ),
                b INTEGER,
                c MAP(VARCHAR, VARCHAR)
            )[]
        """
        check_equal(
            duckdb_cursor,
            df,
            reference_query,
            Value(
                data,
                type=duckdb_type,
            ),
        )
