import pytest
import duckdb
import pandas as pd
import numpy as np


def compare_results(query, list_values=[]):
    df_duck = duckdb.query(query).df()
    counter = 0
    duck_values = df_duck['a']
    for duck_value in duck_values:
        assert duck_value == list_values[counter]
        counter += 1


class TestFetchNested(object):
    def test_fetch_df_list(self, duckdb_cursor):
        # Integers
        compare_results("SELECT a from (select list_value(3,5,10) as a) as t", [[3, 5, 10]])
        compare_results("SELECT a from (select list_value(3,5,NULL) as a) as t", [[3, 5, None]])
        compare_results("SELECT a from (select list_value(NULL,NULL,NULL) as a) as t", [[None, None, None]])
        compare_results("SELECT a from (select list_value() as a) as t", [[]])

        # Strings
        compare_results(
            "SELECT a from (select list_value('test','test_one','test_two') as a) as t",
            [['test', 'test_one', 'test_two']],
        )
        compare_results(
            "SELECT a from (select list_value('test','test_one',NULL) as a) as t", [['test', 'test_one', None]]
        )

        # Big Lists
        compare_results("SELECT a from (SELECT LIST(i) as a FROM range(10000) tbl(i)) as t", [list(range(0, 10000))])

        # #Multiple Lists
        compare_results(
            "SELECT a from (SELECT LIST(i) as a FROM range(5) tbl(i) group by i%2) as t", [[0, 2, 4], [1, 3]]
        )

        # Unique Constants
        compare_results(
            "SELECT a from (SELECT list_value(1) as a FROM range(5) tbl(i)) as t", [[1], [1], [1], [1], [1]]
        )

        # Nested Lists
        compare_results(
            "SELECT LIST(le) as a FROM (SELECT LIST(i) le from range(5) tbl(i) group by i%2) as t",
            [[[0, 2, 4], [1, 3]]],
        )

        # LIST[LIST[LIST[LIST[LIST[INTEGER]]]]]]
        compare_results(
            "SELECT list (lllle)  as a from (SELECT list (llle) lllle from (SELECT list(lle) llle from (SELECT LIST(le) lle FROM (SELECT LIST(i) le from range(5) tbl(i) group by i%2) as t) as t1) as t2) as t3",
            [[[[[[0, 2, 4], [1, 3]]]]]],
        )

        compare_results(
            '''SELECT grp,lst,a FROM (select grp, lst, case when grp>1 then lst else list_value(null) end as a
                         from (SELECT a_1%4 as grp, list(a_1) as lst FROM range(7) tbl(a_1) group by grp order by all) as lst_tbl) as T;''',
            [[None], [None], [2, 6], [3]],
        )

        # Tests for converting multiple lists to/from Pandas with NULL values and/or strings
        compare_results(
            "SELECT list(st) as a from (select i, case when i%5 then NULL else i::VARCHAR end as st from range(10) tbl(i)) as t group by i%2",
            [['0', None, None, None, None], [None, None, '5', None, None]],
        )

    def test_struct_df(self, duckdb_cursor):
        compare_results("SELECT a from (SELECT STRUCT_PACK(a := 42, b := 43) as a) as t", [{'a': 42, 'b': 43}])

        compare_results("SELECT a from (SELECT STRUCT_PACK(a := NULL, b := 43) as a) as t", [{'a': None, 'b': 43}])

        compare_results("SELECT a from (SELECT STRUCT_PACK(a := NULL) as a) as t", [{'a': None}])

        compare_results(
            "SELECT a from (SELECT STRUCT_PACK(a := i, b := i) as a FROM range(10) tbl(i)) as t",
            [
                {'a': 0, 'b': 0},
                {'a': 1, 'b': 1},
                {'a': 2, 'b': 2},
                {'a': 3, 'b': 3},
                {'a': 4, 'b': 4},
                {'a': 5, 'b': 5},
                {'a': 6, 'b': 6},
                {'a': 7, 'b': 7},
                {'a': 8, 'b': 8},
                {'a': 9, 'b': 9},
            ],
        )

        compare_results(
            "SELECT a from (SELECT STRUCT_PACK(a := LIST_VALUE(1,2,3), b := i) as a FROM range(10) tbl(i)) as t",
            [
                {'a': [1, 2, 3], 'b': 0},
                {'a': [1, 2, 3], 'b': 1},
                {'a': [1, 2, 3], 'b': 2},
                {'a': [1, 2, 3], 'b': 3},
                {'a': [1, 2, 3], 'b': 4},
                {'a': [1, 2, 3], 'b': 5},
                {'a': [1, 2, 3], 'b': 6},
                {'a': [1, 2, 3], 'b': 7},
                {'a': [1, 2, 3], 'b': 8},
                {'a': [1, 2, 3], 'b': 9},
            ],
        )

    def test_map_df(self, duckdb_cursor):
        compare_results(
            "SELECT a from (select MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, 7)) as a) as t",
            [{'key': [1, 2, 3, 4], 'value': [10, 9, 8, 7]}],
        )

        with pytest.raises(duckdb.InvalidInputException, match="Map keys have to be unique"):
            compare_results(
                "SELECT a from (select MAP(LIST_VALUE(1, 2, 3, 4,2, NULL),LIST_VALUE(10, 9, 8, 7,11,42)) as a) as t",
                [{'key': [1, 2, 3, 4, 2, None], 'value': [10, 9, 8, 7, 11, 42]}],
            )

        compare_results("SELECT a from (select MAP(LIST_VALUE(),LIST_VALUE()) as a) as t", [{'key': [], 'value': []}])

        with pytest.raises(duckdb.InvalidInputException, match="Map keys have to be unique"):
            compare_results(
                "SELECT a from (select MAP(LIST_VALUE('Jon Lajoie', 'Backstreet Boys', 'Tenacious D','Jon Lajoie' ),LIST_VALUE(10,9,10,11)) as a) as t",
                [{'key': ['Jon Lajoie', 'Backstreet Boys', 'Tenacious D', 'Jon Lajoie'], 'value': [10, 9, 10, 11]}],
            )

        with pytest.raises(duckdb.InvalidInputException, match="Map keys can not be NULL"):
            compare_results(
                "SELECT a from (select MAP(LIST_VALUE('Jon Lajoie', NULL, 'Tenacious D',NULL,NULL ),LIST_VALUE(10,9,10,11,13)) as a) as t",
                [{'key': ['Jon Lajoie', None, 'Tenacious D', None, None], 'value': [10, 9, 10, 11, 13]}],
            )

        with pytest.raises(duckdb.InvalidInputException, match="Map keys can not be NULL"):
            compare_results(
                "SELECT a from (select MAP(LIST_VALUE(NULL, NULL, NULL,NULL,NULL ),LIST_VALUE(10,9,10,11,13)) as a) as t",
                [{'key': [None, None, None, None, None], 'value': [10, 9, 10, 11, 13]}],
            )

        with pytest.raises(duckdb.InvalidInputException, match="Map keys can not be NULL"):
            compare_results(
                "SELECT a from (select MAP(LIST_VALUE(NULL, NULL, NULL,NULL,NULL ),LIST_VALUE(NULL, NULL, NULL,NULL,NULL )) as a) as t",
                [{'key': [None, None, None, None, None], 'value': [None, None, None, None, None]}],
            )

        compare_results(
            "SELECT m as a from (select MAP(list_value(1), list_value(2)) from range(5) tbl(i)) tbl(m)",
            [
                {'key': [1], 'value': [2]},
                {'key': [1], 'value': [2]},
                {'key': [1], 'value': [2]},
                {'key': [1], 'value': [2]},
                {'key': [1], 'value': [2]},
            ],
        )

        compare_results(
            "SELECT m as a from (select MAP(lsta,lstb) as m from (SELECT list(i) as lsta, list(i) as lstb from range(10) tbl(i) group by i%5 order by all) as lst_tbl) as T",
            [
                {'key': [0, 5], 'value': [0, 5]},
                {'key': [1, 6], 'value': [1, 6]},
                {'key': [2, 7], 'value': [2, 7]},
                {'key': [3, 8], 'value': [3, 8]},
                {'key': [4, 9], 'value': [4, 9]},
            ],
        )

    def test_nested_mix(self, duckdb_cursor):
        # List of structs W/ Struct that is NULL entirely
        compare_results(
            "SELECT [{'i':1,'j':2},NULL,{'i':2,'j':NULL}] as a", [[{'i': 1, 'j': 2}, None, {'i': 2, 'j': None}]]
        )

        # Lists of structs with lists
        compare_results("SELECT [{'i':1,'j':[2,3]},NULL] as a", [[{'i': 1, 'j': [2, 3]}, None]])

        # Maps embedded in a struct
        compare_results(
            "SELECT {'i':mp,'j':mp2} as a FROM (SELECT MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, 7)) as mp, MAP(LIST_VALUE(1, 2, 3, 5),LIST_VALUE(10, 9, 8, 7)) as mp2) as t",
            [{'i': {'key': [1, 2, 3, 4], 'value': [10, 9, 8, 7]}, 'j': {'key': [1, 2, 3, 5], 'value': [10, 9, 8, 7]}}],
        )

        # List of maps
        compare_results(
            "SELECT [mp,mp2] as a FROM (SELECT MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, 7)) as mp, MAP(LIST_VALUE(1, 2, 3, 5),LIST_VALUE(10, 9, 8, 7)) as mp2) as t",
            [[{'key': [1, 2, 3, 4], 'value': [10, 9, 8, 7]}, {'key': [1, 2, 3, 5], 'value': [10, 9, 8, 7]}]],
        )

        # Map with list as key and/or value
        compare_results(
            "SELECT MAP(LIST_VALUE([1,2],[3,4],[5,4]),LIST_VALUE([1,2],[3,4],[5,4])) as a",
            [{'key': [[1, 2], [3, 4], [5, 4]], 'value': [[1, 2], [3, 4], [5, 4]]}],
        )

        # Map with struct as key and/or value
        compare_results(
            "SELECT MAP(LIST_VALUE({'i':1,'j':2},{'i':3,'j':4}),LIST_VALUE({'i':1,'j':2},{'i':3,'j':4})) as a",
            [{'key': [{'i': 1, 'j': 2}, {'i': 3, 'j': 4}], 'value': [{'i': 1, 'j': 2}, {'i': 3, 'j': 4}]}],
        )

        # Null checks on lists with structs
        compare_results(
            "SELECT [{'i':1,'j':[2,3]},NULL,{'i':1,'j':[2,3]}] as a",
            [[{'i': 1, 'j': [2, 3]}, None, {'i': 1, 'j': [2, 3]}]],
        )

        # Struct that is NULL entirely
        df_duck = duckdb.query("SELECT col0 as a FROM (VALUES ({'i':1,'j':2}), (NULL), ({'i':1,'j':2}), (NULL))").df()
        duck_values = df_duck['a']
        assert duck_values[0] == {'i': 1, 'j': 2}
        assert np.isnan(duck_values[1])
        assert duck_values[2] == {'i': 1, 'j': 2}
        assert np.isnan(duck_values[3])

        # MAP that is NULL entirely
        df_duck = duckdb.query(
            "SELECT col0 as a FROM (VALUES (MAP(LIST_VALUE(1,2),LIST_VALUE(3,4))),(NULL), (MAP(LIST_VALUE(1,2),LIST_VALUE(3,4))), (NULL))"
        ).df()
        duck_values = df_duck['a']
        assert duck_values[0] == {'key': [1, 2], 'value': [3, 4]}
        assert np.isnan(duck_values[1])
        assert duck_values[2] == {'key': [1, 2], 'value': [3, 4]}
        assert np.isnan(duck_values[3])
