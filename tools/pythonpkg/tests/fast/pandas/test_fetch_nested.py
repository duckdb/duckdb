import pytest
import duckdb
import sys

pd = pytest.importorskip("pandas")
import numpy as np


def compare_results(con, query, expected):
    expected = pd.DataFrame.from_dict(expected)

    unsorted_res = con.query(query).df()
    print(unsorted_res, unsorted_res['a'][0].__class__)
    df_duck = con.query("select * from unsorted_res order by all").df()
    print(df_duck, df_duck['a'][0].__class__)
    print(expected, expected['a'][0].__class__)
    pd.testing.assert_frame_equal(df_duck, expected)


def list_test_cases():
    # fmt: off
    test_cases = [
        ("SELECT list_value(3,5,10) as a", {
            'a': [
                [3, 5, 10]
            ]
        }),
        ("SELECT list_value(3,5,NULL) as a", {
            'a': [
                np.ma.array(
                    [3, 5, 0],
                mask=[0, 0, 1],
            )
            ]
        }),
        ("SELECT list_value(NULL,NULL,NULL) as a", {
            'a': [
                np.ma.array(
                    [0, 0, 0],
                mask=[1, 1, 1],
            )
            ]
        }),
        ("SELECT list_value() as a", {
            'a': [
                np.array([])
            ]
        }),
        ("SELECT list_value('test','test_one','test_two') as a", {
            'a': [
                np.array([
                    'test', 'test_one', 'test_two'
                ])
            ]
        }),
        ("SELECT a from (SELECT LIST(i) as a FROM range(10000) tbl(i)) as t", {
            'a': [
                list(range(0, 10000))
            ]
        }),
        ("SELECT LIST(i) as a FROM range(5) tbl(i) group by i%2 order by all", {
            'a': [
                [0, 2, 4],
                [1, 3]
            ]
        }),
        ("SELECT list_value(1) as a FROM range(5) tbl(i)", {
            'a': [
                [1],
                [1],
                [1],
                [1],
                [1]
            ]
        }),
        ("""
            SELECT
                list(st) AS a
            FROM
            (
                SELECT
                    i,
                    CASE WHEN i%5
                        THEN NULL
                        ELSE i::VARCHAR
                    END AS st
                FROM range(10) tbl(i)
            ) AS t
            GROUP BY i%2
            ORDER BY all
        """, {
            'a': [
                ['0', None, None, None, None],
                [None, None, '5', None, None]
            ]
        }),
    ]

    return test_cases
    # These tests are problematic
    # They cause errors on highest NumPy version supported by 3.7
    # And a crash on manylinux 2014, x86_64 AMD, python 3.12.1
    test_cases.extend([
        ("""
            SELECT * from values
                ([[1, 3], [0,2,4]])
            t(a)
        """, {
            'a': [
                [
                    [1, 3],
                    [0, 2, 4]
                ]
            ]
        }),
        ("""
            SELECT * from values
                ([[[[[0, 2, 4], [1, 3]]]]])
            t(a)
        """, {
            'a': [
                [[[[[0, 2, 4], [1, 3]]]]]
            ]
        }),
        ("""
            select * from values
                ([
                    [NULL],
                    [NULL],
                    [2, 6],
                    [3]
                ])
            t(a)
        """, {
            'a': [
                np.array([
                    np.ma.array([0], mask=[1], dtype=np.int32()),
                    np.ma.array([0], mask=[1], dtype=np.int32()),
                    np.array([2, 6]),
                    np.array([3]),
                ], dtype='object')
            ]
        }),
    ])
    # fmt: on
    return test_cases


class TestFetchNested(object):
    @pytest.mark.parametrize('query, expected', list_test_cases())
    def test_fetch_df_list(self, duckdb_cursor, query, expected):
        compare_results(duckdb_cursor, query, expected)

    # fmt: off
    @pytest.mark.parametrize('query, expected', [
        ("SELECT a from (SELECT STRUCT_PACK(a := 42, b := 43) as a) as t", {
            'a': [
                {'a': 42, 'b': 43}
            ]
        }),
        ("SELECT a from (SELECT STRUCT_PACK(a := NULL) as a) as t", {
            'a': [
                {'a': None}
            ]
        }),
        ("SELECT a from (SELECT STRUCT_PACK(a := i, b := i) as a FROM range(10) tbl(i)) as t", {
            'a': [
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
            ]
        }),
        ("SELECT a from (SELECT STRUCT_PACK(a := LIST_VALUE(1,2,3), b := i) as a FROM range(10) tbl(i)) as t", {
            'a': [
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
            ]
        }),
    ])
    # fmt: on
    def test_struct_df(self, duckdb_cursor, query, expected):
        compare_results(duckdb_cursor, query, expected)

    # fmt: off
    @pytest.mark.parametrize('query, expected, expected_error', [
        ("SELECT a from (select MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, 7)) as a) as t", {
            'a': [
                {
                    '1':10,
                    '2':9,
                    '3':8,
                    '4':7
                }
            ]
        }, ""),
        ("SELECT a from (select MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, 7)) as a) as t", {
            'a': [
                {
                    '1':10,
                    '2':9,
                    '3':8,
                    '4':7
                }
            ]
        }, ""),
        ("SELECT a from (select MAP(LIST_VALUE(),LIST_VALUE()) as a) as t", {
            'a': [
                {}
            ]
        }, ""),
        ("SELECT m as a from (select MAP(list_value(1), list_value(2)) from range(5) tbl(i)) tbl(m)", {
            'a': [
                {
                    '1':2
                },
                {
                    '1':2
                },
                {
                    '1':2
                },
                {
                    '1':2
                },
                {
                    '1':2
                }
            ]
        }, ""),
        ("SELECT m as a from (select MAP(lsta,lstb) as m from (SELECT list(i) as lsta, list(i) as lstb from range(10) tbl(i) group by i%5 order by all) as lst_tbl) as T", {
            'a': [
                {
                    '0':0,
                    '5':5
                },
                {
                    '1':1,
                    '6':6
                },
                {
                    '2':2,
                    '7':7
                },
                {
                    '3':3,
                    '8':8
                },
                {
                    '4':4,
                    '9':9
                }
            ]
        }, ""),
        ("SELECT a from (select MAP(LIST_VALUE(1, 2, 3, 4,2, NULL),LIST_VALUE(10, 9, 8, 7,11,42)) as a) as t", {
            'a': [
                {
                    '1':10,
                    '2':9,
                    '3':8,
                    '4':7,
                    '2':11,
                    None:42,
                }
            ]
        }, "Map keys must be unique"),
        ("SELECT a from (select MAP(LIST_VALUE('Jon Lajoie', 'Backstreet Boys', 'Tenacious D','Jon Lajoie' ),LIST_VALUE(10,9,10,11)) as a) as t", {
            'a': [
                {
                    'key': ['Jon Lajoie', 'Backstreet Boys', 'Tenacious D', 'Jon Lajoie'],
                    'value': [10, 9, 10, 11]
                }
            ]
        }, "Map keys must be unique"),
        ("SELECT a from (select MAP(LIST_VALUE('Jon Lajoie', NULL, 'Tenacious D',NULL,NULL ),LIST_VALUE(10,9,10,11,13)) as a) as t", {
            'a': [
                {
                    'key': ['Jon Lajoie', None, 'Tenacious D', None, None],
                    'value': [10, 9, 10, 11, 13]
                }
            ]
        }, "Map keys can not be NULL"),
        ("SELECT a from (select MAP(LIST_VALUE(NULL, NULL, NULL,NULL,NULL ),LIST_VALUE(10,9,10,11,13)) as a) as t", {
            'a': [
                {
                    'key': [None, None, None, None, None],
                    'value': [10, 9, 10, 11, 13]
                }
            ]
        }, "Map keys can not be NULL"),
        ("SELECT a from (select MAP(LIST_VALUE(NULL, NULL, NULL,NULL,NULL ),LIST_VALUE(NULL, NULL, NULL,NULL,NULL )) as a) as t", {
            'a': [
                {
                    'key': [None, None, None, None, None],
                    'value': [None, None, None, None, None]
                }
            ]
        }, "Map keys can not be NULL"),
    ])
    # fmt: on
    def test_map_df(self, duckdb_cursor, query, expected, expected_error):
        if not expected_error:
            compare_results(duckdb_cursor, query, expected)
        else:
            with pytest.raises(duckdb.InvalidInputException, match=expected_error):
                compare_results(duckdb_cursor, query, expected)

    # fmt: off
    @pytest.mark.parametrize('query, expected', [
        ("""
            SELECT [
                {'i':1,'j':2},
                NULL,
                {'i':2,'j':NULL}
            ] as a
        """, {
            'a': [
                np.ma.array([
                    {'i': 1, 'j': 2},
                    None,
                    {'i': 2, 'j': None}
                ], mask=[0, 1, 0])
            ]
        }),
        ("""
            SELECT [{'i':1,'j':2},NULL,{'i':2,'j':NULL}] as a
        """, {
            'a': [
                np.ma.array([
                    {'i': 1, 'j': 2},
                    None,
                    {'i': 2, 'j': None}
                ], mask=[0, 1, 0])
            ]
        }),
        ("""
            SELECT [{'i':1,'j':[2,3]},NULL] as a
        """, {
            'a': [
                np.ma.array([
                    {'i': 1, 'j': [2, 3]},
                    None,
                ], mask=[0, 1])
            ]
        }),
        ("""
            SELECT {'i':mp,'j':mp2} as a FROM (SELECT MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, 7)) as mp, MAP(LIST_VALUE(1, 2, 3, 5),LIST_VALUE(10, 9, 8, 7)) as mp2) as t
        """, {
            'a': [
                {
                    'i': {
                        '1':10,
                        '2':9,
                        '3':8,
                        '4':7
                    },
                    'j': {
                        '1':10,
                        '2':9,
                        '3':8,
                        '5':7
                    },
                }
            ]
        }),
        ("""
            SELECT [mp,mp2] as a FROM (SELECT MAP(LIST_VALUE(1, 2, 3, 4),LIST_VALUE(10, 9, 8, 7)) as mp, MAP(LIST_VALUE(1, 2, 3, 5),LIST_VALUE(10, 9, 8, 7)) as mp2) as t
        """, {
            'a': [
                [
                    {
                        '1':10,
                        '2':9,
                        '3':8,
                        '4':7
                    },
                    {
                        '1':10,
                        '2':9,
                        '3':8,
                        '5':7
                    }
                ]
            ]
        }),
        ("""
            SELECT MAP(LIST_VALUE([1,2],[3,4],[5,4]),LIST_VALUE([1,2],[3,4],[5,4])) as a
        """, {
            'a': [
                {'key': [[1, 2], [3, 4], [5, 4]], 'value': [[1, 2], [3, 4], [5, 4]]}
            ]
        }),
        ("""
            SELECT MAP(LIST_VALUE({'i':1,'j':2},{'i':3,'j':4}),LIST_VALUE({'i':1,'j':2},{'i':3,'j':4})) as a
        """, {
            'a': [
                {'key': [{'i': 1, 'j': 2}, {'i': 3, 'j': 4}], 'value': [{'i': 1, 'j': 2}, {'i': 3, 'j': 4}]}
            ]
        }),
        ("""
            SELECT [{'i':1,'j':[2,3]},NULL,{'i':1,'j':[2,3]}] as a
        """, {
            'a': [
                np.ma.array([
                    {'i': 1, 'j': [2, 3]},
                    None,
                    {'i': 1, 'j': [2, 3]}
                ], mask=[0, 1, 0])
            ]
        }),
        ("""
            SELECT * FROM (VALUES
            ({'i':1,'j':2}),
            ({'i':1,'j':2}),
            (NULL),
            (NULL)
        ) t(a)
        """, {
            'a': [
                {'i': 1, 'j': 2},
                {'i': 1, 'j': 2},
                pd.NA,
                pd.NA
            ]
        }),
        ("""
            SELECT a FROM (VALUES
                (MAP(LIST_VALUE(1,2),LIST_VALUE(3,4))),
                (MAP(LIST_VALUE(1,2),LIST_VALUE(3,4))),
                (NULL),
                (NULL)
            ) t(a)
        """, {
            'a': [
                {
                    '1':3,
                    '2':4
                },
                {
                    '1':3,
                    '2':4
                },
                pd.NA,
                pd.NA
            ]
        }),
    ])
    # fmt: on
    def test_nested_mix(self, duckdb_cursor, query, expected):
        compare_results(duckdb_cursor, query, expected)
