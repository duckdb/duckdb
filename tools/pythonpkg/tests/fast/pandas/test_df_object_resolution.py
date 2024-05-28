import duckdb
import datetime
import numpy as np
import platform
import pytest
import decimal
import math
from decimal import Decimal
import re
from conftest import NumpyPandas, ArrowPandas

standard_vector_size = duckdb.__standard_vector_size__


def create_generic_dataframe(data, pandas):
    return pandas.DataFrame({'0': pandas.Series(data=data, dtype='object')})


def create_repeated_nulls(size):
    data = [None, "a"]
    n = size
    data = data * n
    return data


def create_trailing_non_null(size):
    data = [None for _ in range(size - 1)]
    data.append('this is a long string')
    return data


class IntString:
    def __init__(self, value: int):
        self.value = value

    def __str__(self):
        return str(self.value)


# To avoid DECIMAL being upgraded to DOUBLE (because DOUBLE outranks DECIMAL as a LogicalType)
# These floats had their precision preserved as string and are now cast to decimal.Decimal
def ConvertStringToDecimal(data: list, pandas):
    for i in range(len(data)):
        if isinstance(data[i], str):
            data[i] = decimal.Decimal(data[i])
    data = pandas.Series(data=data, dtype='object')
    return data


class ObjectPair:
    def __init__(self, obj1, obj2):
        self.first = obj1
        self.second = obj2

    def __repr__(self):
        return str([self.first, self.second])


def construct_list(pair):
    return [[pair.first], [pair.second]]


def construct_struct(pair):
    return [{'v1': pair.first}, {'v1': pair.second}]


def construct_map(pair):
    return [
        {'key': ['v1', 'v2'], "value": [pair.first, pair.first]},
        {'key': ['v1', 'v2'], "value": [pair.second, pair.second]},
    ]


def check_struct_upgrade(expected_type: str, creation_method, pair: ObjectPair, pandas, cursor):
    column_data = creation_method(pair)
    df = pandas.DataFrame(data={"col": column_data})
    rel = cursor.query("select col from df")
    res = rel.fetchall()
    print("COLUMN_DATA", column_data)
    print("RESULT", res)
    assert expected_type == rel.types[0]


class TestResolveObjectColumns(object):
    # TODO: add support for ArrowPandas
    @pytest.mark.parametrize('pandas', [NumpyPandas()])
    def test_integers(self, pandas, duckdb_cursor):
        data = [5, 0, 3]
        df_in = create_generic_dataframe(data, pandas)
        # These are float64 because pandas would force these to be float64 even if we set them to int8, int16, int32, int64 respectively
        df_expected_res = pandas.DataFrame({'0': pandas.Series(data=data, dtype='int32')})
        df_out = duckdb_cursor.sql("SELECT * FROM df_in").df()
        print(df_out)
        pandas.testing.assert_frame_equal(df_expected_res, df_out)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_struct_correct(self, pandas, duckdb_cursor):
        data = [{'a': 1, 'b': 3, 'c': 3, 'd': 7}]
        df = pandas.DataFrame({'0': pandas.Series(data=data)})
        duckdb_col = duckdb_cursor.sql("SELECT {a: 1, b: 3, c: 3, d: 7} as '0'").df()
        converted_col = duckdb_cursor.sql("SELECT * FROM df").df()
        pandas.testing.assert_frame_equal(duckdb_col, converted_col)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_map_fallback_different_keys(self, pandas, duckdb_cursor):
        x = pandas.DataFrame(
            [
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'e': 7}],  #'e' instead of 'd' as key
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
            ]
        )

        converted_df = duckdb_cursor.sql("SELECT * FROM x").df()
        y = pandas.DataFrame(
            [
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'e'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
            ]
        )
        equal_df = duckdb_cursor.sql("SELECT * FROM y").df()
        pandas.testing.assert_frame_equal(converted_df, equal_df)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_map_fallback_incorrect_amount_of_keys(self, pandas, duckdb_cursor):
        x = pandas.DataFrame(
            [
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3}],  # incorrect amount of keys
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
            ]
        )
        converted_df = duckdb_cursor.sql("SELECT * FROM x").df()
        y = pandas.DataFrame(
            [
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c'], 'value': [1, 3, 3]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
            ]
        )
        equal_df = duckdb_cursor.sql("SELECT * FROM y").df()
        pandas.testing.assert_frame_equal(converted_df, equal_df)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_struct_value_upgrade(self, pandas, duckdb_cursor):
        x = pandas.DataFrame(
            [
                [{'a': 1, 'b': 3, 'c': 3, 'd': 'string'}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
            ]
        )
        y = pandas.DataFrame(
            [
                [{'a': 1, 'b': 3, 'c': 3, 'd': 'string'}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': '7'}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': '7'}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': '7'}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': '7'}],
            ]
        )
        converted_df = duckdb_cursor.sql("SELECT * FROM x").df()
        equal_df = duckdb_cursor.sql("SELECT * FROM y").df()
        pandas.testing.assert_frame_equal(converted_df, equal_df)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_struct_null(self, pandas, duckdb_cursor):
        x = pandas.DataFrame(
            [
                [None],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
            ]
        )
        y = pandas.DataFrame(
            [
                [None],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
            ]
        )
        converted_df = duckdb_cursor.sql("SELECT * FROM x").df()
        equal_df = duckdb_cursor.sql("SELECT * FROM y").df()
        pandas.testing.assert_frame_equal(converted_df, equal_df)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_map_fallback_value_upgrade(self, pandas, duckdb_cursor):
        x = pandas.DataFrame(
            [
                [{'a': 1, 'b': 3, 'c': 3, 'd': 'test'}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
            ]
        )
        y = pandas.DataFrame(
            [
                [{'a': '1', 'b': '3', 'c': '3', 'd': 'test'}],
                [{'a': '1', 'b': '3', 'c': '3', 'd': '7'}],
                [{'a': '1', 'b': '3', 'c': '3'}],
                [{'a': '1', 'b': '3', 'c': '3', 'd': '7'}],
                [{'a': '1', 'b': '3', 'c': '3', 'd': '7'}],
            ]
        )
        converted_df = duckdb_cursor.sql("SELECT * FROM x").df()
        equal_df = duckdb_cursor.sql("SELECT * FROM y").df()
        pandas.testing.assert_frame_equal(converted_df, equal_df)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_map_correct(self, pandas, duckdb_cursor):
        x = pandas.DataFrame(
            [
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
            ]
        )
        x.rename(columns={0: 'a'}, inplace=True)
        converted_col = duckdb_cursor.sql("select * from x as 'a'").df()
        duckdb_cursor.sql(
            """
            CREATE TABLE tmp(
                a MAP(VARCHAR, INTEGER)
            );
        """
        )
        for _ in range(5):
            duckdb_cursor.sql(
                """
                INSERT INTO tmp VALUES (MAP(['a', 'b', 'c', 'd'], [1, 3, 3, 7]))
            """
            )
        duckdb_col = duckdb_cursor.sql("select a from tmp AS '0'").df()
        print(duckdb_col.columns)
        print(converted_col.columns)
        pandas.testing.assert_frame_equal(converted_col, duckdb_col)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    @pytest.mark.parametrize('sample_size', [1, 10])
    @pytest.mark.parametrize('fill', [1000, 10000])
    @pytest.mark.parametrize('get_data', [create_repeated_nulls, create_trailing_non_null])
    def test_analyzing_nulls(self, pandas, duckdb_cursor, fill, sample_size, get_data):
        data = get_data(fill)
        df1 = pandas.DataFrame(data={"col1": data})
        duckdb_cursor.execute(f"SET GLOBAL pandas_analyze_sample={sample_size}")
        df = duckdb_cursor.execute("select * from df1").df()

        pandas.testing.assert_frame_equal(df1, df)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_nested_map(self, pandas, duckdb_cursor):
        df = pandas.DataFrame(data={'col1': [{'a': {'b': {'x': 'A', 'y': 'B'}}}, {'c': {'b': {'x': 'A'}}}]})

        rel = duckdb_cursor.sql("select * from df")
        expected_rel = duckdb_cursor.sql(
            """
            select x::map(varchar, struct(b map(varchar, varchar))) col1 from (VALUES
                ('{a={b: {x=A, y=B}}}'),
                ('{c={b: {x=A}}}')
            ) t(x)
        """
        )

        res = str(rel)
        expected_res = str(expected_rel)
        assert res == expected_res

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_map_value_upgrade(self, pandas, duckdb_cursor):
        x = pandas.DataFrame(
            [
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 'test']}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
            ]
        )
        x.rename(columns={0: 'a'}, inplace=True)
        converted_col = duckdb_cursor.sql("select * from x").df()
        duckdb_cursor.sql(
            """
            CREATE TABLE tmp2(
                a MAP(VARCHAR, VARCHAR)
            );
        """
        )
        duckdb_cursor.sql(
            """
            INSERT INTO tmp2 VALUES (MAP(['a', 'b', 'c', 'd'], ['1', '3', '3', 'test']))
        """
        )
        for _ in range(4):
            duckdb_cursor.sql(
                """
                INSERT INTO tmp2 VALUES (MAP(['a', 'b', 'c', 'd'], ['1', '3', '3', '7']))
            """
            )
        duckdb_col = duckdb_cursor.sql("select a from tmp2 AS '0'").df()
        print(duckdb_col.columns)
        print(converted_col.columns)
        pandas.testing.assert_frame_equal(converted_col, duckdb_col)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_map_duplicate(self, pandas, duckdb_cursor):
        x = pandas.DataFrame([[{'key': ['a', 'a', 'b'], 'value': [4, 0, 4]}]])
        with pytest.raises(
            duckdb.InvalidInputException, match="Dict->Map conversion failed because 'key' list contains duplicates"
        ):
            duckdb_cursor.sql("select * from x").show()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_map_nullkey(self, pandas, duckdb_cursor):
        x = pandas.DataFrame([[{'key': [None, 'a', 'b'], 'value': [4, 0, 4]}]])
        with pytest.raises(
            duckdb.InvalidInputException, match="Dict->Map conversion failed because 'key' list contains None"
        ):
            converted_col = duckdb_cursor.sql("select * from x").df()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_map_nullkeylist(self, pandas, duckdb_cursor):
        x = pandas.DataFrame([[{'key': None, 'value': None}]])
        converted_col = duckdb_cursor.sql("select * from x").df()
        duckdb_col = duckdb_cursor.sql("SELECT MAP(NULL, NULL) as '0'").df()
        pandas.testing.assert_frame_equal(duckdb_col, converted_col)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_map_fallback_nullkey(self, pandas, duckdb_cursor):
        x = pandas.DataFrame([[{'a': 4, None: 0, 'c': 4}], [{'a': 4, None: 0, 'd': 4}]])
        with pytest.raises(
            duckdb.InvalidInputException, match="Dict->Map conversion failed because 'key' list contains None"
        ):
            converted_col = duckdb_cursor.sql("select * from x").df()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_map_fallback_nullkey_coverage(self, pandas, duckdb_cursor):
        x = pandas.DataFrame(
            [
                [{'key': None, 'value': None}],
                [{'key': None, None: 5}],
            ]
        )
        with pytest.raises(
            duckdb.InvalidInputException, match="Dict->Map conversion failed because 'key' list contains None"
        ):
            converted_col = duckdb_cursor.sql("select * from x").df()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_structs_in_nested_types(self, pandas, duckdb_cursor):
        # This test is testing a bug that occurred when type upgrades occurred inside nested types
        # STRUCT(key1 varchar) + STRUCT(key1 varchar, key2 varchar) turns into MAP
        # But when inside a nested structure, this upgrade did not happen properly

        pairs = {
            'v1': ObjectPair({'key1': 21}, {'key1': 21, 'key2': 42}),
            'v2': ObjectPair({'key1': 21}, {'key2': 21}),
            'v3': ObjectPair({'key1': 21, 'key2': 42}, {'key1': 21}),
            'v4': ObjectPair({}, {'key1': 21}),
        }

        for _, pair in pairs.items():
            check_struct_upgrade('MAP(VARCHAR, INTEGER)[]', construct_list, pair, pandas, duckdb_cursor)

        for key, pair in pairs.items():
            if key == 'v4':
                expected_type = 'MAP(VARCHAR, MAP(VARCHAR, INTEGER))'
            else:
                expected_type = 'STRUCT(v1 MAP(VARCHAR, INTEGER))'
            check_struct_upgrade(expected_type, construct_struct, pair, pandas, duckdb_cursor)

        for key, pair in pairs.items():
            check_struct_upgrade('MAP(VARCHAR, MAP(VARCHAR, INTEGER))', construct_map, pair, pandas, duckdb_cursor)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_structs_of_different_sizes(self, pandas, duckdb_cursor):
        # This list has both a STRUCT(v1) and a STRUCT(v1, v2) member
        # Those can't be combined
        df = pandas.DataFrame(
            data={
                "col": [
                    [
                        {
                            "key": "value",
                        }
                    ],
                    [
                        {
                            "key": "value",
                            "key1": "value",
                        }
                    ],
                ]
            }
        )
        res = duckdb_cursor.query("select typeof(col) from df").fetchall()
        # So we fall back to converting them as VARCHAR instead
        assert res == [('MAP(VARCHAR, VARCHAR)[]',), ('MAP(VARCHAR, VARCHAR)[]',)]

        malformed_struct = duckdb.Value({"v1": 1, "v2": 2}, duckdb.struct_type({'v1': int}))
        with pytest.raises(
            duckdb.InvalidInputException,
            match=re.escape(
                "We could not convert the object {'v1': 1, 'v2': 2} to the desired target type (STRUCT(v1 BIGINT))"
            ),
        ):
            res = duckdb_cursor.execute("select $1", [malformed_struct])
            print(res)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_struct_key_conversion(self, pandas, duckdb_cursor):
        x = pandas.DataFrame(
            [
                [{IntString(5): 1, IntString(-25): 3, IntString(32): 3, IntString(32456): 7}],
            ]
        )
        duckdb_col = duckdb_cursor.sql("select {'5':1, '-25':3, '32':3, '32456':7} as '0'").df()
        converted_col = duckdb_cursor.sql("select * from x").df()
        duckdb_cursor.sql("drop view if exists tbl")
        pandas.testing.assert_frame_equal(duckdb_col, converted_col)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_list_correct(self, pandas, duckdb_cursor):
        x = pandas.DataFrame([{'0': [[5], [34], [-245]]}])
        duckdb_col = duckdb_cursor.sql("select [[5], [34], [-245]] as '0'").df()
        converted_col = duckdb_cursor.sql("select * from x").df()
        duckdb_cursor.sql("drop view if exists tbl")
        pandas.testing.assert_frame_equal(duckdb_col, converted_col)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_list_contains_null(self, pandas, duckdb_cursor):
        x = pandas.DataFrame([{'0': [[5], None, [-245]]}])
        duckdb_col = duckdb_cursor.sql("select [[5], NULL, [-245]] as '0'").df()
        converted_col = duckdb_cursor.sql("select * from x").df()
        duckdb_cursor.sql("drop view if exists tbl")
        pandas.testing.assert_frame_equal(duckdb_col, converted_col)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_list_starts_with_null(self, pandas, duckdb_cursor):
        x = pandas.DataFrame([{'0': [None, [5], [-245]]}])
        duckdb_col = duckdb_cursor.sql("select [NULL, [5], [-245]] as '0'").df()
        converted_col = duckdb_cursor.sql("select * from x").df()
        duckdb_cursor.sql("drop view if exists tbl")
        pandas.testing.assert_frame_equal(duckdb_col, converted_col)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_list_value_upgrade(self, pandas, duckdb_cursor):
        x = pandas.DataFrame([{'0': [['5'], [34], [-245]]}])
        duckdb_rel = duckdb_cursor.sql("select [['5'], ['34'], ['-245']] as '0'")
        duckdb_col = duckdb_rel.df()
        converted_col = duckdb_cursor.sql("select * from x").df()
        pandas.testing.assert_frame_equal(duckdb_col, converted_col)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_list_column_value_upgrade(self, pandas, duckdb_cursor):
        x = pandas.DataFrame(
            [
                [[1, 25, 300]],
                [[500, 345, 30]],
                [[50, 'a', 67]],
            ]
        )
        x.rename(columns={0: 'a'}, inplace=True)
        converted_col = duckdb_cursor.sql("select * from x").df()
        duckdb_cursor.sql(
            """
            CREATE TABLE tmp3(
                a VARCHAR[]
            );
        """
        )
        duckdb_cursor.sql(
            """
            INSERT INTO tmp3 VALUES (['1', '25', '300'])
        """
        )
        duckdb_cursor.sql(
            """
            INSERT INTO tmp3 VALUES (['500', '345', '30'])
        """
        )
        duckdb_cursor.sql(
            """
            INSERT INTO tmp3 VALUES (['50', 'a', '67'])
        """
        )
        duckdb_col = duckdb_cursor.sql("select a from tmp3 AS '0'").df()
        print(duckdb_col.columns)
        print(converted_col.columns)
        pandas.testing.assert_frame_equal(converted_col, duckdb_col)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_ubigint_object_conversion(self, pandas, duckdb_cursor):
        # UBIGINT + TINYINT would result in HUGEINT, but conversion to HUGEINT is not supported yet from pandas->duckdb
        # So this instead becomes a DOUBLE
        data = [18446744073709551615, 0]
        x = pandas.DataFrame({'0': pandas.Series(data=data, dtype='object')})
        converted_col = duckdb_cursor.sql("select * from x").df()
        if pandas.backend == 'numpy_nullable':
            float64 = np.dtype('float64')
            assert isinstance(converted_col['0'].dtype, float64.__class__) == True
        else:
            uint64 = np.dtype('uint64')
            assert isinstance(converted_col['0'].dtype, uint64.__class__) == True

    @pytest.mark.parametrize('pandas', [NumpyPandas()])
    def test_double_object_conversion(self, pandas, duckdb_cursor):
        data = [18446744073709551616, 0]
        x = pandas.DataFrame({'0': pandas.Series(data=data, dtype='object')})
        converted_col = duckdb_cursor.sql("select * from x").df()
        double_dtype = np.dtype('float64')
        assert isinstance(converted_col['0'].dtype, double_dtype.__class__) == True

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    @pytest.mark.xfail(
        condition=platform.system() == "Emscripten",
        reason="older numpy raises a warning when running with Pyodide",
    )
    def test_numpy_object_with_stride(self, pandas, duckdb_cursor):
        df = pandas.DataFrame(columns=["idx", "evens", "zeros"])

        df["idx"] = list(range(10))
        for col in df.columns[1:]:
            df[col].values[:] = 0

        counter = 0
        for i in range(10):
            df.loc[df["idx"] == i, "evens"] += counter
            counter += 2

        res = duckdb_cursor.sql("select * from df").fetchall()
        assert res == [
            (0, 0, 0),
            (1, 2, 0),
            (2, 4, 0),
            (3, 6, 0),
            (4, 8, 0),
            (5, 10, 0),
            (6, 12, 0),
            (7, 14, 0),
            (8, 16, 0),
            (9, 18, 0),
        ]

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_numpy_stringliterals(self, pandas, duckdb_cursor):
        df = pandas.DataFrame({"x": list(map(np.str_, range(3)))})

        res = duckdb_cursor.execute("select * from df").fetchall()
        assert res == [('0',), ('1',), ('2',)]

    @pytest.mark.parametrize('pandas', [NumpyPandas()])
    def test_integer_conversion_fail(self, pandas, duckdb_cursor):
        data = [2**10000, 0]
        x = pandas.DataFrame({'0': pandas.Series(data=data, dtype='object')})
        converted_col = duckdb_cursor.sql("select * from x").df()
        print(converted_col['0'])
        double_dtype = np.dtype('object')
        assert isinstance(converted_col['0'].dtype, double_dtype.__class__) == True

    # Most of the time numpy.datetime64 is just a wrapper around a datetime.datetime object
    # But to support arbitrary precision, it can fall back to using an `int` internally

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])  # Which we don't support yet
    def test_numpy_datetime(self, pandas, duckdb_cursor):
        numpy = pytest.importorskip("numpy")

        data = []
        data += [numpy.datetime64('2022-12-10T21:38:24.578696')] * standard_vector_size
        data += [numpy.datetime64('2022-02-21T06:59:23.324812')] * standard_vector_size
        data += [numpy.datetime64('1974-06-05T13:12:01.000000')] * standard_vector_size
        data += [numpy.datetime64('2049-01-13T00:24:31.999999')] * standard_vector_size
        x = pandas.DataFrame({'dates': pandas.Series(data=data, dtype='object')})
        res = duckdb_cursor.sql("select distinct * from x").df()
        assert len(res['dates'].__array__()) == 4

    @pytest.mark.parametrize('pandas', [NumpyPandas()])
    def test_numpy_datetime_int_internally(self, pandas, duckdb_cursor):
        numpy = pytest.importorskip("numpy")

        data = [numpy.datetime64('2022-12-10T21:38:24.0000000000001')]
        x = pandas.DataFrame({'dates': pandas.Series(data=data, dtype='object')})
        with pytest.raises(
            duckdb.ConversionException,
            match=re.escape("Conversion Error: Unimplemented type for cast (BIGINT -> TIMESTAMP)"),
        ):
            rel = duckdb.query_df(x, "x", "create table dates as select dates::TIMESTAMP WITHOUT TIME ZONE from x")

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_fallthrough_object_conversion(self, pandas, duckdb_cursor):
        x = pandas.DataFrame(
            [
                [IntString(4)],
                [IntString(2)],
                [IntString(0)],
            ]
        )
        duckdb_col = duckdb_cursor.sql("select * from x").df()
        df_expected_res = pandas.DataFrame({'0': pandas.Series(['4', '2', '0'])})
        pandas.testing.assert_frame_equal(duckdb_col, df_expected_res)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_numeric_decimal(self, pandas, duckdb_cursor):
        # DuckDB uses DECIMAL where possible, so all the 'float' types here are actually DECIMAL
        reference_query = """
            CREATE TABLE tbl AS SELECT * FROM (
                VALUES
                (5,                   5002340,                  -234234234234.0),
                (12.0,                13,                       324234234.00000005),
                (-123.0,              -12.0000000005,           -128),
                (-234234.0,           7453324234.0,             345345),
                (NULL,                NULL,                     1.00000),
                (1.234,               -324234234,               1324234359)
            ) tbl(a, b, c);
        """
        duckdb_cursor.execute(reference_query)
        # Because of this we need to wrap these native floats as DECIMAL for this test, to avoid these decimals being "upgraded" to DOUBLE
        x = pandas.DataFrame(
            {
                '0': ConvertStringToDecimal([5, '12.0', '-123.0', '-234234.0', None, '1.234'], pandas),
                '1': ConvertStringToDecimal(
                    [5002340, 13, '-12.0000000005', '7453324234.0', None, '-324234234'], pandas
                ),
                '2': ConvertStringToDecimal(
                    ['-234234234234.0', '324234234.00000005', -128, 345345, '1E5', '1324234359'], pandas
                ),
            }
        )
        reference = duckdb_cursor.sql("select * from tbl").fetchall()
        conversion = duckdb_cursor.sql("select * from x").fetchall()

        assert conversion == reference

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_numeric_decimal_coverage(self, pandas, duckdb_cursor):
        x = pandas.DataFrame(
            {'0': [Decimal("nan"), Decimal("+nan"), Decimal("-nan"), Decimal("inf"), Decimal("+inf"), Decimal("-inf")]}
        )
        conversion = duckdb_cursor.sql("select * from x").fetchall()
        print(conversion[0][0].__class__)
        for item in conversion:
            assert isinstance(item[0], float)
        assert math.isnan(conversion[0][0])
        assert math.isnan(conversion[1][0])
        assert math.isnan(conversion[2][0])
        assert math.isinf(conversion[3][0])
        assert math.isinf(conversion[4][0])
        assert math.isinf(conversion[5][0])
        assert str(conversion) == '[(nan,), (nan,), (nan,), (inf,), (inf,), (inf,)]'

    # Test that the column 'offset' is actually used when converting,

    @pytest.mark.parametrize(
        'pandas', [NumpyPandas(), ArrowPandas()]
    )  # and that the same 2048 (STANDARD_VECTOR_SIZE) values are not being scanned over and over again
    def test_multiple_chunks(self, pandas, duckdb_cursor):
        data = []
        data += [datetime.date(2022, 9, 13) for x in range(standard_vector_size)]
        data += [datetime.date(2022, 9, 14) for x in range(standard_vector_size)]
        data += [datetime.date(2022, 9, 15) for x in range(standard_vector_size)]
        data += [datetime.date(2022, 9, 16) for x in range(standard_vector_size)]
        x = pandas.DataFrame({'dates': pandas.Series(data=data, dtype='object')})
        res = duckdb_cursor.sql("select distinct * from x").df()
        assert len(res['dates'].__array__()) == 4

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_multiple_chunks_aggregate(self, pandas, duckdb_cursor):
        duckdb_cursor.execute(f"SET GLOBAL pandas_analyze_sample=4096")
        duckdb_cursor.execute(
            "create table dates as select '2022-09-14'::DATE + INTERVAL (i::INTEGER) DAY as i from range(4096) tbl(i);"
        )
        rel = duckdb_cursor.sql("select * from dates")
        res = rel.df()
        date_df = res.copy()

        # Convert the dataframe to datetime
        date_df['i'] = pandas.to_datetime(res['i']).dt.date
        assert str(date_df['i'].dtype) == 'object'

        expected_res = [
            (
                1840017600.0,  # Saturday, April 22, 2028 12:00:00 PM (avg)
                1663113600.0,  # Wednesday, September 14, 2022 12:00:00 AM (min)
                2016921600.0,  # Wednesday, November 30, 2033 12:00:00 AM (max)
            )
        ]

        rel = duckdb_cursor.sql(
            """
            select
                avg(epoch(i)),
                min(epoch(i)),
                max(epoch(i))
            from date_df
        """
        )
        actual_res = rel.fetchall()
        assert expected_res == actual_res

        # Now interleave nulls into the dataframe
        duckdb_cursor.execute('drop table dates')
        for i in range(0, len(res['i']), 2):
            res.loc[i, 'i'] = None
        duckdb_cursor.execute('create table dates as select * from res')

        expected_res = [
            (
                1840060800.0,  # Sunday, April 23, 2028 12:00:00 AM
                1663200000.0,  # Thursday, September 15, 2022 12:00:00 AM
                2016921600.0,  # Wednesday, November 30, 2033 12:00:00 AM
            )
        ]
        # Convert the dataframe to datetime
        date_df = res.copy()
        date_df['i'] = pandas.to_datetime(res['i']).dt.date
        assert str(date_df['i'].dtype) == 'object'

        actual_res = duckdb_cursor.sql(
            """
            select
                avg(epoch(i)),
                min(epoch(i)),
                max(epoch(i))
            from date_df
        """
        ).fetchall()

        assert expected_res == actual_res

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_mixed_object_types(self, pandas, duckdb_cursor):
        x = pandas.DataFrame(
            {
                'nested': pandas.Series(
                    data=[{'a': 1, 'b': 2}, [5, 4, 3], {'key': [1, 2, 3], 'value': ['a', 'b', 'c']}], dtype='object'
                ),
            }
        )
        res = duckdb_cursor.sql("select * from x").df()
        assert res['nested'].dtype == np.dtype('object')

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_struct_deeply_nested_in_struct(self, pandas, duckdb_cursor):
        x = pandas.DataFrame(
            [
                {
                    # STRUCT(b STRUCT(x VARCHAR, y VARCHAR))
                    'a': {'b': {'x': 'A', 'y': 'B'}}
                },
                {
                    # STRUCT(b STRUCT(x VARCHAR))
                    'a': {'b': {'x': 'A'}}
                },
            ]
        )
        # The dataframe has incompatible struct schemas in the nested child
        # This gets upgraded to STRUCT(b MAP(VARCHAR, VARCHAR))
        res = duckdb_cursor.sql("select * from x").fetchall()
        assert res == [({'b': {'key': ['x', 'y'], 'value': ['A', 'B']}},), ({'b': {'key': ['x'], 'value': ['A']}},)]

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_struct_deeply_nested_in_list(self, pandas, duckdb_cursor):
        x = pandas.DataFrame(
            {
                'a': [
                    [
                        # STRUCT(x VARCHAR, y VARCHAR)[]
                        {'x': 'A', 'y': 'B'},
                        # STRUCT(x VARCHAR)[]
                        {'x': 'A'},
                    ]
                ]
            }
        )
        # The dataframe has incompatible struct schemas in the nested child
        # This gets upgraded to STRUCT(b MAP(VARCHAR, VARCHAR))
        res = duckdb_cursor.sql("select * from x").fetchall()
        assert res == [([{'key': ['x', 'y'], 'value': ['A', 'B']}, {'key': ['x'], 'value': ['A']}],)]

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_analyze_sample_too_small(self, pandas, duckdb_cursor):
        data = [1 for _ in range(9)] + [[1, 2, 3]] + [1 for _ in range(9991)]
        x = pandas.DataFrame({'a': pandas.Series(data=data)})
        with pytest.raises(duckdb.InvalidInputException, match="Failed to cast value: Unimplemented type for cast"):
            res = duckdb_cursor.sql("select * from x").df()

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_numeric_decimal_zero_fractional(self, pandas, duckdb_cursor):
        decimals = pandas.DataFrame(
            data={
                "0": [
                    Decimal("0.00"),
                    Decimal("125.90"),
                    Decimal("0.001"),
                    Decimal("2502.63"),
                    Decimal("0.000123"),
                    Decimal("0.00"),
                    Decimal("321.00"),
                ]
            }
        )
        reference_query = """
            CREATE TABLE tbl AS SELECT * FROM (
                VALUES
                (0.00),
                (125.90),
                (0.001),
                (2502.63),
                (0.000123),
                (0.00),
                (321.00)
            ) tbl(a);
        """
        duckdb_cursor.execute(reference_query)
        reference = duckdb_cursor.sql("select * from tbl").fetchall()
        conversion = duckdb.query_df(decimals, "x", "select * from x").fetchall()

        assert conversion == reference

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_numeric_decimal_incompatible(self, pandas, duckdb_cursor):
        reference_query = """
            CREATE TABLE tbl AS SELECT * FROM (
                VALUES
                (5,                   5002340,                  -234234234234),
                (12.0,                13,                       324234234.00000005),
                (-123.0,              -12.0000000005,           -128),
                (-234234.0,           7453324234,               345345),
                (NULL,                NULL,                     0),
                (1.234,               -324234234,               1324234359)
            ) tbl(a, b, c);
        """
        duckdb_cursor.execute(reference_query)
        x = pandas.DataFrame(
            {
                '0': ConvertStringToDecimal(['5', '12.0', '-123.0', '-234234.0', None, '1.234'], pandas),
                '1': ConvertStringToDecimal([5002340, 13, '-12.0000000005', 7453324234, None, '-324234234'], pandas),
                '2': ConvertStringToDecimal(
                    [-234234234234, '324234234.00000005', -128, 345345, 0, '1324234359'], pandas
                ),
            }
        )
        reference = duckdb_cursor.sql("select * from tbl").fetchall()
        conversion = duckdb_cursor.sql("select * from x").fetchall()

        assert conversion == reference
        print(reference)
        print(conversion)

    @pytest.mark.parametrize(
        'pandas', [NumpyPandas(), ArrowPandas()]
    )  # result: [('1E-28',), ('10000000000000000000000000.0',)]
    def test_numeric_decimal_combined(self, pandas, duckdb_cursor):
        decimals = pandas.DataFrame(
            data={"0": [Decimal("0.0000000000000000000000000001"), Decimal("10000000000000000000000000.0")]}
        )
        reference_query = """
            CREATE TABLE tbl AS SELECT * FROM (
                VALUES
                (0.0000000000000000000000000001),
                (10000000000000000000000000.0),
            ) tbl(a);
        """
        duckdb_cursor.execute(reference_query)
        reference = duckdb_cursor.sql("select * from tbl").fetchall()
        conversion = duckdb.query_df(decimals, "x", "select * from x").fetchall()
        assert conversion == reference
        print(reference)
        print(conversion)

    # result: [('1234.0',), ('123456789.0',), ('1234567890123456789.0',), ('0.1234567890123456789',)]
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_numeric_decimal_varying_sizes(self, pandas, duckdb_cursor):
        decimals = pandas.DataFrame(
            data={
                "0": [
                    Decimal("1234.0"),
                    Decimal("123456789.0"),
                    Decimal("1234567890123456789.0"),
                    Decimal("0.1234567890123456789"),
                ]
            }
        )
        reference_query = """
            CREATE TABLE tbl AS SELECT * FROM (
                VALUES
                    (1234.0),
                    (123456789.0),
                    (1234567890123456789.0),
                    (0.1234567890123456789)
            ) tbl(a);
        """
        duckdb_cursor.execute(reference_query)
        reference = duckdb_cursor.sql("select * from tbl").fetchall()
        conversion = duckdb.query_df(decimals, "x", "select * from x").fetchall()
        assert conversion == reference
        print(reference)
        print(conversion)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_numeric_decimal_fallback_to_double(self, pandas, duckdb_cursor):
        # The widths of these decimal values are bigger than the max supported width for DECIMAL
        data = [
            Decimal("1.234567890123456789012345678901234567890123456789"),
            Decimal("123456789012345678901234567890123456789012345678.0"),
        ]
        decimals = pandas.DataFrame(data={"0": data})
        reference_query = """
            CREATE TABLE tbl AS SELECT * FROM (
                VALUES
                    (1.234567890123456789012345678901234567890123456789),
                    (123456789012345678901234567890123456789012345678.0)
            ) tbl(a);
        """
        duckdb_cursor.execute(reference_query)
        reference = duckdb_cursor.sql("select * from tbl").fetchall()
        conversion = duckdb.query_df(decimals, "x", "select * from x").fetchall()
        assert conversion == reference
        assert isinstance(conversion[0][0], float)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_numeric_decimal_double_mixed(self, pandas, duckdb_cursor):
        data = [
            Decimal("1.234"),
            Decimal("1.234567891234567890123456789012345678901234567890123456789"),
            Decimal("0.00000000000345"),
            Decimal("0.00000000000000000000000000000000000000000000000000000000000123456789"),
            Decimal("1234543534535213412342342.2345456"),
            Decimal("123456789123456789123456789123456789123456789123456789123456789123456789"),
            Decimal("1232354.000000000000000000000000000035"),
            Decimal("123.5e300"),
        ]
        decimals = pandas.DataFrame(data={"0": data})
        reference_query = """
            CREATE TABLE tbl AS SELECT * FROM (
                VALUES
                    (1.234),
                    (1.234567891234567890123456789012345678901234567890123456789),
                    (0.00000000000345),
                    (0.00000000000000000000000000000000000000000000000000000000000123456789),
                    (1234543534535213412342342.2345456),
                    (123456789123456789123456789123456789123456789123456789123456789123456789),
                    (1232354.000000000000000000000000000035),
                    (123.5e300)
            ) tbl(a);
        """
        duckdb_cursor.execute(reference_query)
        reference = duckdb_cursor.sql("select * from tbl").fetchall()
        conversion = duckdb_cursor.sql("select * from decimals").fetchall()
        assert conversion == reference
        assert isinstance(conversion[0][0], float)

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_numeric_decimal_out_of_range(self, pandas, duckdb_cursor):
        data = [Decimal("1.234567890123456789012345678901234567"), Decimal("123456789012345678901234567890123456.0")]
        decimals = pandas.DataFrame(data={"0": data})
        reference_query = """
            CREATE TABLE tbl AS SELECT * FROM (
                VALUES
                    (1.234567890123456789012345678901234567),
                    (123456789012345678901234567890123456.0)
            ) tbl(a);
        """
        duckdb_cursor.execute(reference_query)
        reference = duckdb_cursor.sql("select * from tbl").fetchall()
        conversion = duckdb_cursor.sql("select * from decimals").fetchall()
        assert conversion == reference
