import pandas as pd
import duckdb
import datetime
import numpy as np
import pytest
import decimal

def create_generic_dataframe(data):
    return pd.DataFrame({'0': pd.Series(data=data, dtype='object')})

class IntString:
    def __init__(self, value: int):
        self.value = value
    def __str__(self):
        return str(self.value)

# To avoid DECIMAL being upgraded to DOUBLE (because DOUBLE outranks DECIMAL as a LogicalType)
# These floats had their precision preserved as string and are now cast to decimal.Decimal
def ConvertStringToDecimal(data: list):
    for i in range(len(data)):
        if isinstance(data[i], str):
            data[i] = decimal.Decimal(data[i])
    data = pd.Series(data=data, dtype='object')
    return data

class TestResolveObjectColumns(object):

    def test_integers(self, duckdb_cursor):
        data = [5, 0, 3]
        df_in = create_generic_dataframe(data)
        # These are float64 because pandas would force these to be float64 even if we set them to int8, int16, int32, int64 respectively
        df_expected_res = pd.DataFrame({'0': pd.Series(data=data, dtype='int8')})
        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        print(df_out)
        pd.testing.assert_frame_equal(df_expected_res, df_out)

    def test_struct_correct(self, duckdb_cursor):
        data = [{'a': 1, 'b': 3, 'c': 3, 'd': 7}]
        df = pd.DataFrame({'0': pd.Series(data=data)})
        duckdb_col = duckdb.query("SELECT {a: 1, b: 3, c: 3, d: 7} as '0'").df()
        converted_col = duckdb.query_df(df, "data", "SELECT * FROM data").df()
        pd.testing.assert_frame_equal(duckdb_col, converted_col)

    def test_map_fallback_different_keys(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'e': 7}], #'e' instead of 'd' as key
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}]
            ]
        )

        converted_df = duckdb.query_df(x, "x", "SELECT * FROM x").df()
        y = pd.DataFrame(
            [
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'e'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
            ]
        )
        equal_df = duckdb.query_df(y, "y", "SELECT * FROM y").df()
        pd.testing.assert_frame_equal(converted_df, equal_df)

    def test_map_fallback_incorrect_amount_of_keys(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3}],         #incorrect amount of keys
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}]
            ]
        )
        converted_df = duckdb.query_df(x, "x", "SELECT * FROM x").df()
        y = pd.DataFrame(
            [
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c'], 'value': [1, 3, 3]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
            ]
        )
        equal_df = duckdb.query_df(y, "y", "SELECT * FROM y").df()
        pd.testing.assert_frame_equal(converted_df, equal_df)

    def test_struct_value_upgrade(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [{'a': 1, 'b': 3, 'c': 3, 'd': 'string'}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}]
            ]
        )
        y = pd.DataFrame(
            [
                [{'a': 1, 'b': 3, 'c': 3, 'd': 'string'}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': '7'}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': '7'}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': '7'}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': '7'}]
            ]
        )
        converted_df = duckdb.query_df(x, "x", "SELECT * FROM x").df()
        equal_df = duckdb.query_df(y, "y", "SELECT * FROM y").df()
        pd.testing.assert_frame_equal(converted_df, equal_df)

    def test_struct_null(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [None],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}]
            ]
        )
        y = pd.DataFrame(
            [
                [None],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}]
            ]
        )
        converted_df = duckdb.query_df(x, "x", "SELECT * FROM x").df()
        equal_df = duckdb.query_df(y, "y", "SELECT * FROM y").df()
        pd.testing.assert_frame_equal(converted_df, equal_df)

    def test_map_fallback_value_upgrade(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [{'a': 1, 'b': 3, 'c': 3, 'd': 'test'}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}],
                [{'a': 1, 'b': 3, 'c': 3, 'd': 7}]
            ]
        )
        y = pd.DataFrame(
            [
                [{'a': '1', 'b': '3', 'c': '3', 'd': 'test'}],
                [{'a': '1', 'b': '3', 'c': '3', 'd': '7'}],
                [{'a': '1', 'b': '3', 'c': '3'}],
                [{'a': '1', 'b': '3', 'c': '3', 'd': '7'}],
                [{'a': '1', 'b': '3', 'c': '3', 'd': '7'}]
            ]
        )
        converted_df = duckdb.query_df(x, "df", "SELECT * FROM df").df()
        equal_df = duckdb.query_df(y, "df", "SELECT * FROM df").df()
        pd.testing.assert_frame_equal(converted_df, equal_df)

    def test_map_correct(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}]
            ]
        )
        x.rename(columns = {0 : 'a'}, inplace = True)
        converted_col = duckdb.query_df(x, "x", "select * from x as 'a'").df()
        duckdb.query("""
            CREATE TABLE tmp(
                a MAP(VARCHAR, INTEGER)
            );
        """)
        for _ in range(5):
            duckdb.query("""
                INSERT INTO tmp VALUES (MAP(['a', 'b', 'c', 'd'], [1, 3, 3, 7]))
            """)
        duckdb_col = duckdb.query("select a from tmp AS '0'").df()
        print(duckdb_col.columns)
        print(converted_col.columns)
        pd.testing.assert_frame_equal(converted_col, duckdb_col)

    def test_map_value_upgrade(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 'test']}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}],
                [{'key': ['a', 'b', 'c', 'd'], 'value': [1, 3, 3, 7]}]
            ]
        )
        x.rename(columns = {0 : 'a'}, inplace = True)
        converted_col = duckdb.query_df(x, "x", "select * from x").df()
        duckdb.query("""
            CREATE TABLE tmp2(
                a MAP(VARCHAR, VARCHAR)
            );
        """)
        duckdb.query("""
            INSERT INTO tmp2 VALUES (MAP(['a', 'b', 'c', 'd'], ['1', '3', '3', 'test']))
        """)
        for _ in range(4):
            duckdb.query("""
                INSERT INTO tmp2 VALUES (MAP(['a', 'b', 'c', 'd'], ['1', '3', '3', '7']))
            """)
        duckdb_col = duckdb.query("select a from tmp2 AS '0'").df()
        print(duckdb_col.columns)
        print(converted_col.columns)
        pd.testing.assert_frame_equal(converted_col, duckdb_col)

    def test_map_duplicate(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [{'key': ['a', 'a', 'b'], 'value': [4, 0, 4]}]
            ]
        )
        with pytest.raises(Exception, match="Dict->Map conversion failed because 'key' list contains duplicates"):
            converted_col = duckdb.query_df(x, "x", "select * from x").df()

    def test_map_nullkey(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [{'key': [None, 'a', 'b'], 'value': [4, 0, 4]}]
            ]
        )
        with pytest.raises(Exception, match="Dict->Map conversion failed because 'key' list contains None"):
            converted_col = duckdb.query_df(x, "x", "select * from x").df()

    def test_map_nullkeylist(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [{'key': None, 'value': None}]
            ]
        )
        # Isn't actually converted to MAP because isinstance(None, list) != True
        converted_col = duckdb.query_df(x, "x", "select * from x").df()
        duckdb_col = duckdb.query("SELECT {key: NULL, value: NULL} as '0'").df()
        pd.testing.assert_frame_equal(duckdb_col, converted_col)

    def test_map_fallback_nullkey(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [{'a': 4, None: 0, 'c': 4}],
                [{'a': 4, None: 0, 'd': 4}]
            ]
        )
        with pytest.raises(Exception, match="Dict->Map conversion failed because 'key' list contains None"):
            converted_col = duckdb.query_df(x, "x", "select * from x").df()

    def test_map_fallback_nullkey_coverage(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [{'key': None, 'value': None}],
                [{'key': None, None: 5}],
            ]
        )
        with pytest.raises(Exception, match="Dict->Map conversion failed because 'key' list contains None"):
            converted_col = duckdb.query_df(x, "x", "select * from x").df()

    def test_struct_key_conversion(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [{
                    IntString(5) :      1,
                    IntString(-25):     3,
                    IntString(32):      3,
                    IntString(32456):   7
                }],
            ]
        )
        duckdb_col = duckdb.query("select {'5':1, '-25':3, '32':3, '32456':7} as '0'").df()
        converted_col = duckdb.query_df(x, "tbl", "select * from tbl").df()
        pd.testing.assert_frame_equal(duckdb_col, converted_col)

    def test_list_correct(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                {'0': [[5], [34], [-245]]}
            ]
        )
        duckdb_col = duckdb.query("select [[5], [34], [-245]] as '0'").df()
        converted_col = duckdb.query_df(x, "tbl", "select * from tbl").df()
        pd.testing.assert_frame_equal(duckdb_col, converted_col)

    def test_list_contains_null(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                {'0': [[5], None, [-245]]}
            ]
        )
        duckdb_col = duckdb.query("select [[5], NULL, [-245]] as '0'").df()
        converted_col = duckdb.query_df(x, "tbl", "select * from tbl").df()
        pd.testing.assert_frame_equal(duckdb_col, converted_col)

    def test_list_starts_with_null(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                {'0': [None, [5], [-245]]}
            ]
        )
        duckdb_col = duckdb.query("select [NULL, [5], [-245]] as '0'").df()
        converted_col = duckdb.query_df(x, "tbl", "select * from tbl").df()
        pd.testing.assert_frame_equal(duckdb_col, converted_col)

    def test_list_value_upgrade(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                {'0': [['5'], [34], [-245]]}
            ]
        )
        duckdb_col = duckdb.query("select [['5'], ['34'], ['-245']] as '0'").df()
        converted_col = duckdb.query_df(x, "tbl", "select * from tbl").df()
        pd.testing.assert_frame_equal(duckdb_col, converted_col)

    def test_list_column_value_upgrade(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [ [1, 25, 300] ],
                [ [500, 345, 30] ],
                [ [50, 'a', 67] ],
            ]
        )
        x.rename(columns = {0 : 'a'}, inplace = True)
        converted_col = duckdb.query_df(x, "x", "select * from x").df()
        duckdb.query("""
            CREATE TABLE tmp3(
                a VARCHAR[]
            );
        """)
        duckdb.query("""
            INSERT INTO tmp3 VALUES (['1', '25', '300'])
        """)
        duckdb.query("""
            INSERT INTO tmp3 VALUES (['500', '345', '30'])
        """)
        duckdb.query("""
            INSERT INTO tmp3 VALUES (['50', 'a', '67'])
        """)
        duckdb_col = duckdb.query("select a from tmp3 AS '0'").df()
        print(duckdb_col.columns)
        print(converted_col.columns)
        pd.testing.assert_frame_equal(converted_col, duckdb_col)

    def test_ubigint_object_conversion(self, duckdb_cursor):
        data = [18446744073709551615, 0]
        x = pd.DataFrame({'0': pd.Series(data=data, dtype='object')})
        converted_col = duckdb.query_df(x, "x", "select * from x").df()
        uint64_dtype = np.dtype('uint64')
        assert isinstance(converted_col['0'].dtype, uint64_dtype.__class__) == True

    def test_double_object_conversion(self, duckdb_cursor):
        data = [18446744073709551616, 0]
        x = pd.DataFrame({'0': pd.Series(data=data, dtype='object')})
        converted_col = duckdb.query_df(x, "x", "select * from x").df()
        double_dtype = np.dtype('float64')
        assert isinstance(converted_col['0'].dtype, double_dtype.__class__) == True

    def test_integer_conversion_fail(self, duckdb_cursor):
        data = [2**10000, 0]
        x = pd.DataFrame({'0': pd.Series(data=data, dtype='object')})
        converted_col = duckdb.query_df(x, "x", "select * from x").df()
        print(converted_col['0'])
        double_dtype = np.dtype('object')
        assert isinstance(converted_col['0'].dtype, double_dtype.__class__) == True

    def test_fallthrough_object_conversion(self, duckdb_cursor):
        x = pd.DataFrame(
            [
                [IntString(4)],
                [IntString(2)],
                [IntString(0)],
            ]
        )
        duckdb_col = duckdb.query_df(x, "x", "select * from x").df()
        df_expected_res = pd.DataFrame({'0': pd.Series(['4','2','0'])})
        pd.testing.assert_frame_equal(duckdb_col, df_expected_res)

    def test_numeric_decimal(self):
        duckdb_conn = duckdb.connect()

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
        duckdb_conn.execute(reference_query)
        # Because of this we need to wrap these native floats as DECIMAL for this test, to avoid these decimals being "upgraded" to DOUBLE
        x = pd.DataFrame({
            '0': ConvertStringToDecimal([5, '12.0', '-123.0', '-234234.0', None, '1.234']),
            '1': ConvertStringToDecimal([5002340, 13, '-12.0000000005', '7453324234.0', None, '-324234234']),
            '2': ConvertStringToDecimal(['-234234234234.0',  '324234234.00000005', -128, 345345, '1E5', '1324234359'])
        })
        reference = duckdb.query("select * from tbl", connection=duckdb_conn).fetchall()
        conversion = duckdb.query_df(x, "x", "select * from x").fetchall()

        assert(conversion == reference)

    def test_mixed_object_types(self):
        x = pd.DataFrame({
            'nested': pd.Series(data=[{'a': 1, 'b': 2}, [5, 4, 3], {'key': [1,2,3], 'value': ['a', 'b', 'c']}], dtype='object'),
        })
        res = duckdb.query_df(x, "x", "select * from x").df()
        assert(res['nested'].dtype == np.dtype('object'))


    def test_analyze_sample_too_small(self):
        data = [1 for _ in range(9)] + [[1,2,3]] + [1 for _ in range(9991)]
        x = pd.DataFrame({
            'a': pd.Series(data=data)
        })
        with pytest.raises(Exception, match="Unimplemented type for cast"):
            res = duckdb.query_df(x, "x", "select * from x").df()

    def test_numeric_decimal_incompatible(self):
        duckdb_conn = duckdb.connect()
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
        # Currently this raises a ConversionException due to an unrelated bug, so we can't accurately test faulty conversions
        # This test serves as a reminder that this functionality needs to be properly tested once this bug is fixed.
        # See PR #3985
        with pytest.raises(Exception, match="Conversion Error"):
            duckdb_conn.execute(reference_query)
        #x = pd.DataFrame({
        #    '0': ConvertStringToDecimal(['5', '12.0', '-123.0', '-234234.0', None, '1.234']),
        #    '1': ConvertStringToDecimal([5002340, 13, '-12.0000000005', 7453324234, None, '-324234234']),
        #    '2': ConvertStringToDecimal([-234234234234,  '324234234.00000005', -128, 345345, 0, '1324234359'])
        #})
        #conversion = duckdb.query_df(x, "x", "select * from x").fetchall()
