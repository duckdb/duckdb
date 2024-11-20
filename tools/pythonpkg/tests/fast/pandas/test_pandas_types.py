import duckdb
import pytest
import pandas as pd
import numpy
import string
from packaging import version
import warnings
from contextlib import suppress


def round_trip(data, pandas_type):
    df_in = pd.DataFrame(
        {
            'object': pd.Series(data, dtype=pandas_type),
        }
    )

    df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
    print(df_out)
    print(df_in)
    assert df_out.equals(df_in)


class TestNumpyNullableTypes(object):
    def test_pandas_numeric(self):
        base_df = pd.DataFrame({'a': range(10)})

        data_types = [
            "uint8",
            "UInt8",
            "uint16",
            "UInt16",
            "uint32",
            "UInt32",
            "uint64",
            "UInt64",
            "int8",
            "Int8",
            "int16",
            "Int16",
            "int32",
            "Int32",
            "int64",
            "Int64",
            "float32",
            "float64",
        ]

        if version.parse(pd.__version__) >= version.parse('1.2.0'):
            # These DTypes where added in 1.2.0
            data_types.extend(["Float32", "Float64"])
        # Generate a dataframe with all the types, in the form of:
        # b=type1,
        # c=type2
        # ..
        data = {}
        for letter, dtype in zip(string.ascii_lowercase, data_types):
            data[letter] = base_df.a.astype(dtype)

        df = pd.DataFrame.from_dict(data)
        conn = duckdb.connect()
        out_df = conn.execute('select * from df').df()

        # Verify that the types in the out_df are correct
        # FIXME: we don't support outputting pandas specific types (i.e UInt64)
        for letter, item in zip(string.ascii_lowercase, data_types):
            column_name = letter
            assert str(out_df[column_name].dtype) == item.lower()

    def test_pandas_unsigned(self, duckdb_cursor):
        unsigned_types = ['uint8', 'uint16', 'uint32', 'uint64']
        data = numpy.array([0, 1, 2, 3])
        for u_type in unsigned_types:
            round_trip(data, u_type)

    def test_pandas_bool(self, duckdb_cursor):
        data = numpy.array([True, False, False, True])
        round_trip(data, 'bool')

    def test_pandas_masked_float64(self, duckdb_cursor, tmp_path):
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        # Create a sample DataFrame
        testdf = pd.DataFrame({"value": [26.0, 26.0, 26.0, pd.NA, 27.0, pd.NA, pd.NA, pd.NA, pd.NA, 29.0]})

        # Set the correct dtype for the 'value' column
        testdf["value"] = testdf["value"].astype(pd.Float64Dtype())

        # Write the DataFrame to a Parquet file using tmp_path fixture
        parquet_path = tmp_path / "testdf.parquet"
        pq.write_table(pa.Table.from_pandas(testdf), parquet_path)

        # Read the Parquet file back into a DataFrame
        testdf2 = pd.read_parquet(parquet_path)

        # Use duckdb_cursor to query the parquet data
        result = duckdb_cursor.execute("SELECT MIN(value) FROM testdf2").fetchall()
        assert result[0][0] == 26

    def test_pandas_boolean(self, duckdb_cursor):
        data = numpy.array([True, None, pd.NA, numpy.nan, True])
        df_in = pd.DataFrame(
            {
                'object': pd.Series(data, dtype='boolean'),
            }
        )

        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()
        assert df_out['object'][0] == df_in['object'][0]
        assert pd.isna(df_out['object'][1])
        assert pd.isna(df_out['object'][2])
        assert pd.isna(df_out['object'][3])
        assert df_out['object'][4] == df_in['object'][4]

    def test_pandas_float32(self, duckdb_cursor):
        data = numpy.array([0.1, 0.32, 0.78, numpy.nan])
        df_in = pd.DataFrame(
            {
                'object': pd.Series(data, dtype='float32'),
            }
        )

        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()

        assert df_out['object'][0] == df_in['object'][0]
        assert df_out['object'][1] == df_in['object'][1]
        assert df_out['object'][2] == df_in['object'][2]
        assert pd.isna(df_out['object'][3])

    def test_pandas_float64(self):
        data = numpy.array([0.233, numpy.nan, 3456.2341231, float('-inf'), -23424.45345, float('+inf'), 0.0000000001])
        df_in = pd.DataFrame(
            {
                'object': pd.Series(data, dtype='float64'),
            }
        )
        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()

        for i in range(len(data)):
            if pd.isna(df_out['object'][i]):
                assert i == 1
                continue
            assert df_out['object'][i] == df_in['object'][i]

    def test_pandas_interval(self, duckdb_cursor):
        if pd.__version__ != '1.2.4':
            return

        data = numpy.array([2069211000000000, numpy.datetime64("NaT")])
        df_in = pd.DataFrame(
            {
                'object': pd.Series(data, dtype='timedelta64[ns]'),
            }
        )

        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df()

        assert df_out['object'][0] == df_in['object'][0]
        assert pd.isnull(df_out['object'][1])

    def test_pandas_encoded_utf8(self, duckdb_cursor):
        data = u'\u00c3'  # Unicode data
        data = [data.encode('utf8')]
        expected_result = data[0]
        df_in = pd.DataFrame({'object': pd.Series(data, dtype='object')})
        result = duckdb.query_df(df_in, "data", "SELECT * FROM data").fetchone()[0]
        assert result == expected_result

    @pytest.mark.parametrize(
        'dtype',
        [
            'bool',
            'utinyint',
            'usmallint',
            'uinteger',
            'ubigint',
            'tinyint',
            'smallint',
            'integer',
            'bigint',
            'float',
            'double',
        ],
    )
    def test_producing_nullable_dtypes(self, duckdb_cursor, dtype):
        class Input:
            def __init__(self, value, expected_dtype):
                self.value = value
                self.expected_dtype = expected_dtype

        inputs = {
            'bool': Input('true', 'BooleanDtype'),
            'utinyint': Input('255', 'UInt8Dtype'),
            'usmallint': Input('65535', 'UInt16Dtype'),
            'uinteger': Input('4294967295', 'UInt32Dtype'),
            'ubigint': Input('18446744073709551615', 'UInt64Dtype'),
            'tinyint': Input('-128', 'Int8Dtype'),
            'smallint': Input('-32768', 'Int16Dtype'),
            'integer': Input('-2147483648', 'Int32Dtype'),
            'bigint': Input('-9223372036854775808', 'Int64Dtype'),
            'float': Input('268043421344044473239570760152672894976.0000000000', 'float32'),
            'double': Input(
                '14303088389124869511075243108389716684037132417196499782261853698893384831666205572097390431189931733040903060865714975797777061496396865611606109149583360363636503436181348332896211726552694379264498632046075093077887837955077425420408952536212326792778411457460885268567735875437456412217418386401944141824.0000000000',
                'float64',
            ),
        }

        input = inputs[dtype]
        if not hasattr(pd, input.expected_dtype) and not hasattr(numpy, input.expected_dtype):
            pytest.skip("Could not test this nullable type, the version of pandas does not provide it")

        query = f"""
            select
                a::{dtype} a
            from (VALUES
                (NULL),
                ({input.value}),
                (NULL)
            ) t(a)
        """

        rel = duckdb_cursor.sql(query)
        # Pandas <= 2.2.3 does not convert without throwing a warning
        warnings.simplefilter(action='ignore', category=RuntimeWarning)
        with suppress(TypeError):
            df = rel.df()
            warnings.resetwarnings()

            if hasattr(pd, input.expected_dtype):
                expected_dtype = getattr(pd, input.expected_dtype)
            else:
                expected_dtype = numpy.dtype(input.expected_dtype)
            assert isinstance(df['a'].dtype, expected_dtype)
