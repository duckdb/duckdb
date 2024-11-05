import numpy as np
import datetime
import duckdb
import pytest
import warnings
from contextlib import suppress
from conftest import create_masked_array


class TestPandasObjectInteger(object):
    # Signed Masked Integer types
    @pytest.mark.parametrize('nullable', [True, False])
    def test_object_integer(self, nullable):
        pd = pytest.importorskip("pandas")

        if nullable:
            int8 = 'Int8'
            int16 = 'Int16'
            int32 = 'Int32'
            int64 = 'Int64'
        else:
            # we use float because pandas only accepts None in float columns
            int8 = 'float64'
            int16 = 'float64'
            int32 = 'float64'
            int64 = 'float64'

        df_in = pd.DataFrame(
            {
                int8: pd.Series([None, 1, -1], dtype=int8),
                int16: pd.Series([None, 1, -1], dtype=int16),
                int32: pd.Series([None, 1, -1], dtype=int32),
                int64: pd.Series([None, 1, -1], dtype=int64),
            }
        )
        warnings.simplefilter(action='ignore', category=RuntimeWarning)
        df_expected_res = pd.DataFrame(
            {
                int8: create_masked_array([0, 1, -1], mask=[True, False, False], dtype=int8, nullable=nullable),
                int16: create_masked_array([0, 1, -1], mask=[True, False, False], dtype=int16, nullable=nullable),
                int32: create_masked_array([0, 1, -1], mask=[True, False, False], dtype=int32, nullable=nullable),
                int64: create_masked_array([0, 1, -1], mask=[True, False, False], dtype=int64, nullable=nullable),
            }
        )
        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df(prefer_nullable_dtypes=nullable)
        warnings.resetwarnings()
        pd.testing.assert_frame_equal(df_expected_res, df_out)

    # Unsigned Masked Integer types
    @pytest.mark.parametrize('nullable', [True, False])
    def test_object_uinteger(self, nullable):
        pd = pytest.importorskip("pandas")

        if nullable:
            uint8 = 'UInt8'
            uint16 = 'UInt16'
            uint32 = 'UInt32'
            uint64 = 'UInt64'
        else:
            uint8 = 'uint8'
            uint16 = 'uint16'
            uint32 = 'uint32'
            uint64 = 'uint64'

        with suppress(TypeError):
            df_in = pd.DataFrame(
                {
                    uint8: pd.Series([None, 1, 255], dtype=uint8),
                    uint16: pd.Series([None, 1, 65535], dtype=uint16),
                    uint32: pd.Series([None, 1, 4294967295], dtype=uint32),
                    uint64: pd.Series([None, 1, 18446744073709551615], dtype=uint64),
                }
            )
            warnings.simplefilter(action='ignore', category=RuntimeWarning)
            df_expected_res = pd.DataFrame(
                {
                    uint8: create_masked_array([0, 1, 255], mask=[True, False, False], dtype=uint8, nullable=nullable),
                    uint16: create_masked_array(
                        [0, 1, 65535], mask=[True, False, False], dtype=uint16, nullable=nullable
                    ),
                    uint32: create_masked_array(
                        [0, 1, 4294967295], mask=[True, False, False], dtype=uint32, nullable=nullable
                    ),
                    uint64: create_masked_array(
                        [0, 1, 18446744073709551615], mask=[True, False, False], dtype=uint64, nullable=nullable
                    ),
                }
            )
            df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df(prefer_nullable_dtypes=nullable)
            warnings.resetwarnings()
            pd.testing.assert_frame_equal(df_expected_res, df_out)

    # Unsigned Masked float/double types
    @pytest.mark.parametrize('nullable', [True, False])
    def test_object_float(self, nullable):
        # Require pandas 1.2.0 >= for this, because Float32|Float64 was not added before this version
        pd = pytest.importorskip("pandas", '1.2.0')
        if nullable:
            float32 = 'Float32'
            float64 = 'Float64'
        else:
            float32 = 'float32'
            float64 = 'float64'
        df_in = pd.DataFrame(
            {
                float32: pd.Series([None, 1, 4294967295], dtype=float32),
                float64: pd.Series([None, 1, 18446744073709551615], dtype=float64),
            }
        )
        df_expected_res = pd.DataFrame(
            {
                float32: create_masked_array(
                    [0, 1, 4294967295], mask=[True, False, False], dtype=float32, nullable=nullable
                ),
                float64: create_masked_array(
                    [0, 1, 18446744073709551615], mask=[True, False, False], dtype=float64, nullable=nullable
                ),
            }
        )
        df_out = duckdb.query_df(df_in, "data", "SELECT * FROM data").df(prefer_nullable_dtypes=nullable)
        pd.testing.assert_frame_equal(df_expected_res, df_out)
