import duckdb
import pytest
from conftest import NumpyPandas, ArrowPandas


class TestLimitPandas(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_limit_df(self, duckdb_cursor, pandas):
        df_in = pandas.DataFrame(
            {
                'numbers': [1, 2, 3, 4, 5],
            }
        )
        limit_df = duckdb.limit(df_in, 2)
        assert len(limit_df.execute().fetchall()) == 2

    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_aggregate_df(self, duckdb_cursor, pandas):
        df_in = pandas.DataFrame(
            {
                'numbers': [1, 2, 2, 2],
            }
        )
        aggregate_df = duckdb.aggregate(df_in, 'count(numbers)', 'numbers').order('all')
        assert aggregate_df.execute().fetchall() == [(1,), (3,)]
