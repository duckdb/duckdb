import pytest
import duckdb


class TestExplain(object):
    def test_explain_basic(self):
        res = duckdb.sql('select 42').explain()
        assert isinstance(res, str)

    def test_explain_standard(self):
        res = duckdb.sql('select 42').explain('standard')
        assert isinstance(res, str)

        res = duckdb.sql('select 42').explain('STANDARD')
        assert isinstance(res, str)

        res = duckdb.sql('select 42').explain(duckdb.STANDARD)
        assert isinstance(res, str)

        res = duckdb.sql('select 42').explain(duckdb.ExplainType.STANDARD)
        assert isinstance(res, str)

        res = duckdb.sql('select 42').explain(0)
        assert isinstance(res, str)

    def test_explain_analyze(self):
        res = duckdb.sql('select 42').explain('analyze')
        assert isinstance(res, str)

        res = duckdb.sql('select 42').explain('ANALYZE')
        assert isinstance(res, str)

        res = duckdb.sql('select 42').explain(duckdb.ANALYZE)
        assert isinstance(res, str)

        res = duckdb.sql('select 42').explain(duckdb.ExplainType.ANALYZE)
        assert isinstance(res, str)

        res = duckdb.sql('select 42').explain(1)
        assert isinstance(res, str)

    def test_explain_df(self):
        pd = pytest.importorskip("pandas")
        df = pd.DataFrame({'a': [42]})
        res = duckdb.sql('select * from df').explain('ANALYZE')
        assert isinstance(res, str)
