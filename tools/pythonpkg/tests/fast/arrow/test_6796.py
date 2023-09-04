import duckdb
import pytest
from conftest import NumpyPandas, ArrowPandas

pyarrow = pytest.importorskip('pyarrow')


@pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
def test_6796(pandas):
    conn = duckdb.connect()
    input_df = pandas.DataFrame({"foo": ["bar"]})
    conn.register("input_df", input_df)

    query = """
	select * from input_df
	union all
	select * from input_df
	"""

    # fetching directly into Pandas works
    res_df = conn.execute(query).fetch_df()
    res_arrow = conn.execute(query).fetch_arrow_table()

    df_arrow_table = pyarrow.Table.from_pandas(res_df)

    result_1 = conn.execute("select * from df_arrow_table order by all").fetchall()

    result_2 = conn.execute("select * from res_arrow order by all").fetchall()

    assert result_1 == result_2
