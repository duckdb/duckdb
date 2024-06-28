import duckdb
import pytest

pyarrow = pytest.importorskip('pyarrow')


@pytest.mark.parametrize('arrow_large_buffer_size', [True, False])
def test_10795(arrow_large_buffer_size):
    conn = duckdb.connect()
    conn.sql(f"set arrow_large_buffer_size={arrow_large_buffer_size}")
    arrow = conn.sql("select map(['non-inlined string', 'test', 'duckdb'], [42, 1337, 123]) as map").to_arrow_table()
    assert arrow.to_pydict() == {'map': [[('non-inlined string', 42), ('test', 1337), ('duckdb', 123)]]}
