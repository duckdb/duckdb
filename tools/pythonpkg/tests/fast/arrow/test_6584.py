from concurrent.futures import ThreadPoolExecutor
import duckdb
import pytest

pyarrow = pytest.importorskip('pyarrow')


def f(cur, i, data):
    cur.execute(f"create table t_{i} as select * from data")
    return cur.execute(f"select * from t_{i}").arrow()


def test_6584():
    pool = ThreadPoolExecutor(max_workers=2)
    data = pyarrow.Table.from_pydict({"a": [1, 2, 3]})
    c = duckdb.connect()
    futures = []
    for i in range(2):
        fut = pool.submit(f, c.cursor(), i, data)
        futures.append(fut)

    for fut in futures:
        arrow_res = fut.result()
        assert data.equals(arrow_res)
