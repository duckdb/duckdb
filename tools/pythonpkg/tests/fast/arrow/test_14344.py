import duckdb
import pytest

pa = pytest.importorskip("pyarrow")
import hashlib


def test_14344(duckdb_cursor):
    my_table = pa.Table.from_pydict({"foo": pa.array([hashlib.sha256("foo".encode()).digest()], type=pa.binary())})
    my_table2 = pa.Table.from_pydict(
        {"foo": pa.array([hashlib.sha256("foo".encode()).digest()], type=pa.binary()), "a": ["123"]}
    )

    res = duckdb_cursor.sql(
        f"""
		SELECT
			my_table2.* EXCLUDE (foo)
		FROM
			my_table
		LEFT JOIN
			my_table2
		USING (foo)
	"""
    ).fetchall()
    assert res == [('123',)]
