from pytest import importorskip

importorskip('pyarrow')

import duckdb
from pyarrow import scalar, string, large_string, list_, int32, types


def test_nested(duckdb_cursor):
    res = run(duckdb_cursor, 'select 42::UNION(name VARCHAR, attr UNION(age INT, veteran BOOL)) as res')
    assert types.is_union(res.type)
    assert res.value.value == scalar(42, type=int32())


def test_union_contains_nested_data(duckdb_cursor):
    _ = importorskip("pyarrow", minversion="11")
    res = run(duckdb_cursor, "select ['hello']::UNION(first_name VARCHAR, middle_names VARCHAR[]) as res")
    assert types.is_union(res.type)
    assert res.value == scalar(['hello'], type=list_(string()))


def test_unions_inside_lists_structs_maps(duckdb_cursor):
    res = run(duckdb_cursor, "select [union_value(name := 'Frank')] as res")
    assert types.is_list(res.type)
    assert types.is_union(res.type.value_type)
    assert res[0].value == scalar('Frank', type=string())


def test_unions_with_struct(duckdb_cursor):
    duckdb_cursor.execute(
        """
		CREATE TABLE tbl (a UNION(a STRUCT(a INT, b BOOL)))
	"""
    )
    duckdb_cursor.execute(
        """
		INSERT INTO tbl VALUES ({'a': 42, 'b': true})
	"""
    )

    rel = duckdb_cursor.table('tbl')
    arrow = rel.arrow()

    duckdb_cursor.execute("create table other as select * from arrow")
    rel2 = duckdb_cursor.table('other')
    res = rel2.fetchall()
    assert res == [({'a': 42, 'b': True},)]


def run(conn, query):
    return conn.sql(query).arrow().columns[0][0]
