from pytest import importorskip

importorskip('pyarrow')

import duckdb
from pyarrow import scalar, string, large_string, list_, int32, types


def test_nested():
    res = run('select 42::UNION(name VARCHAR, attr UNION(age INT, veteran BOOL)) as res')
    assert types.is_union(res.type)
    assert res.value.value == scalar(42, type=int32())


def test_union_contains_nested_data():
    res = run("select ['hello']::UNION(first_name VARCHAR, middle_names VARCHAR[]) as res")
    assert types.is_union(res.type)
    assert res.value == scalar(['hello'], type=list_(string()))


def test_unions_inside_lists_structs_maps():
    res = run("select [union_value(name := 'Frank')] as res")
    assert types.is_list(res.type)
    assert types.is_union(res.type.value_type)
    assert res[0].value == scalar('Frank', type=string())


def run(query):
    return duckdb.sql(query).arrow().columns[0][0]
