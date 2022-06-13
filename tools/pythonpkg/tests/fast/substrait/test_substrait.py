import pandas as pd


def test_roundtrip_substrait(duckdb_cursor):
    res = duckdb_cursor.get_substrait("select * from integers limit 5")
    proto_bytes = res.fetchone()[0]

    query_result = duckdb_cursor.from_substrait(proto_bytes)

    expected = pd.Series(range(5), name="i", dtype="int32")

    pd.testing.assert_series_equal(query_result.df()["i"], expected)

