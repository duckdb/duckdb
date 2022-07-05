import pandas as pd
import duckdb

def test_roundtrip_substrait(require):
    connection = require('substrait')
    if connection is None:
        return

    connection.execute('CREATE TABLE integers (i integer)')
    connection.execute('INSERT INTO integers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)')
    res = connection.get_substrait("select * from integers limit 5")
    proto_bytes = res.fetchone()[0]

    query_result = connection.from_substrait(proto_bytes)

    expected = pd.Series(range(5), name="i", dtype="int32")

    pd.testing.assert_series_equal(query_result.df()["i"], expected)

