import duckdb
import tempfile
import os
import pandas as pd

class TestInsert(object):
    test_df = pd.DataFrame.from_dict({"i":[1, 2, 3], "j":["one", "two", "three"]})\
    # connect to an in-memory temporary database
    conn = duckdb.connect()
    # get a cursor
    cursor = conn.cursor()
    conn.execute("CREATE TABLE test (i INTEGER, j STRING)")
    rel = conn.table("test")
    rel.insert([1,'one'])
    rel.insert([2,'two'])
    rel.insert([3,'three'])
    rel_a3 = cursor.table('test').project('CAST(i as BIGINT)i, j').to_df()
    pd.testing.assert_frame_equal(rel_a3, test_df)
