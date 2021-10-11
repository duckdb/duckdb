#!/usr/bin/env python
# -*- coding: utf-8 -*-

import duckdb
import pandas as pd
import numpy

# Join from pandas not matching identical strings #1767
class TestIssue1767(object):
    def test_unicode_join_pandas(self, duckdb_cursor):
        A = pd.DataFrame({"key": ["a", "п"]})
        B = pd.DataFrame({"key": ["a", "п"]})
        con = duckdb.connect(":memory:")
        arrow = con.register("A", A).register("B", B)
        q = arrow.query("""SELECT key FROM "A" FULL JOIN "B" USING ("key") ORDER BY key""")
        result = q.df()

        d = {'key': ["a", "п"]}
        df = pd.DataFrame(data=d)
        assert (result.equals(df))


