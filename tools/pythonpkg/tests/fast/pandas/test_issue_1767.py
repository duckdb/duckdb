#!/usr/bin/env python
# -*- coding: utf-8 -*-

import duckdb
import numpy
import pytest
from conftest import NumpyPandas, ArrowPandas


# Join from pandas not matching identical strings #1767
class TestIssue1767(object):
    @pytest.mark.parametrize('pandas', [NumpyPandas(), ArrowPandas()])
    def test_unicode_join_pandas(self, duckdb_cursor, pandas):
        A = pandas.DataFrame({"key": ["a", "п"]})
        B = pandas.DataFrame({"key": ["a", "п"]})
        con = duckdb.connect(":memory:")
        arrow = con.register("A", A).register("B", B)
        q = arrow.query("""SELECT key FROM "A" FULL JOIN "B" USING ("key") ORDER BY key""")
        result = q.df()

        d = {'key': ["a", "п"]}
        df = pandas.DataFrame(data=d)
        pandas.testing.assert_frame_equal(result, df)
