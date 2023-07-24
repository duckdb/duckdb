import duckdb

import pytest

pd = pytest.importorskip("pandas")


class TestResolveObjectColumns(object):
    def test_result_collector_setting(self):
        con = duckdb.connect()

        class Thrower:
            def __init__(self):
                self.counter = 0

            def __call__(self, x):
                if self.counter == 0:
                    # Only raise the first time this is invoked
                    self.counter += 1
                    raise Exception()
                else:
                    self.counter += 1

        thrower = Thrower()

        con.create_function("throw", thrower, ['VARCHAR'], 'VARCHAR')
        rel = con.sql("select throw(a) from (select 'test') tbl(a)")

        # This will cause the 'thrower' to raise an exception
        try:
            res = rel.df()
        except:
            pass

        # If the result collector isn't reset to nullptr this would cause a NotImplementedException when Fetch() is invoked
        res = rel.fetchall()
        assert res == [(None,)]
