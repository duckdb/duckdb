import duckdb


class Test6315(object):
    def test_6315(self, duckdb_cursor):
        # segfault when accessing description after fetching rows
        c = duckdb.connect(":memory:")
        rv = c.execute("select * from sqlite_master where type = 'table'")
        rv.fetchall()
        desc = rv.description
        names = [x[0] for x in desc]
        assert names == ['type', 'name', 'tbl_name', 'rootpage', 'sql']

        # description of relation
        rel = c.sql("select * from sqlite_master where type = 'table'")
        desc = rel.description
        names = [x[0] for x in desc]
        assert names == ['type', 'name', 'tbl_name', 'rootpage', 'sql']

        rel.fetchall()
        desc = rel.description
        names = [x[0] for x in desc]
        assert names == ['type', 'name', 'tbl_name', 'rootpage', 'sql']
