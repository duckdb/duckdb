import traceback

class TestNull(object):

    def test_fetchone_null(self, duckdb_cursor):
        duckdb_cursor.execute("CREATE TABLE atable (Value int)")
        duckdb_cursor.execute("INSERT INTO atable VALUES (1)")
        duckdb_cursor.execute("SELECT * FROM atable")
        assert(duckdb_cursor.fetchone()[0] == 1)
        assert(duckdb_cursor.fetchone() is None)
        
        
       