import duckdb
import numpy

def check_result(duckdb_cursor,value, type):
    duckdb_cursor.execute("SELECT " + str(value)+"::"+ type)
    results = duckdb_cursor.fetchall()
    assert results[0][0] == value

class TestNumeric(object):
    def test_numeric_results(self, duckdb_cursor):
        check_result(duckdb_cursor,1,"TINYINT")
        check_result(duckdb_cursor,1,"SMALLINT")
        check_result(duckdb_cursor,1,"FLOAT")