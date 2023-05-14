import duckdb
import pytest
import os

class TestWindowsAbsPath(object):
    def test_windows_path_accent(self):
        if os.name != 'nt':
            return
        current_directory = os.getcwd()
        test_dir = os.path.join(current_directory, 'tést')
        dbpath = os.path.join(test_dir, 'test.db')
        try:
            os.mkdir(test_dir)
        except FileExistsError:
            pass
        con = duckdb.connect(dbpath)
        con.execute("CREATE OR REPLACE TABLE int AS SELECT * FROM range(10) t(i)")
        res = con.execute("SELECT COUNT(*) FROM int").fetchall()
        assert res[0][0] == 10
        del res
        del con

        os.chdir('tést')
        dbpath = os.path.join('..',  dbpath)
        con = duckdb.connect(dbpath)
        res = con.execute("SELECT COUNT(*) FROM int").fetchall()
        assert res[0][0] == 10
        del res
        del con

        os.chdir('..')

    def test_windows_abs_path(self):
        if os.name != 'nt':
            return
        current_directory = os.getcwd()
        dbpath = os.path.join(current_directory, 'test.db')
        con = duckdb.connect(dbpath)
        con.execute("CREATE OR REPLACE TABLE int AS SELECT * FROM range(10) t(i)")
        res = con.execute("SELECT COUNT(*) FROM int").fetchall()
        assert res[0][0] == 10
        del res
        del con

        assert dbpath[1] == ':'
        # remove the drive separator and reconnect
        dbpath = dbpath[2:]
        con = duckdb.connect(dbpath)
        res = con.execute("SELECT COUNT(*) FROM int").fetchall()
        assert res[0][0] == 10
        del res
        del con

        # forward slashes work as well
        dbpath = dbpath.replace('\\', '/')
        con = duckdb.connect(dbpath)
        res = con.execute("SELECT COUNT(*) FROM int").fetchall()
        assert res[0][0] == 10
        del res
        del con
