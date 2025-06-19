import duckdb
import pytest

pa = pytest.importorskip("pyarrow")


class TestArrowBinaryView(object):
    def test_arrow_binary_view(self, duckdb_cursor):
        con = duckdb.connect()
        tab = pa.table({"x": pa.array([b"abc", b"thisisaverybigbinaryyaymorethanfifteen", None], pa.binary_view())})
        assert con.execute("FROM tab").fetchall() == [(b'abc',), (b'thisisaverybigbinaryyaymorethanfifteen',), (None,)]
        # By default we won't export a view
        assert not con.execute("FROM tab").arrow().equals(tab)
        # We do the binary view from 1.4 onwards
        con.execute("SET arrow_output_version = 1.4")
        assert con.execute("FROM tab").arrow().equals(tab)

        assert con.execute("FROM tab where x = 'thisisaverybigbinaryyaymorethanfifteen'").fetchall() == [
            (b'thisisaverybigbinaryyaymorethanfifteen',)
        ]
