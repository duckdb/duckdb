import pytest
import duckdb
pandas = pytest.importorskip("pandas")
import os

def ExpectedExampleOutput():
    return [
        (1, 'Dulce', 'Abril', 'Female', 'United States', 32, '15/10/2017', 1562),
        (2, 'Mara', 'Hashimoto', 'Female', 'Great Britain', 25, '16/08/2016', 1582),
        (3, 'Philip', 'Gent', 'Male', 'France', 36, '21/05/2015', 2587),
        (4, 'Kathleen', 'Hanner', 'Female', 'United States', 25, '15/10/2017', 3549),
        (5, 'Nereida', 'Magwood', 'Female', 'United States', 58, '16/08/2016', 2468),
        (6, 'Gaston', 'Brumm', 'Male', 'United States', 24, '21/05/2015', 2554),
        (7, 'Etta', 'Hurn', 'Female', 'Great Britain', 56, '15/10/2017', 3598),
        (8, 'Earlean', 'Melgar', 'Female', 'United States', 27, '16/08/2016', 2456),
        (9, 'Vincenza', 'Weiland', 'Female', 'United States', 40, '21/05/2015', 6548)
    ]

class TestReadExcel(object):
    def test_call_from_wrapper(self):
        file_name = os.path.join(os.path.dirname(os.path.realpath(__file__)),'..','data','example.xls')
        res = duckdb.read_excel(file_name).fetchall()
        assert res == ExpectedExampleOutput()

    def test_call_from_connection(self, duckdb_cursor):
        file_name = os.path.join(os.path.dirname(os.path.realpath(__file__)),'..','data','example.xls')
        res = duckdb_cursor.read_excel(file_name).fetchall()
        assert res == ExpectedExampleOutput()