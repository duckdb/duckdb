import numpy as np
from datetime import time, timezone
import duckdb
import pytest

pandas = pytest.importorskip("pandas")


class TestTimeTz(object):
    def test_time_tz(self, duckdb_cursor):
        df = pandas.DataFrame({"col1": [time(1, 2, 3, tzinfo=timezone.utc)]})

        sql = f'SELECT * FROM df'

        connection = duckdb_cursor.connect()
        connection.execute(sql)

        res = connection.fetchall()
        print(res)
