import threading
import time

import duckdb
import pytest


class TestConnectionInterrupt(object):
    def test_connection_interrupt(self):
        conn = duckdb.connect()

        def interrupt():
            # Wait for query to start running before interrupting
            time.sleep(0.1)
            conn.interrupt()

        thread = threading.Thread(target=interrupt)
        thread.start()
        with pytest.raises(duckdb.InterruptException):
            conn.execute("select count(*) from range(1000000000)").fetchall()

    def test_interrupt_closed_connection(self):
        conn = duckdb.connect()
        conn.close()
        with pytest.raises(duckdb.ConnectionException):
            conn.interrupt()
