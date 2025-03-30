import platform
import threading
import time

import duckdb
import pytest


class TestQueryProgress(object):
    @pytest.mark.xfail(
        condition=platform.system() == "Emscripten",
        reason="threads not allowed on Emscripten",
    )
    def test_query_progress(self):
        conn = duckdb.connect()

        def thread_target():
            # Wait for query to start running before interrupting
            time.sleep(0.1)
            x = conn.query_progress()
            time.sleep(0.5)
            y = conn.query_progress()
            assert 1 > y > x > 0
            
        thread = threading.Thread(target=thread_target)
        thread.start()
        conn.execute("select count(*) from range(100000000000)").fetchall()
        thread.join()

    def test_query_progress_closed_connection(self):
        conn = duckdb.connect()
        conn.close()
        with pytest.raises(duckdb.ConnectionException):
            conn.query_progress()
