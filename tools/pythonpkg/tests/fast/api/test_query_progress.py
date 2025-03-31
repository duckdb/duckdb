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
    def test_query_progress(self, reraise):
        conn = duckdb.connect()

        def thread_target():
            with reraise:
                # Wait for query to start running before interrupting
                time.sleep(0.1)
                x = conn.query_progress()
                time.sleep(0.1)
                y = conn.query_progress()
                assert 1 > y > x > 0
            
        thread = threading.Thread(target=thread_target)
        print("START")
        thread.start()
        conn.execute("select count(*) from range(10000000000)").fetchall()
        print("JOIN")
        thread.join()

    def test_query_progress_closed_connection(self):
        conn = duckdb.connect()
        conn.close()
        with pytest.raises(duckdb.ConnectionException):
            conn.query_progress()
