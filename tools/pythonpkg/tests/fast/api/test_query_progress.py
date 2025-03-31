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
        conn.sql("set enable_progress_bar_print=false")
        conn.sql("set progress_bar_time=0")
        conn.sql("create table t as (select range as n from range(10000000))")

        def thread_target():
            with reraise:
                # Wait for query to start running before interrupting
                time.sleep(0.1)
                x = conn.query_progress()
                time.sleep(0.1)
                y = conn.query_progress()
                assert 100 > y > x > 0
            
        thread = threading.Thread(target=thread_target)
        print("START")
        thread.start()
        conn.execute("select max(sha1(n::varchar)) from t").fetchall()
        print("JOIN")
        thread.join()

    def test_query_progress_closed_connection(self):
        conn = duckdb.connect()
        conn.close()
        with pytest.raises(duckdb.ConnectionException):
            conn.query_progress()
