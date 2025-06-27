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
            # run a very slow query which hopefully isn't too memory intensive.
            with reraise:
                try:
                    conn.execute("select max(sha1(n::varchar)) from t").fetchall()
                except duckdb.InterruptException:
                    pass

        thread = threading.Thread(target=thread_target)
        thread.start()

        # monitor the query running in the thread, wait for progress > 0
        # the 'for/else' is just so the test times out after 5 seconds if the
        # query never progresses.  This will also fail if the query is too
        # quick as it will be back at -1 as soon as the query is finished.

        for _ in range(0, 500):
            assert thread.is_alive(), "query finished too quick"
            if (qp1 := conn.query_progress()) > 0:
                break
            time.sleep(0.01)
        else:
            pytest.fail("query start timeout")

        # keep monitoring and wait for the progress to increase
        for _ in range(0, 500):
            assert thread.is_alive(), "query finished too quick"
            if (qp2 := conn.query_progress()) > qp1:
                break
            time.sleep(0.01)
        else:
            pytest.fail("query progress timeout")

        # check that progress numbers are sensible
        assert 100 >= qp2 > qp1 > 0

        # kill the query to reduce CPU usage.
        conn.interrupt()
        thread.join()

    def test_query_progress_closed_connection(self):
        conn = duckdb.connect()
        conn.close()
        with pytest.raises(duckdb.ConnectionException):
            conn.query_progress()
