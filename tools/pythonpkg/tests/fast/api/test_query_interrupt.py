import duckdb
import time
import pytest

import threading
import _thread as thread


def send_keyboard_interrupt():
    # Wait a little, so we're sure the 'execute' has started
    time.sleep(0.1)
    # Send an interrupt to the main thread
    thread.interrupt_main()


class TestQueryInterruption(object):
    def test_query_interruption(self):
        con = duckdb.connect()
        thread = threading.Thread(target=send_keyboard_interrupt)
        # Start the thread
        thread.start()
        try:
            res = con.execute('select count(*) from range(1000000000)').fetchall()
        except RuntimeError:
            # If this is not reached, we could not cancel the query before it completed
            # indicating that the query interruption functionality is broken
            assert True
        except KeyboardInterrupt:
            pytest.fail()
