import duckdb
import time
import pytest

import threading
import _thread as thread

def send_keyboard_interrupt():
    # Wait a couple seconds
    time.sleep(2)
    # Send an interrupt to the main thread
    thread.interrupt_main()

class TestQueryInterruption(object):
    def test_query_interruption(self):
        con = duckdb.connect()
        thread = threading.Thread(target=send_keyboard_interrupt)
        # Start the thread
        thread.start()
        with pytest.raises(RuntimeError):
            res = con.execute('select count(*) from range(100000000000000)').fetchall()
        # If this is not reached, this test will hang forever
        # indicating that the query interruption functionality is broken
