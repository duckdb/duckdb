# Test that long running queries properly respond to interrupts
# As reported in https://github.com/duckdb/duckdb/issues/10216
# This is important for interactive responses like in Jupyter notebook
# Adapt interrupt test from ipython as shown here:
# https://github.com/itamarst/ipython/blob/561d08809d08a4473e5a085392678544888fdb61/IPython/utils/tests/test_process.py

import duckdb
import pandas as pd
import numpy as np
import signal
import time
from _thread import interrupt_main  # Py 3
import threading


def large_test_data_gen():
    print("generating test data")
    n = 1_000_000
    columns = 100
    df = pd.DataFrame({f'col{i}': 1000 * np.random.sample(n) for i in range(columns)})
    print("test data generation complete")
    return df


def long_running_query():
    return """
    SELECT 
    DISTINCT ON (floor(col0))
    *
    FROM df
    ORDER by col0 DESC                 
    """


def limited_column_query():
    return """
    SELECT 
    DISTINCT ON (floor(col0))
    col0, col1, col2
    FROM df
    ORDER by col0 DESC             
    """


class TestQueryInterrupt(object):

    def assert_interrupts(self, query_name, query, test_data):
        """
        Interrupt a query after a delay and check response time
        """

        # Some tests can overwrite SIGINT handler (by using pdb for example),
        # which then breaks this test, so just make sure it's operating
        # normally.
        signal.signal(signal.SIGINT, signal.default_int_handler)
        interrupt_delay = 0.1

        def interrupt():
            # Wait for query to start:
            time.sleep(interrupt_delay)
            print("Sending interrupt")
            interrupt_main()

        print(f"testing {query_name}")
        result = None
        qry = query()
        df = test_data
        stmt = duckdb.sql(qry)
        threading.Thread(target=interrupt).start()
        start = time.time()
        try:
            result = stmt.fetchall()
        except RuntimeError as err:
            # Success!
            assert err.args[0] == 'Query interrupted'
            print("Successful interrupt with error")
        end = time.time()

        # In case query finished before interrupt, wait for interrupt
        if result:
            try:
                time.sleep(interrupt_delay)
            except KeyboardInterrupt:
                pass
        elapsed = end - start - interrupt_delay
        print("elapsed time: " + str(elapsed))

        # Elapsed time is not entirely predictable, there may still be significant C++ unwind time
        # when returning from exception
        assert (
            elapsed < 10
        ), query_name + "Query didn't respond to interrupt fast enough. Took %s seconds to respond." % (end - start)
        print("\n")

    def test_system_interrupt(self):
        """
        Test that queries respond promptly to keyboard interrupts
        """
        large_test_data = large_test_data_gen()
        print("\n")
        self.assert_interrupts("limited_column_query", limited_column_query, large_test_data)
        self.assert_interrupts("long_running_query", long_running_query, large_test_data)
