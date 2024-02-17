# Show query crash when complex query is interrupted
# In cpp code we simulate interrupt to show crash with EndQueryInternal

# import pytest
import duckdb
import pandas as pd
import numpy as np
import faulthandler

faulthandler.enable()


def large_test_data_gen():
    print("generating test data")
    n = 1_000_000
    columns = 1000
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


class TestQueryInterrupt(object):
    def configure_duckdb(self):
        # Configure memory and threads for test
        duckdb.execute(
            """
            -- set the memory limit of the system 
            SET memory_limit = '200GB';
            -- configure the system to use x threads
            SET threads TO 16;
            -- temp dir
            SET temp_directory='.tmp';
            -- enable printing of a progress bar during long-running queries
            SET enable_progress_bar = true;
            """
        )

        stmt = duckdb.sql(
            """
            -- show a list of all available settings
            SELECT * FROM duckdb_settings() order by name;
            """
        )

        """
        print("DB settings:")
        vals = stmt.fetchall()
        for val in vals:
            print(val)
        """
        

    def assert_interrupts(self, query_name, query, test_data):
        """
        Check that interrupt returns error
        """

        print(f"testing {query_name}")
        result = None
        qry = query()
        df = test_data
        stmt = duckdb.sql(qry)
        result = stmt.fetchall()
        # print(result)


    
    def test_query_interrupt(self):
        """
        Test long running query with simulated interrupt
        """
        print("Configuring DB")
        self.configure_duckdb()
        print("Done with configuration")
        large_test_data = large_test_data_gen()
        print("\n")
        self.assert_interrupts("long_running_query", long_running_query, large_test_data)
        

def run_test():
    print("Starting test.")
    mytest = TestQueryInterrupt()
    mytest.test_query_interrupt()
    print("Tests complete!")


run_test()
