import duckdb
import os

def get_query_binary(query_number):
    bin_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),'ibis_tpch_binary',str(query_number)+".bin")
    file=open(bin_file_path,"r")
    return file.readline()

def execute_substrait(duckdb_cursor, query_number):
    proto_bytes = get_query_binary(query_number)
    result = duckdb_cursor.execute("CALL from_substrait("+proto_bytes+"::BLOB)").df().sort_index(ascending=False, axis=1)
    query = duckdb_cursor.execute("select query from tpch_queries() where query_nr="+str(query_number)).fetchone()[0]
    answers = duckdb_cursor.execute(query).df().sort_index(ascending=False, axis=1)
    assert result.equals(answers)

class TestTPCHIbisSubstrait(object):
    # Ibis has a < instead of <= in the filter
    # def test_q01(self,duckdb_cursor):
    #     execute_substrait(duckdb_cursor,1)
    
    def test_q03(self,duckdb_cursor):
        execute_substrait(duckdb_cursor,3)

    # We are missing any
    # def test_q04(self,duckdb_cursor):
    #     execute_substrait(duckdb_cursor,4)

    # We are missing any
    # def test_q05(self,duckdb_cursor):
    #     execute_substrait(duckdb_cursor,5)  

    def test_q06(self,duckdb_cursor):
        execute_substrait(duckdb_cursor,6)

    # It seems that Ibis is exporting a cast function with only one child?
    # def test_q09(self,duckdb_cursor):
    #     execute_substrait(duckdb_cursor,9)

    def test_q10(self,duckdb_cursor):
        execute_substrait(duckdb_cursor,10)

# # FAILED test_ibis_tpch.py::TestTPCHIbisSubstrait::test_q11 - We seem to be missing a sum aggregation somewhere
#     def test_q11(self,duckdb_cursor):
#         execute_substrait(duckdb_cursor,11)

# # FAILED test_ibis_tpch.py::TestTPCHIbisSubstrait::test_q12 - RuntimeError: Catalog Error: Scalar Function with name value_list does not exist!
#     def test_q12(self,duckdb_cursor):
#         execute_substrait(duckdb_cursor,12)
# # FAILED test_ibis_tpch.py::TestTPCHIbisSubstrait::test_q13 - RuntimeError: Catalog Error: Scalar Function with name not does not exist!
#     def test_q13(self,duckdb_cursor):
#         execute_substrait(duckdb_cursor,13)

# FAILED test_ibis_tpch.py::TestTPCHIbisSubstrait::test_q16 - RuntimeError: Parser Error: syntax error at or near "\"
# FAILED test_ibis_tpch.py::TestTPCHIbisSubstrait::test_q18 - RuntimeError: Parser Error: syntax error at or near "\"
# FAILED test_ibis_tpch.py::TestTPCHIbisSubstrait::test_q19 - RuntimeError: Parser Error: syntax error at or near "\"
# FAILED test_ibis_tpch.py::TestTPCHIbisSubstrait::test_q20 - RuntimeError: Parser Error: syntax error at or near "\"
# FAILED test_ibis_tpch.py::TestTPCHIbisSubstrait::test_q21 - RuntimeError: Parser Error: syntax error at or near "\"
# FAILED test_ibis_tpch.py::TestTPCHIbisSubstrait::test_q22 - RuntimeError: Parser Error: syntax error at or near "\"
    # def test_q16(self,duckdb_cursor):
    #     execute_substrait(duckdb_cursor,16)

    # def test_q18(self,duckdb_cursor):
    #     execute_substrait(duckdb_cursor,18)

    # def test_q19(self,duckdb_cursor):
    #     execute_substrait(duckdb_cursor,19)

    # def test_q20(self,duckdb_cursor):
    #     execute_substrait(duckdb_cursor,20)

    # def test_q21(self,duckdb_cursor):
    #     execute_substrait(duckdb_cursor,21)

    # def test_q22(self,duckdb_cursor):
    #     execute_substrait(duckdb_cursor,22)