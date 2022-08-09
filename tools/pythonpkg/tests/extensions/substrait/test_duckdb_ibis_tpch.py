import duckdb
import pytest

try:
    from ibis_substrait.compiler.decompile import decompile
    from ibis_substrait.proto.substrait import plan_pb2
    can_run = True

    def duck_to_ibis(duck_con, query_number):
        # Gets TPC-H Query for DuckDB
        query = duck_con.execute("select query from tpch_queries() where query_nr="+str(query_number)).fetchone()[0]
        try:
            # Compiles TPC-H in DuckDB to substrait proto
            proto = duck_con.get_substrait(query).fetchone()[0]
        except Exception as err:
            raise ValueError("DuckDB Compilation: " + str(err))
        try:
            # Executed Ibis' substrait ```proto``` in Ibis
            plan = plan_pb2.Plan()
            plan.ParseFromString(proto)
            (result,) = decompile(plan)
        except Exception as err:
            raise ValueError("Ibis Consumption: " + str(err))

except:
    can_run = False

def execute_substrait_ibis_to_duck(require, query_number):
    if not can_run:
        return
    connection = require('substrait', 'test.db')
    if not connection:
        return
    duck_to_ibis(connection,query_number)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_01(require):
    execute_substrait_ibis_to_duck(require,1)
    
@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_02(require):
    execute_substrait_ibis_to_duck(require,2)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_03(require):
    execute_substrait_ibis_to_duck(require,3)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_04(require):
    execute_substrait_ibis_to_duck(require,4)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_05(require):
    execute_substrait_ibis_to_duck(require,5)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_06(require):
    execute_substrait_ibis_to_duck(require,6)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_07(require):
    execute_substrait_ibis_to_duck(require,7)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_08(require):
    execute_substrait_ibis_to_duck(require,8)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_09(require):
    execute_substrait_ibis_to_duck(require,9)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_10(require):
    execute_substrait_ibis_to_duck(require,10)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_11(require):
    execute_substrait_ibis_to_duck(require,11)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_12(require):
    execute_substrait_ibis_to_duck(require,12)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_13(require):
    execute_substrait_ibis_to_duck(require,13)
    
@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_14(require):
    execute_substrait_ibis_to_duck(require,14)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_15(require):
    execute_substrait_ibis_to_duck(require,15)

@pytest.mark.xfail(reason="DuckDB Compilation: INTERNAL Error: INTERNAL Error: CHUNK_GET")
def test_query_substrait_ibis_to_duck_16(require):
    execute_substrait_ibis_to_duck(require,16)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_17(require):
    execute_substrait_ibis_to_duck(require,17)
    
@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_18(require):
    execute_substrait_ibis_to_duck(require,18)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_19(require):
    execute_substrait_ibis_to_duck(require,19)

@pytest.mark.xfail(reason="Ibis Consumption: unknown field type when decompiling: <class 'substrait.type_pb2.VarChar'>")
def test_query_substrait_ibis_to_duck_20(require):
    execute_substrait_ibis_to_duck(require,20)

@pytest.mark.xfail(reason="DuckDB Compilation: INTERNAL Error: INTERNAL Error: DELIM_JOIN")
def test_query_substrait_ibis_to_duck_21(require):
    execute_substrait_ibis_to_duck(require,21)

@pytest.mark.xfail(reason="DuckDB Compilation: INTERNAL Error: INTERNAL Error: CHUNK_GET")
def test_query_substrait_ibis_to_duck_22(require):
    execute_substrait_ibis_to_duck(require,22)

