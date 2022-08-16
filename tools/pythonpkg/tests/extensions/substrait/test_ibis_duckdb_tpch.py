import duckdb
import pytest

SubstraitCompiler = pytest.importorskip('ibis_substrait.compiler.core')
ibis = pytest.importorskip('ibis')
BaseBackend = pytest.importorskip('ibis.backends.base')
parse_type = pytest.importorskip('ibis.backends.duckdb.datatypes')
get_tpch_query = pytest.importorskip('ibis_tpch_util')


def unbound_from_duckdb(table):  # noqa: D103
    return ibis.table(
        list(zip(table.columns, map(parse_type.parse_type, table.dtypes))), name=table.alias
    )

class TPCHBackend(BaseBackend.BaseBackend):  # noqa: D101
    def __init__(self,duck_con,  scale_factor=0.1):  # noqa: D107
        self.con = duck_con
        try:
            self.con.table('lineitem')
        except:
            self.con.execute(f"CALL dbgen(sf={scale_factor})")

        _tables = self.con.execute("PRAGMA show_tables").fetchall()
        _tables = map(lambda x: x[0], _tables)

        self.tables = {
            table.alias: unbound_from_duckdb(table)
            for table in map(
                self.con.table,
                _tables,
            )
        }

    def table(self, table):  # noqa: D102
        return self.tables.get(table)

    def current_database(self):  # noqa: D102
        ...

    def list_databases(self):  # noqa: D102
        ...

    def list_tables(self):  # noqa: D102
        ...

    def version(self):  # noqa: D102
        return "awesome"


def ibis_to_duck(duck_con, query_number):
    # Gets TPC-H Query for IBIS
    tpch_query = get_tpch_query.get_tpch_query(query_number)(duck_con)
    # Ibis Substrait Compiler (i.e., Producer)
    compiler = SubstraitCompiler.SubstraitCompiler()
    try:
        # Compiles TPC-H in Ibis to substrait proto
        proto = compiler.compile(tpch_query)
    except Exception as err:
        raise ValueError("Ibis Compilation: " + str(err))
    try:
        # Executed Ibis' substrait ```proto``` in DuckDB
        result = duck_con.con.from_substrait(proto.SerializeToString())
    except Exception as err:
        raise ValueError("DuckDB Consumption: " + str(err))
    
    # Result Checking
    result = result.df().sort_index(ascending=False, axis=1)
    query = duck_con.con.execute("select query from tpch_queries() where query_nr="+str(query_number)).fetchone()[0]
    answer = duck_con.con.execute(query).df().sort_index(ascending=False, axis=1)
    assert result.equals(answer)


def run_query(require, query_number):
    connection = require('substrait', 'test.db')
    if not connection:
        return
    duck_con = TPCHBackend(duck_con=connection)
    ibis_to_duck(duck_con,query_number)

def test_query_substrait_ibis_to_duck_01(require):
    run_query(require,1)
    
@pytest.mark.xfail(reason="Ibis Compilation: 'TableArrayView")
def test_query_substrait_ibis_to_duck_02(require):
    run_query(require,2)

@pytest.mark.xfail(reason="Attempting to fetch from an unsuccessful query result")
def test_query_substrait_ibis_to_duck_03(require):
    run_query(require,3)

@pytest.mark.xfail(reason="Ibis Compilation: 'ExistsSubquery'")
def test_query_substrait_ibis_to_duck_04(require):
    run_query(require,4)

@pytest.mark.xfail(reason="Attempting to fetch from an unsuccessful query result")
def test_query_substrait_ibis_to_duck_05(require):
    run_query(require,5)
    
def test_query_substrait_ibis_to_duck_06(require):
    run_query(require,6)

@pytest.mark.xfail(reason="Ibis Compilation: (SelfReference(table=UnboundTable: nation")
def test_query_substrait_ibis_to_duck_07(require):
    run_query(require,7)

@pytest.mark.xfail(reason="Ibis Compilation: (SelfReference(table=UnboundTable: nation")
def test_query_substrait_ibis_to_duck_08(require):
    run_query(require,8)

@pytest.mark.xfail(reason="DuckDB Consumption: Unsupported expression type 10")
def test_query_substrait_ibis_to_duck_09(require):
    run_query(require,9)

@pytest.mark.xfail(reason="Attempting to fetch from an unsuccessful query result")    
def test_query_substrait_ibis_to_duck_10(require):
    run_query(require,10)

@pytest.mark.xfail(reason="Results do not match")
def test_query_substrait_ibis_to_duck_11(require):
    run_query(require,11)

def test_query_substrait_ibis_to_duck_12(require):
    run_query(require,12)

@pytest.mark.xfail(reason="DuckDB Consumption: Catalog Error: Scalar Function with name not does not exist!")
def test_query_substrait_ibis_to_duck_13(require):
    run_query(require,13)
    
@pytest.mark.xfail(reason="Ibis Compilation: Parameter to MergeFrom() must be instance of same class: expected substrait.Expression got substrait")
def test_query_substrait_ibis_to_duck_14(require):
    run_query(require,14)

@pytest.mark.xfail(reason="Ibis Compilation: non-empty child_rel_field_offsets passed in to selection translation rule")
def test_query_substrait_ibis_to_duck_15(require):
    run_query(require,15)

@pytest.mark.xfail(reason="Ibis Compilation: 'IntegerColumn' object is not iterable")
def test_query_substrait_ibis_to_duck_16(require):
    run_query(require,16)

@pytest.mark.xfail(reason="Ibis Compilation: <class 'ibis.expr.operations.relations.Aggregation'>")
def test_query_substrait_ibis_to_duck_17(require):
    run_query(require,17)
    
@pytest.mark.xfail(reason="Ibis Compilation: 'IntegerColumn' object is not iterable")
def test_query_substrait_ibis_to_duck_18(require):
    run_query(require,18)

def test_query_substrait_ibis_to_duck_19(require):
    run_query(require,19)

@pytest.mark.xfail(reason="Ibis Compilation: 'IntegerColumn' object is not iterable")
def test_query_substrait_ibis_to_duck_20(require):
    run_query(require,20)

@pytest.mark.xfail(reason="Ibis Compilation: 'ExistsSubquery'")
def test_query_substrait_ibis_to_duck_21(require):
    run_query(require,21)

@pytest.mark.xfail(reason="Ibis Compilation: 'NotExistsSubquery'")
def test_query_substrait_ibis_to_duck_22(require):
    run_query(require,22)
