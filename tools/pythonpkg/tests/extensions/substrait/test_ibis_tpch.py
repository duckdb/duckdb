import duckdb
import pytest

try:
    from ibis_substrait.compiler.core import SubstraitCompiler
    import ibis
    from ibis.backends.base import BaseBackend
    from ibis.backends.duckdb.datatypes import parse_type
    from ibis_tpch_util import get_tpch_query
    can_run = True

    def unbound_from_duckdb(table):  # noqa: D103
        return ibis.table(
            list(zip(table.columns, map(parse_type, table.dtypes))), name=table.alias
        )

    class TPCHBackend(BaseBackend):  # noqa: D101
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


    def tpch_execute_ibis_to_duck_query(duck_con, query_number):
        tpch_query = get_tpch_query(query_number)(duck_con)
        compiler = SubstraitCompiler()
        try:
            proto = compiler.compile(tpch_query)
        except Exception:
            raise ValueError("can't compile")
        result = duck_con.con.from_substrait(proto.SerializeToString()).df().sort_index(ascending=False, axis=1)
        query = duck_con.con.execute("select query from tpch_queries() where query_nr="+str(query_number)).fetchone()[0]
        answer = duck_con.con.execute(query).df().sort_index(ascending=False, axis=1)
        if not (result.equals(answer)):
            print (query_number)
            print (result)
            print (answer)
        assert result.equals(answer)
except:
    can_run = False

def execute(require, query_number):
    if not can_run:
        return
    connection = require('substrait', 'test.db')
    if not connection:
        return
    duck_con = TPCHBackend(duck_con=connection)
    tpch_execute_ibis_to_duck_query(duck_con,query_number)

def test_query_substrait_01(require):
    execute(require,1)
    
@pytest.mark.xfail(reason="can't compile")
def test_query_substrait_02(require):
    execute(require,2)

def test_query_substrait_03(require):
    execute(require,3)

@pytest.mark.xfail(reason="Scalar Function with name any does not exist!")
def test_query_substrait_04(require):
    execute(require,4)

def test_query_substrait_05(require):
    execute(require,5)
    
def test_query_substrait_06(require):
    execute(require,6)

@pytest.mark.xfail(reason="can't compile")
def test_query_substrait_07(require):
    execute(require,7)

@pytest.mark.xfail(reason="can't compile")
def test_query_substrait_08(require):
    execute(require,8)

@pytest.mark.xfail(reason="Scalar Function with name extractyear does not exist!")
def test_query_substrait_09(require):
    execute(require,9)
    
def test_query_substrait_10(require):
    execute(require,10)

@pytest.mark.xfail(reason="Result does not match")
def test_query_substrait_11(require):
    execute(require,11)

def test_query_substrait_12(require):
    execute(require,12)

@pytest.mark.xfail(reason="can't compile")
def test_query_substrait_13(require):
    execute(require,13)
    
@pytest.mark.xfail(reason="can't compile")
def test_query_substrait_14(require):
    execute(require,14)

@pytest.mark.xfail(reason="can't compile")
def test_query_substrait_15(require):
    execute(require,15)

@pytest.mark.xfail(reason="can't compile")
def test_query_substrait_16(require):
    execute(require,16)

@pytest.mark.xfail(reason="can't compile")
def test_query_substrait_17(require):
    execute(require,17)
    
@pytest.mark.xfail(reason="can't compile")
def test_query_substrait_18(require):
    execute(require,18)

def test_query_substrait_19(require):
    execute(require,19)

@pytest.mark.xfail(reason="can't compile")
def test_query_substrait_20(require):
    execute(require,20)

@pytest.mark.xfail(reason="Scalar Function with name any does not exist!")
def test_query_substrait_21(require):
    execute(require,21)

@pytest.mark.xfail(reason="Scalar Function with name any does not exist!")
def test_query_substrait_22(require):
    execute(require,22)
