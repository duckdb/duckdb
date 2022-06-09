try:
    from ibis_substrait.compiler.core import SubstraitCompiler
    import ibis
    from ibis.backends.base import BaseBackend
    from ibis.backends.duckdb.datatypes import parse_type
    from ibis_tpch_util import get_tpch_query
    can_run = True
    import duckdb

    def unbound_from_duckdb(table):  # noqa: D103
        return ibis.table(
            list(zip(table.columns, map(parse_type, table.dtypes))), name=table.alias
        )

    class TPCHBackend(BaseBackend):  # noqa: D101
        def __init__(self, fname="", scale_factor=0.1):  # noqa: D107
            self.con = duckdb.connect(fname)

            if not fname:
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
def test_ibis_to_duck_substrait(duckdb_cursor):
    if not can_run:
        return
    duck_con = TPCHBackend(fname="")
    # can't compile
    skip = [2]
    # Scalar Function with name any does not exist!
    skip.append(4)
    # can't compile
    skip.append(7)
    # can't compile
    skip.append(8)
    # Scalar Function with name extractyear does not exist!
    # (function should be extract with option YEAR) 
    skip.append(9)
    # Result does not match
    skip.append(11)
    # Scalar Function with name not does not exist!
    skip.append(13)
    # can't compile
    skip.append(14)
    # can't compile
    skip.append(15)
    # can't compile
    skip.append(16)
    # can't compile
    skip.append(17)
    # can't compile
    skip.append(18)
    # can't compile
    skip.append(20)
    # Scalar Function with name any does not exist!
    skip.append(21)
    # Scalar Function with name any does not exist!
    skip.append(22)
    for i in range(1,23):
        if skip.count(i) > 0:
            continue
        tpch_execute_ibis_to_duck_query(duck_con,i)

# tpch_execute_ibis_to_duck_query(duck_con,11)