import sys

import pytest
pa = pytest.importorskip("pyarrow")
adbc_driver_manager = pytest.importorskip("adbc_driver_manager")

try:
    adbc_driver_duckdb = pytest.importorskip("adbc_driver_duckdb.dbapi")
    con = adbc_driver_duckdb.connect()
except:
    pytest.skip("'duckdb_adbc_init' was not exported in this install, try running 'python3 setup.py install'.", allow_module_level=True)

def _import(handle):
    """Helper to import a C Data Interface handle."""
    if isinstance(handle, adbc_driver_manager.ArrowArrayStreamHandle):
        return pa.RecordBatchReader._import_from_c(handle.address)
    elif isinstance(handle, adbc_driver_manager.ArrowSchemaHandle):
        return pa.Schema._import_from_c(handle.address)
    raise NotImplementedError(f"Importing {handle!r}")

def _bind(stmt, batch):
    array = adbc_driver_manager.ArrowArrayHandle()
    schema = adbc_driver_manager.ArrowSchemaHandle()
    batch._export_to_c(array.address, schema.address)
    stmt.bind(array, schema)

class TestADBCStatementBind(object):
    def test_statement_bind(self):
        
        # We don't support preparing multiple rows yet
        expected_result = pa.array([
            8
        ], type=pa.int64())

        data = pa.record_batch(
            [
                [1, 2, 3, 4],
            ],
            names=["ints"],
        )

        con = adbc_driver_duckdb.connect()
        with con.cursor() as cursor:
            statement = cursor.adbc_statement
            statement.set_sql_query("select ? * 2 as i")
            statement.prepare()
            _bind(statement, data)
            res, number_of_rows = statement.execute_query()
            print(number_of_rows)
            table = _import(res).read_all()
            print("table", table)

            result = table['i']
            assert result.num_chunks == 1
            result_values = result.chunk(0)
            assert result_values == expected_result

