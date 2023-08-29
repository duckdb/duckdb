import sys

import pytest

pa = pytest.importorskip("pyarrow")
adbc_driver_manager = pytest.importorskip("adbc_driver_manager")

try:
    adbc_driver_duckdb = pytest.importorskip("adbc_driver_duckdb.dbapi")
    con = adbc_driver_duckdb.connect()
except:
    pytest.skip(
        "'duckdb_adbc_init' was not exported in this install, try running 'python3 setup.py install'.",
        allow_module_level=True,
    )


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
    def test_bind_multiple_rows(self):
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
            with pytest.raises(
                adbc_driver_manager.NotSupportedError, match="Binding multiple rows at once is not supported yet"
            ):
                res, number_of_rows = statement.execute_query()

    def test_bind_single_row(self):
        expected_result = pa.array([8], type=pa.int64())

        data = pa.record_batch(
            [
                [4],
            ],
            names=["ints"],
        )

        con = adbc_driver_duckdb.connect()
        with con.cursor() as cursor:
            statement = cursor.adbc_statement
            statement.set_sql_query("select ? * 2 as i")
            statement.prepare()

            raw_schema = statement.get_parameter_schema()
            schema = _import(raw_schema)
            assert schema.names == ['0']

            _bind(statement, data)
            res, _ = statement.execute_query()
            table = _import(res).read_all()

            result = table['i']
            assert result.num_chunks == 1
            result_values = result.chunk(0)
            assert result_values == expected_result

    def test_multiple_parameters(self):
        int_data = pa.array([5])
        varchar_data = pa.array(['not a short string'])
        bool_data = pa.array([True])

        # Create the schema
        schema = pa.schema([('a', pa.int64()), ('b', pa.string()), ('c', pa.bool_())])

        # Create the PyArrow table
        expected_res = pa.Table.from_arrays([int_data, varchar_data, bool_data], schema=schema)

        data = pa.record_batch(
            [[5], ['not a short string'], [True]],
            names=["ints", "strings", "bools"],
        )

        con = adbc_driver_duckdb.connect()
        with con.cursor() as cursor:
            statement = cursor.adbc_statement
            statement.set_sql_query("select ? as a, ? as b, ? as c")
            statement.prepare()

            raw_schema = statement.get_parameter_schema()
            schema = _import(raw_schema)
            assert schema.names == ['0', '1', '2']

            _bind(statement, data)
            res, _ = statement.execute_query()
            table = _import(res).read_all()

            assert table == expected_res

    def test_bind_composite_type(self):
        data_dict = {
            'field1': pa.array([10], type=pa.int64()),
            'field2': pa.array([3.14], type=pa.float64()),
            'field3': pa.array(['example with long string'], type=pa.string()),
        }
        # Create the StructArray
        struct_array = pa.StructArray.from_arrays(arrays=data_dict.values(), names=data_dict.keys())

        schema = pa.schema([(name, array.type) for name, array in zip(['a'], [struct_array])])

        # Create the RecordBatch
        record_batch = pa.RecordBatch.from_arrays([struct_array], schema=schema)

        con = adbc_driver_duckdb.connect()
        with con.cursor() as cursor:
            statement = cursor.adbc_statement
            statement.set_sql_query("select ? as a")
            statement.prepare()

            raw_schema = statement.get_parameter_schema()
            schema = _import(raw_schema)
            assert schema.names == ['0']

            _bind(statement, record_batch)
            res, _ = statement.execute_query()
            table = _import(res).read_all()
            result = table['a']
            result = result.chunk(0)
            assert result == struct_array

    def test_too_many_parameters(self):
        data = pa.record_batch(
            [[12423], ['not a short string']],
            names=["ints", "strings"],
        )

        con = adbc_driver_duckdb.connect()
        with con.cursor() as cursor:
            statement = cursor.adbc_statement
            statement.set_sql_query("select ? as a")
            statement.prepare()

            raw_schema = statement.get_parameter_schema()
            schema = _import(raw_schema)
            assert schema.names == ['0']

            array = adbc_driver_manager.ArrowArrayHandle()
            schema = adbc_driver_manager.ArrowSchemaHandle()

            data._export_to_c(array.address, schema.address)
            statement.bind(array, schema)

            with pytest.raises(adbc_driver_manager.ProgrammingError, match="INVALID_ARGUMENT"):
                res, _ = statement.execute_query()

    def test_not_enough_parameters(self):
        data = pa.record_batch(
            [['not a short string']],
            names=["strings"],
        )

        con = adbc_driver_duckdb.connect()
        with con.cursor() as cursor:
            statement = cursor.adbc_statement
            statement.set_sql_query("select ? as a, ? as b")
            statement.prepare()

            raw_schema = statement.get_parameter_schema()
            schema = _import(raw_schema)
            assert schema.names == ['0', '1']

            array = adbc_driver_manager.ArrowArrayHandle()
            schema = adbc_driver_manager.ArrowSchemaHandle()
            data._export_to_c(array.address, schema.address)
            statement.bind(array, schema)
            with pytest.raises(
                adbc_driver_manager.ProgrammingError,
                match="Values were not provided for the following prepared statement parameters: 2",
            ):
                res, _ = statement.execute_query()
