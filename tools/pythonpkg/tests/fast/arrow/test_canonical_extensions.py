import duckdb
import pytest
import uuid
import json

pa = pytest.importorskip('pyarrow')


class TestCanonicalExtensionTypes(object):

    def test_uuid(self, duckdb_cursor):
        class UuidType(pa.ExtensionType):
            def __init__(self):
                pa.ExtensionType.__init__(self, pa.binary(16), "arrow.uuid")

            def __arrow_ext_serialize__(self):
                # since we don't have a parameterized type, we don't need extra
                # metadata to be deserialized
                return b''

            @classmethod
            def __arrow_ext_deserialize__(self, storage_type, serialized):
                # return an instance of this subclass given the serialized
                # metadata.
                return UuidType()

        pa.register_extension_type(UuidType())

        storage_array = pa.array([uuid.uuid4().bytes for _ in range(4)], pa.binary(16))
        uuid_type = UuidType()
        storage_array = uuid_type.wrap_array(storage_array)

        arrow_table = pa.Table.from_arrays([storage_array], names=['uuid_col'])

        duck_arrow = duckdb_cursor.execute('FROM arrow_table').arrow()

        assert duck_arrow.equals(arrow_table)

        pa.unregister_extension_type("arrow.uuid")

    def test_uuid_exception(self, duckdb_cursor):
        class UuidTypeWrong(pa.ExtensionType):
            def __init__(self):
                pa.ExtensionType.__init__(self, pa.binary(4), "arrow.uuid")

            def __arrow_ext_serialize__(self):
                # since we don't have a parameterized type, we don't need extra
                # metadata to be deserialized
                return b''

            @classmethod
            def __arrow_ext_deserialize__(self, storage_type, serialized):
                # return an instance of this subclass given the serialized
                # metadata.
                return UuidTypeWrong()

        pa.register_extension_type(UuidTypeWrong())

        storage_array = pa.array(['aaaa'], pa.binary(4))
        uuid_type = UuidTypeWrong()
        storage_array = uuid_type.wrap_array(storage_array)

        arrow_table = pa.Table.from_arrays([storage_array], names=['uuid_col'])

        with pytest.raises(duckdb.InvalidInputException, match="arrow.uuid must be a fixed-size binary of 16 bytes"):
            duck_arrow = duckdb_cursor.execute('FROM arrow_table').arrow()

    def test_json(self, duckdb_cursor):
        class JSONType(pa.ExtensionType):
            def __init__(self):
                pa.ExtensionType.__init__(self, pa.string(), "arrow.json")

            def __arrow_ext_serialize__(self):
                # since we don't have a parameterized type, we don't need extra
                # metadata to be deserialized
                return b''

            @classmethod
            def __arrow_ext_deserialize__(self, storage_type, serialized):
                # return an instance of this subclass given the serialized
                # metadata.
                return JSONType()

        pa.register_extension_type(JSONType())

        data = {"name": "Pedro", "age": 28, "car": "VW Fox"}

        # Convert dictionary to JSON string
        json_string = json.dumps(data)

        storage_array = pa.array([json_string], pa.string())
        uuid_type = JSONType()
        storage_array = uuid_type.wrap_array(storage_array)

        arrow_table = pa.Table.from_arrays([storage_array], names=['json_col'])

        duck_arrow = duckdb_cursor.execute('FROM arrow_table').arrow()

        assert duck_arrow.equals(arrow_table)

        pa.unregister_extension_type("arrow.json")

    def test_json_throw(self, duckdb_cursor):
        class JSONType(pa.ExtensionType):
            def __init__(self):
                pa.ExtensionType.__init__(self, pa.int32(), "arrow.json")

            def __arrow_ext_serialize__(self):
                # since we don't have a parameterized type, we don't need extra
                # metadata to be deserialized
                return b''

            @classmethod
            def __arrow_ext_deserialize__(self, storage_type, serialized):
                # return an instance of this subclass given the serialized
                # metadata.
                return JSONType()

        pa.register_extension_type(JSONType())

        storage_array = pa.array([32], pa.int32())
        uuid_type = JSONType()
        storage_array = uuid_type.wrap_array(storage_array)

        arrow_table = pa.Table.from_arrays([storage_array], names=['json_col'])

        with pytest.raises(duckdb.InvalidInputException, match="arrow.json must be of a varchar format "):
            duck_arrow = duckdb_cursor.execute('FROM arrow_table').arrow()
