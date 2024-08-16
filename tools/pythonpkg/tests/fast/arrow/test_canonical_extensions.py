import duckdb
import pytest
import uuid

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
