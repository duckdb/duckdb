import duckdb
import pytest
import uuid
import json
from uuid import UUID
import datetime

pa = pytest.importorskip('pyarrow')

from arrow_canonical_extensions import UuidType, JSONType, UHugeIntType, HugeIntType


class TestCanonicalExtensionTypes(object):

    def test_uuid(self):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("SET arrow_lossless_conversion = true")

        pa.register_extension_type(UuidType())

        storage_array = pa.array([uuid.uuid4().bytes for _ in range(4)], pa.binary(16))
        uuid_type = UuidType()
        storage_array = uuid_type.wrap_array(storage_array)

        arrow_table = pa.Table.from_arrays([storage_array], names=['uuid_col'])

        duck_arrow = duckdb_cursor.execute('FROM arrow_table').arrow()

        assert duck_arrow.equals(arrow_table)

        pa.unregister_extension_type("arrow.uuid")

    def test_uuid_from_duck(self):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("SET arrow_lossless_conversion = true")

        pa.register_extension_type(UuidType())

        arrow_table = duckdb_cursor.execute("select uuid from test_all_types()").fetch_arrow_table()

        assert arrow_table.to_pylist() == [
            {'uuid': b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'},
            {'uuid': b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff'},
            {'uuid': None},
        ]

        assert duckdb_cursor.execute("FROM arrow_table").fetchall() == [
            (UUID('00000000-0000-0000-0000-000000000000'),),
            (UUID('ffffffff-ffff-ffff-ffff-ffffffffffff'),),
            (None,),
        ]

        arrow_table = duckdb_cursor.execute(
            "select '00000000-0000-0000-0000-000000000100'::UUID as uuid"
        ).fetch_arrow_table()

        assert arrow_table.to_pylist() == [
            {'uuid': b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00'}
        ]
        assert duckdb_cursor.execute("FROM arrow_table").fetchall() == [(UUID('00000000-0000-0000-0000-000000000100'),)]

        pa.unregister_extension_type("arrow.uuid")

    def test_uuid_exception(self):
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

        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("SET arrow_lossless_conversion = true")

        pa.register_extension_type(UuidTypeWrong())

        storage_array = pa.array(['aaaa'], pa.binary(4))
        uuid_type = UuidTypeWrong()
        storage_array = uuid_type.wrap_array(storage_array)

        arrow_table = pa.Table.from_arrays([storage_array], names=['uuid_col'])

        with pytest.raises(duckdb.InvalidInputException, match="arrow.uuid must be a fixed-size binary of 16 bytes"):
            duck_arrow = duckdb_cursor.execute('FROM arrow_table').arrow()

        pa.unregister_extension_type("arrow.uuid")

    def test_json(self, duckdb_cursor):
        pa.register_extension_type(JSONType())

        data = {"name": "Pedro", "age": 28, "car": "VW Fox"}

        # Convert dictionary to JSON string
        json_string = json.dumps(data)

        storage_array = pa.array([json_string], pa.string())
        json_type = JSONType()
        storage_array = json_type.wrap_array(storage_array)

        arrow_table = pa.Table.from_arrays([storage_array], names=['json_col'])

        duck_arrow = duckdb_cursor.execute('FROM arrow_table').arrow()

        assert duck_arrow.equals(arrow_table)

        pa.unregister_extension_type("arrow.json")

    def test_json_throw(self, duckdb_cursor):
        class JSONTypeWrong(pa.ExtensionType):
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
                return JSONTypeWrong()

        pa.register_extension_type(JSONTypeWrong())

        storage_array = pa.array([32], pa.int32())
        json_type = JSONTypeWrong()
        storage_array = json_type.wrap_array(storage_array)

        arrow_table = pa.Table.from_arrays([storage_array], names=['json_col'])

        with pytest.raises(duckdb.InvalidInputException, match="arrow.json must be of a varchar format "):
            duck_arrow = duckdb_cursor.execute('FROM arrow_table').arrow()
        pa.unregister_extension_type("arrow.json")

    def test_uuid_no_def(self):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("SET arrow_lossless_conversion = true")

        res_arrow = duckdb_cursor.execute("select uuid from test_all_types()").arrow()
        res_duck = duckdb_cursor.execute("from res_arrow").fetchall()
        assert res_duck == [
            (UUID('00000000-0000-0000-0000-000000000000'),),
            (UUID('ffffffff-ffff-ffff-ffff-ffffffffffff'),),
            (None,),
        ]

    def test_uuid_no_def_lossless(self):
        duckdb_cursor = duckdb.connect()
        res_arrow = duckdb_cursor.execute("select uuid from test_all_types()").arrow()
        assert res_arrow.to_pylist() == [
            {'uuid': '00000000-0000-0000-0000-000000000000'},
            {'uuid': 'ffffffff-ffff-ffff-ffff-ffffffffffff'},
            {'uuid': None},
        ]

        res_duck = duckdb_cursor.execute("from res_arrow").fetchall()
        assert res_duck == [
            ('00000000-0000-0000-0000-000000000000',),
            ('ffffffff-ffff-ffff-ffff-ffffffffffff',),
            (None,),
        ]

    def test_uuid_no_def_stream(self):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("SET arrow_lossless_conversion = true")

        res_arrow = duckdb_cursor.execute("select uuid from test_all_types()").fetch_record_batch()
        res_duck = duckdb.execute("from res_arrow").fetchall()
        assert res_duck == [
            (UUID('00000000-0000-0000-0000-000000000000'),),
            (UUID('ffffffff-ffff-ffff-ffff-ffffffffffff'),),
            (None,),
        ]

    def test_uuid_udf_registered(self, duckdb_cursor):
        pa.register_extension_type(UuidType())

        def test_function(x):
            print(x.type.__class__)
            return x

        con = duckdb.connect()
        con.create_function('test', test_function, ['UUID'], 'UUID', type='arrow')

        rel = con.sql("select ? as x", params=[uuid.UUID('ffffffff-ffff-ffff-ffff-ffffffffffff')])
        rel.project("test(x) from t").fetchall()

        pa.unregister_extension_type("arrow.uuid")

    def test_uuid_udf_unregistered(self):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("SET arrow_lossless_conversion = true")

        def test_function(x):
            print(x.type.__class__)
            return x

        duckdb_cursor.create_function('test', test_function, ['UUID'], 'UUID', type='arrow')

        rel = duckdb_cursor.sql("select ? as x", params=[uuid.UUID('ffffffff-ffff-ffff-ffff-ffffffffffff')])
        with pytest.raises(duckdb.Error, match="It seems that you are using the UUID arrow canonical extension"):
            rel.project("test(x) from t").fetchall()

    def test_unimplemented_extension(self, duckdb_cursor):
        class MyType(pa.ExtensionType):
            def __init__(self):
                pa.ExtensionType.__init__(self, pa.binary(5), "pedro.binary")

            def __arrow_ext_serialize__(self):
                return b''

            @classmethod
            def __arrow_ext_deserialize__(self, storage_type, serialized):
                return UuidTypeWrong()

                pa.register_extension_type(UuidType())

        storage_array = pa.array(['pedro'], pa.binary(5))
        my_type = MyType()
        storage_array = my_type.wrap_array(storage_array)

        arrow_table = pa.Table.from_arrays([storage_array], names=['pedro_pedro_pedro'])

        with pytest.raises(duckdb.NotImplementedException, match=" Arrow Type with extension name: pedro.binary"):
            duck_arrow = duckdb_cursor.execute('FROM arrow_table').arrow()

    def test_hugeint(self):
        duckdb_cursor = duckdb.connect()

        duckdb_cursor.execute("SET arrow_lossless_conversion = true")

        pa.register_extension_type(HugeIntType())

        storage_array = pa.array([b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff'], pa.binary(16))
        hugeint_type = HugeIntType()
        storage_array = hugeint_type.wrap_array(storage_array)

        arrow_table = pa.Table.from_arrays([storage_array], names=['numbers'])

        assert duckdb_cursor.execute('FROM arrow_table').fetchall() == [(-1,)]

        assert duckdb_cursor.execute('FROM arrow_table').arrow().equals(arrow_table)

        duckdb_cursor.execute("SET arrow_lossless_conversion = false")

        assert not duckdb_cursor.execute('FROM arrow_table').arrow().equals(arrow_table)

        pa.unregister_extension_type("duckdb.hugeint")

    def test_uhugeint(self, duckdb_cursor):
        pa.register_extension_type(UHugeIntType())

        storage_array = pa.array([b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff'], pa.binary(16))
        uhugeint_type = UHugeIntType()
        storage_array = uhugeint_type.wrap_array(storage_array)

        arrow_table = pa.Table.from_arrays([storage_array], names=['numbers'])

        assert duckdb_cursor.execute('FROM arrow_table').fetchall() == [(340282366920938463463374607431768211455,)]

        pa.unregister_extension_type("duckdb.uhugeint")

    def test_bit(self):
        duckdb_cursor = duckdb.connect()

        res_blob = duckdb_cursor.execute("SELECT '0101011'::BIT str FROM range(5) tbl(i)").arrow()

        duckdb_cursor.execute("SET arrow_lossless_conversion = true")

        res_bit = duckdb_cursor.execute("SELECT '0101011'::BIT str FROM range(5) tbl(i)").arrow()

        assert duckdb_cursor.execute("FROM res_blob").fetchall() == [
            (b'\x01\xab',),
            (b'\x01\xab',),
            (b'\x01\xab',),
            (b'\x01\xab',),
            (b'\x01\xab',),
        ]
        assert duckdb_cursor.execute("FROM res_bit").fetchall() == [
            ('0101011',),
            ('0101011',),
            ('0101011',),
            ('0101011',),
            ('0101011',),
        ]

    def test_timetz(self):
        duckdb_cursor = duckdb.connect()

        res_time = duckdb_cursor.execute("SELECT '02:30:00+04'::TIMETZ str FROM range(1) tbl(i)").arrow()

        duckdb_cursor.execute("SET arrow_lossless_conversion = true")

        res_tz = duckdb_cursor.execute("SELECT '02:30:00+04'::TIMETZ str FROM range(1) tbl(i)").arrow()

        assert duckdb_cursor.execute("FROM res_time").fetchall() == [(datetime.time(2, 30),)]
        assert duckdb_cursor.execute("FROM res_tz").fetchall() == [
            (datetime.time(2, 30, tzinfo=datetime.timezone(datetime.timedelta(seconds=14400))),)
        ]
