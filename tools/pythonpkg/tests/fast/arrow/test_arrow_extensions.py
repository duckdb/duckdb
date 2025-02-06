import duckdb
import pytest
import uuid
import json
from uuid import UUID
import datetime

pa = pytest.importorskip('pyarrow', '18.0.0')


class TestCanonicalExtensionTypes(object):

    def test_uuid(self):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("SET arrow_lossless_conversion = true")

        storage_array = pa.array([uuid.uuid4().bytes for _ in range(4)], pa.binary(16))
        storage_array = pa.uuid().wrap_array(storage_array)

        arrow_table = pa.Table.from_arrays([storage_array], names=['uuid_col'])

        duck_arrow = duckdb_cursor.execute('FROM arrow_table').arrow()

        assert duck_arrow.equals(arrow_table)

    def test_uuid_from_duck(self):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("SET arrow_lossless_conversion = true")

        arrow_table = duckdb_cursor.execute("select uuid from test_all_types()").fetch_arrow_table()

        assert arrow_table.to_pylist() == [
            {'uuid': UUID('00000000-0000-0000-0000-000000000000')},
            {'uuid': UUID('ffffffff-ffff-ffff-ffff-ffffffffffff')},
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

        assert arrow_table.to_pylist() == [{'uuid': UUID('00000000-0000-0000-0000-000000000100')}]
        assert duckdb_cursor.execute("FROM arrow_table").fetchall() == [(UUID('00000000-0000-0000-0000-000000000100'),)]

    def test_json(self, duckdb_cursor):
        data = {"name": "Pedro", "age": 28, "car": "VW Fox"}

        # Convert dictionary to JSON string
        json_string = json.dumps(data)

        storage_array = pa.array([json_string], pa.string())

        arrow_table = pa.Table.from_arrays([storage_array], names=['json_col'])

        duckdb_cursor.execute("SET arrow_lossless_conversion = true")
        duck_arrow = duckdb_cursor.execute('FROM arrow_table').arrow()

        assert duck_arrow.equals(arrow_table)

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

    def test_uuid_udf_registered(self):
        def test_function(x):
            print(x.type.__class__)
            return x

        con = duckdb.connect()
        con.create_function('test', test_function, ['UUID'], 'UUID', type='arrow')

        rel = con.sql("select ? as x", params=[uuid.UUID('ffffffff-ffff-ffff-ffff-ffffffffffff')])
        rel.project("test(x) from t").fetchall()

    def test_unimplemented_extension(self, duckdb_cursor):
        class MyType(pa.ExtensionType):
            def __init__(self):
                pa.ExtensionType.__init__(self, pa.binary(5), "pedro.binary")

            def __arrow_ext_serialize__(self):
                return b''

            @classmethod
            def __arrow_ext_deserialize__(cls, storage_type, serialized):
                return UuidTypeWrong()

        storage_array = pa.array(['pedro'], pa.binary(5))
        my_type = MyType()
        storage_array = my_type.wrap_array(storage_array)

        age_array = pa.array([29], pa.int32())

        arrow_table = pa.Table.from_arrays([storage_array, age_array], names=['pedro_pedro_pedro', 'age'])

        duck_arrow = duckdb_cursor.execute('FROM arrow_table').arrow()
        assert duckdb_cursor.execute('FROM duck_arrow').fetchall() == [(b'pedro', 29)]

    def test_hugeint(self):
        con = duckdb.connect()

        con.execute("SET arrow_lossless_conversion = true")

        storage_array = pa.array([b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff'], pa.binary(16))
        hugeint_type = pa.opaque(pa.binary(16), "hugeint", "DuckDB")

        storage_array = hugeint_type.wrap_array(storage_array)

        arrow_table = pa.Table.from_arrays([storage_array], names=['numbers'])

        assert con.execute('FROM arrow_table').fetchall() == [(-1,)]

        assert con.execute('FROM arrow_table').arrow().equals(arrow_table)

        con.execute("SET arrow_lossless_conversion = false")

        assert not con.execute('FROM arrow_table').arrow().equals(arrow_table)

    def test_uhugeint(self, duckdb_cursor):
        storage_array = pa.array([b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff'], pa.binary(16))
        uhugeint_type = pa.opaque(pa.binary(16), "uhugeint", "DuckDB")
        storage_array = uhugeint_type.wrap_array(storage_array)

        arrow_table = pa.Table.from_arrays([storage_array], names=['numbers'])

        assert duckdb_cursor.execute('FROM arrow_table').fetchall() == [(340282366920938463463374607431768211455,)]

    def test_bit(self):
        con = duckdb.connect()

        res_blob = con.execute("SELECT '0101011'::BIT str FROM range(5) tbl(i)").arrow()

        con.execute("SET arrow_lossless_conversion = true")

        res_bit = con.execute("SELECT '0101011'::BIT str FROM range(5) tbl(i)").arrow()

        assert con.execute("FROM res_blob").fetchall() == [
            (b'\x01\xab',),
            (b'\x01\xab',),
            (b'\x01\xab',),
            (b'\x01\xab',),
            (b'\x01\xab',),
        ]
        assert con.execute("FROM res_bit").fetchall() == [
            ('0101011',),
            ('0101011',),
            ('0101011',),
            ('0101011',),
            ('0101011',),
        ]

    def test_timetz(self):
        con = duckdb.connect()

        res_time = con.execute("SELECT '02:30:00+04'::TIMETZ str FROM range(1) tbl(i)").arrow()

        con.execute("SET arrow_lossless_conversion = true")

        res_tz = con.execute("SELECT '02:30:00+04'::TIMETZ str FROM range(1) tbl(i)").arrow()

        assert con.execute("FROM res_time").fetchall() == [(datetime.time(2, 30),)]
        assert con.execute("FROM res_tz").fetchall() == [
            (datetime.time(2, 30, tzinfo=datetime.timezone(datetime.timedelta(seconds=14400))),)
        ]

    def test_varint(self):
        con = duckdb.connect()
        res_varint = con.execute(
            "SELECT '179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368'::varint a FROM range(1) tbl(i)"
        ).arrow()
        assert res_varint.column("a").type.type_name == 'varint'
        assert res_varint.column("a").type.vendor_name == "DuckDB"

        assert con.execute("FROM res_varint").fetchall() == [
            (
                '179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368',
            )
        ]

    def test_nested_types_with_extensions(self):
        duckdb_cursor = duckdb.connect()
        duckdb_cursor.execute("SET arrow_lossless_conversion = true")

        arrow_table = duckdb_cursor.execute("select map {uuid(): 1::uhugeint, uuid(): 2::uhugeint} as li").arrow()

        assert arrow_table.schema[0].type.key_type.extension_name == "arrow.uuid"
        assert arrow_table.schema[0].type.item_type.extension_name == "arrow.opaque"
        assert arrow_table.schema[0].type.item_type.type_name == "uhugeint"
        assert arrow_table.schema[0].type.item_type.vendor_name == "DuckDB"

    def test_extension_dictionary(self, duckdb_cursor):
        indices = pa.array([0, 1, 0, 1, 2, 1, 0, 2])
        dictionary = pa.array(
            [
                b'\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff',
                b'\x01\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff',
                b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff',
            ],
            pa.binary(16),
        )
        uhugeint_type = pa.opaque(pa.binary(16), "uhugeint", "DuckDB")
        dictionary = uhugeint_type.wrap_array(dictionary)

        dict_array = pa.DictionaryArray.from_arrays(indices, dictionary)
        arrow_table = pa.Table.from_arrays([dict_array], ['a'])
        rel = duckdb_cursor.from_arrow(arrow_table)
        assert rel.execute().fetchall() == [
            (340282366920938463463374607431768211200,),
            (340282366920938463463374607431768211201,),
            (340282366920938463463374607431768211200,),
            (340282366920938463463374607431768211201,),
            (340282366920938463463374607431768211455,),
            (340282366920938463463374607431768211201,),
            (340282366920938463463374607431768211200,),
            (340282366920938463463374607431768211455,),
        ]

    def test_boolean(self):
        con = duckdb.connect()
        con.execute("SET arrow_lossless_conversion = true")
        storage_array = pa.array([-1, 0, 1, 2, None], pa.int8())
        bool8_array = pa.ExtensionArray.from_storage(pa.bool8(), storage_array)
        arrow_table = pa.Table.from_arrays([bool8_array], names=['bool8'])
        assert con.execute('FROM arrow_table').fetchall() == [(True,), (False,), (True,), (True,), (None,)]
        result_table = con.execute('FROM arrow_table').arrow()

        res_storage_array = pa.array([1, 0, 1, 1, None], pa.int8())
        res_bool8_array = pa.ExtensionArray.from_storage(pa.bool8(), res_storage_array)
        res_arrow_table = pa.Table.from_arrays([res_bool8_array], names=['bool8'])

        assert result_table.equals(res_arrow_table)
