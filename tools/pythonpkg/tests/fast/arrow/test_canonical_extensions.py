import duckdb
import pytest
import uuid
import json
from uuid import UUID
import datetime

pa = pytest.importorskip('pyarrow', '18.0.0')

from arrow_canonical_extensions import UHugeIntType, HugeIntType, VarIntType


"""
    These fixtures make sure that the extension_type is registered at the start of the function,
    and unregistered at the end.
    
    No matter if an error occurred or the function ended early for whatever reason
"""


@pytest.fixture(scope='function')
def arrow_duckdb_hugeint():
    pa.register_extension_type(HugeIntType())
    yield
    pa.unregister_extension_type("duckdb.hugeint")


@pytest.fixture(scope='function')
def arrow_duckdb_uhugeint():
    pa.register_extension_type(UHugeIntType())
    yield
    pa.unregister_extension_type("duckdb.uhugeint")


@pytest.fixture(scope='function')
def arrow_duckdb_varint():
    pa.register_extension_type(VarIntType())
    yield
    pa.unregister_extension_type("duckdb.varint")


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

        with pytest.raises(duckdb.NotImplementedException, match=" Arrow Type with extension name: pedro.binary"):
            duck_arrow = duckdb_cursor.execute('FROM arrow_table').arrow()
        duck_res = duckdb_cursor.execute('SELECT age FROM arrow_table').fetchall()
        # This works because we project ze unknown extension array
        assert duck_res == [(29,)]

    def test_hugeint(self, arrow_duckdb_hugeint):
        con = duckdb.connect()

        con.execute("SET arrow_lossless_conversion = true")

        storage_array = pa.array([b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff'], pa.binary(16))
        hugeint_type = HugeIntType()
        storage_array = hugeint_type.wrap_array(storage_array)

        arrow_table = pa.Table.from_arrays([storage_array], names=['numbers'])

        assert con.execute('FROM arrow_table').fetchall() == [(-1,)]

        assert con.execute('FROM arrow_table').arrow().equals(arrow_table)

        con.execute("SET arrow_lossless_conversion = false")

        assert not con.execute('FROM arrow_table').arrow().equals(arrow_table)

    def test_uhugeint(self, duckdb_cursor, arrow_duckdb_uhugeint):
        storage_array = pa.array([b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff'], pa.binary(16))
        uhugeint_type = UHugeIntType()
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

    def test_varint(self, arrow_duckdb_varint):
        con = duckdb.connect()
        res_varint = con.execute(
            "SELECT '179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368'::varint a FROM range(1) tbl(i)"
        ).arrow()

        assert res_varint.column("a").type == VarIntType()

        assert con.execute("FROM res_varint").fetchall() == [
            (
                '179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368',
            )
        ]
