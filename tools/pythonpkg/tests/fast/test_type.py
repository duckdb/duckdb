import duckdb
import os
import pandas as pd
import pytest
from typing import Union

class TestRelation(object):
    def test_sqltype(self):
        assert str(duckdb.sqltype('struct(a VARCHAR, b BIGINT)')) == 'STRUCT(a VARCHAR, b BIGINT)'
        # todo: add tests with invalid type_str

    def test_primitive_types(self):
        assert str(duckdb.tinyint) == 'TINYINT'
        assert str(duckdb.smallint) == 'SMALLINT'
        assert str(duckdb.integer) == 'INTEGER'
        assert str(duckdb.bigint) == 'BIGINT'
        assert str(duckdb.sqlnull) == 'NULL'
        assert str(duckdb.boolean) == 'BOOLEAN'
        assert str(duckdb.tinyint) == 'TINYINT'
        assert str(duckdb.utinyint) == 'UTINYINT'
        assert str(duckdb.smallint) == 'SMALLINT'
        assert str(duckdb.usmallint) == 'USMALLINT'
        assert str(duckdb.integer) == 'INTEGER'
        assert str(duckdb.uinteger) == 'UINTEGER'
        assert str(duckdb.bigint) == 'BIGINT'
        assert str(duckdb.ubigint) == 'UBIGINT'
        assert str(duckdb.hugeint) == 'HUGEINT'
        assert str(duckdb.uuid) == 'UUID'
        assert str(duckdb.float) == 'FLOAT'
        assert str(duckdb.double) == 'DOUBLE'
        assert str(duckdb.date) == 'DATE'
        assert str(duckdb.timestamp) == 'TIMESTAMP'
        assert str(duckdb.timestamp_ms) == 'TIMESTAMP_MS'
        assert str(duckdb.timestamp_ns) == 'TIMESTAMP_NS'
        assert str(duckdb.timestamp_s) == 'TIMESTAMP_S'
        assert str(duckdb.time) == 'TIME'
        assert str(duckdb.time_tz) == 'TIME WITH TIME ZONE'
        assert str(duckdb.timestamp_tz) == 'TIMESTAMP WITH TIME ZONE'
        assert str(duckdb.varchar) == 'VARCHAR'
        assert str(duckdb.blob) == 'BLOB'
        assert str(duckdb.bit) == 'BIT'
        assert str(duckdb.interval) == 'INTERVAL'

    def test_array_type(self):
        type = duckdb.array_type(duckdb.bigint)
        assert str(type) == 'BIGINT[]'

    def test_struct_type(self):
        type = duckdb.struct_type({'a': duckdb.bigint, 'b': duckdb.boolean})
        assert str(type) == 'STRUCT(a BIGINT, b BOOLEAN)'

        type = duckdb.struct_type([duckdb.bigint, duckdb.boolean])
        assert str(type) == 'STRUCT(v1 BIGINT, v2 BOOLEAN)'

    def test_map_type(self):
        type = duckdb.map_type(duckdb.sqltype("BIGINT"), duckdb.sqltype("DECIMAL(10, 2)"))
        assert str(type) == 'MAP(BIGINT, DECIMAL(10,2))'

    def test_decimal_type(self):
        type = duckdb.decimal_type(5, 3)
        assert str(type) == 'DECIMAL(5,3)'

    def test_string_type(self):
        type = duckdb.string_type()
        assert str(type) == 'VARCHAR'

    def test_string_type_collation(self):
        type = duckdb.string_type('NOCASE')
        # collation does not show up in the string representation..
        assert str(type) == 'VARCHAR'

    def test_union_type(self):
        type = duckdb.union_type([duckdb.bigint, duckdb.varchar, duckdb.tinyint])
        assert str(type) == 'UNION(v1 BIGINT, v2 VARCHAR, v3 TINYINT)'

        type = duckdb.union_type({'a': duckdb.bigint, 'b': duckdb.varchar, 'c': duckdb.tinyint})
        assert str(type) == 'UNION(a BIGINT, b VARCHAR, c TINYINT)'
    
    import sys
    @pytest.mark.skipif(sys.version_info <= (3,7), reason="requires > python3.7")
    def test_implicit_convert_from_builtin_type(self):
        type = duckdb.list_type(list[str])
        assert str(type.child) == "VARCHAR[]"

        mapping = {
            'VARCHAR': str,
            'BIGINT': int,
            'BLOB': bytes,
            'BLOB': bytearray,
            'BOOLEAN': bool,
            'DOUBLE': float
        }
        for expected, type in mapping.items():
            res = duckdb.list_type(type)
            assert str(res.child) == expected
        
        res = duckdb.list_type(dict['a': str, 'b': int])
        assert str(res.child) == 'STRUCT(a VARCHAR, b BIGINT)'

        res = duckdb.list_type(dict[str, int])
        assert str(res.child) == 'MAP(VARCHAR, BIGINT)'

        res = duckdb.list_type(list[str])
        assert str(res.child) == 'VARCHAR[]'

        res = duckdb.list_type(list[dict[str, dict[list[str], str]]])
        assert str(res.child) == 'MAP(VARCHAR, MAP(VARCHAR[], VARCHAR))[]'

        res = duckdb.list_type(list[Union[str, int]])
        assert str(res.child) == 'UNION(u1 VARCHAR, u2 BIGINT)[]'

    def test_attribute_accessor(self):
        type = duckdb.row_type([duckdb.bigint, duckdb.list_type(duckdb.map_type(duckdb.blob, duckdb.bit))])
        assert hasattr(type, 'a') == False
        assert hasattr(type, 'v1') == True

        field_one = type['v1']
        assert str(field_one) == 'BIGINT'
        field_one = type.v1
        assert str(field_one) == 'BIGINT'

        field_two = type['v2']
        assert str(field_two) == 'MAP(BLOB, BIT)[]'

        child_type = type.v2.child
        assert str(child_type) == 'MAP(BLOB, BIT)'
