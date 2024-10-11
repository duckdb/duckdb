import duckdb
import os
import pandas as pd
import pytest
from typing import Union, Optional
import sys

from duckdb.typing import (
    SQLNULL,
    BOOLEAN,
    TINYINT,
    UTINYINT,
    SMALLINT,
    USMALLINT,
    INTEGER,
    UINTEGER,
    BIGINT,
    UBIGINT,
    HUGEINT,
    UHUGEINT,
    UUID,
    FLOAT,
    DOUBLE,
    DATE,
    TIMESTAMP,
    TIMESTAMP_MS,
    TIMESTAMP_NS,
    TIMESTAMP_S,
    DuckDBPyType,
    TIME,
    TIME_TZ,
    TIMESTAMP_TZ,
    VARCHAR,
    BLOB,
    BIT,
    INTERVAL,
)
import duckdb.typing


class TestType(object):
    def test_sqltype(self):
        assert str(duckdb.sqltype('struct(a VARCHAR, b BIGINT)')) == 'STRUCT(a VARCHAR, b BIGINT)'
        # todo: add tests with invalid type_str

    def test_primitive_types(self):
        assert str(SQLNULL) == '"NULL"'
        assert str(BOOLEAN) == 'BOOLEAN'
        assert str(TINYINT) == 'TINYINT'
        assert str(UTINYINT) == 'UTINYINT'
        assert str(SMALLINT) == 'SMALLINT'
        assert str(USMALLINT) == 'USMALLINT'
        assert str(INTEGER) == 'INTEGER'
        assert str(UINTEGER) == 'UINTEGER'
        assert str(BIGINT) == 'BIGINT'
        assert str(UBIGINT) == 'UBIGINT'
        assert str(HUGEINT) == 'HUGEINT'
        assert str(UHUGEINT) == 'UHUGEINT'
        assert str(UUID) == 'UUID'
        assert str(FLOAT) == 'FLOAT'
        assert str(DOUBLE) == 'DOUBLE'
        assert str(DATE) == 'DATE'
        assert str(TIMESTAMP) == 'TIMESTAMP'
        assert str(TIMESTAMP_MS) == 'TIMESTAMP_MS'
        assert str(TIMESTAMP_NS) == 'TIMESTAMP_NS'
        assert str(TIMESTAMP_S) == 'TIMESTAMP_S'
        assert str(TIME) == 'TIME'
        assert str(TIME_TZ) == 'TIME WITH TIME ZONE'
        assert str(TIMESTAMP_TZ) == 'TIMESTAMP WITH TIME ZONE'
        assert str(VARCHAR) == 'VARCHAR'
        assert str(BLOB) == 'BLOB'
        assert str(BIT) == 'BIT'
        assert str(INTERVAL) == 'INTERVAL'

    def test_list_type(self):
        type = duckdb.list_type(BIGINT)
        assert str(type) == 'BIGINT[]'

    def test_array_type(self):
        type = duckdb.array_type(BIGINT, 3)
        assert str(type) == 'BIGINT[3]'

    def test_struct_type(self):
        type = duckdb.struct_type({'a': BIGINT, 'b': BOOLEAN})
        assert str(type) == 'STRUCT(a BIGINT, b BOOLEAN)'

        # FIXME: create an unnamed struct when fields are provided as a list
        type = duckdb.struct_type([BIGINT, BOOLEAN])
        assert str(type) == 'STRUCT(v1 BIGINT, v2 BOOLEAN)'

    def test_incomplete_struct_type(self):
        with pytest.raises(
            duckdb.InvalidInputException, match='Could not convert empty dictionary to a duckdb STRUCT type'
        ):
            type = duckdb.typing.DuckDBPyType(dict())

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
        type = duckdb.union_type([BIGINT, VARCHAR, TINYINT])
        assert str(type) == 'UNION(v1 BIGINT, v2 VARCHAR, v3 TINYINT)'

        type = duckdb.union_type({'a': BIGINT, 'b': VARCHAR, 'c': TINYINT})
        assert str(type) == 'UNION(a BIGINT, b VARCHAR, c TINYINT)'

    import sys

    @pytest.mark.skipif(sys.version_info < (3, 9), reason="requires >= python3.9")
    def test_implicit_convert_from_builtin_type(self):
        type = duckdb.list_type(list[str])
        assert str(type.child) == "VARCHAR[]"

        mapping = {'VARCHAR': str, 'BIGINT': int, 'BLOB': bytes, 'BLOB': bytearray, 'BOOLEAN': bool, 'DOUBLE': float}
        for expected, type in mapping.items():
            res = duckdb.list_type(type)
            assert str(res.child) == expected

        res = duckdb.list_type({'a': str, 'b': int})
        assert str(res.child) == 'STRUCT(a VARCHAR, b BIGINT)'

        res = duckdb.list_type(dict[str, int])
        assert str(res.child) == 'MAP(VARCHAR, BIGINT)'

        res = duckdb.list_type(list[str])
        assert str(res.child) == 'VARCHAR[]'

        res = duckdb.list_type(list[dict[str, dict[list[str], str]]])
        assert str(res.child) == 'MAP(VARCHAR, MAP(VARCHAR[], VARCHAR))[]'

        res = duckdb.list_type(list[Union[str, int]])
        assert str(res.child) == 'UNION(u1 VARCHAR, u2 BIGINT)[]'

    def test_implicit_convert_from_numpy(self, duckdb_cursor):
        np = pytest.importorskip("numpy")

        type_mapping = {
            'bool': 'BOOLEAN',
            'int8': 'TINYINT',
            'uint8': 'UTINYINT',
            'int16': 'SMALLINT',
            'uint16': 'USMALLINT',
            'int32': 'INTEGER',
            'uint32': 'UINTEGER',
            'int64': 'BIGINT',
            'uint64': 'UBIGINT',
            'float16': 'FLOAT',
            'float32': 'FLOAT',
            'float64': 'DOUBLE',
        }

        builtins = []
        builtins += [np.bool_]
        builtins += [np.byte]
        builtins += [np.ubyte]
        builtins += [np.short]
        builtins += [np.ushort]
        builtins += [np.intc]
        builtins += [np.uintc]
        builtins += [np.int_]
        builtins += [np.uint]
        builtins += [np.longlong]
        builtins += [np.ulonglong]
        builtins += [np.half]
        builtins += [np.float16]
        builtins += [np.single]
        builtins += [np.double]

        for builtin in builtins:
            print(builtin)
            type = duckdb_cursor.list_type(builtin)
            dtype_str = str(builtin().dtype)
            duckdb_type_str = str(type.child)
            assert type_mapping[dtype_str] == duckdb_type_str

    def test_attribute_accessor(self):
        type = duckdb.row_type([BIGINT, duckdb.list_type(duckdb.map_type(BLOB, BIT))])
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

    def test_json_type(self):
        json_type = duckdb.type('JSON')

        val = duckdb.Value('{"duck": 42}', json_type)
        res = duckdb.execute("select typeof($1)", [val]).fetchone()
        assert res == ('JSON',)

    # NOTE: we can support this, but I don't think going through hoops for an outdated version of python is worth it
    @pytest.mark.skipif(sys.version_info < (3, 9), reason="python3.7 does not store Optional[..] in a recognized way")
    def test_optional(self):
        type = duckdb.typing.DuckDBPyType(Optional[str])
        assert type == 'VARCHAR'
        type = duckdb.typing.DuckDBPyType(Optional[Union[int, bool]])
        assert type == 'UNION(u1 BIGINT, u2 BOOLEAN)'
        type = duckdb.typing.DuckDBPyType(Optional[list[int]])
        assert type == 'BIGINT[]'
        type = duckdb.typing.DuckDBPyType(Optional[dict[int, str]])
        assert type == 'MAP(BIGINT, VARCHAR)'
        type = duckdb.typing.DuckDBPyType(Optional[dict[Optional[int], Optional[str]]])
        assert type == 'MAP(BIGINT, VARCHAR)'
        type = duckdb.typing.DuckDBPyType(Optional[dict[Optional[int], Optional[str]]])
        assert type == 'MAP(BIGINT, VARCHAR)'
        type = duckdb.typing.DuckDBPyType(Optional[Union[Optional[str], Optional[bool]]])
        assert type == 'UNION(u1 VARCHAR, u2 BOOLEAN)'
        type = duckdb.typing.DuckDBPyType(Union[str, None])
        assert type == 'VARCHAR'

    @pytest.mark.skipif(sys.version_info < (3, 10), reason="'str | None' syntax requires Python 3.10 or higher")
    def test_optional_310(self):
        type = duckdb.typing.DuckDBPyType(str | None)
        assert type == 'VARCHAR'

    def test_children_attribute(self):
        assert DuckDBPyType('INTEGER[]').children == [('child', DuckDBPyType('INTEGER'))]
        assert DuckDBPyType('INTEGER[2]').children == [('child', DuckDBPyType('INTEGER')), ('size', 2)]
        assert DuckDBPyType('INTEGER[2][3]').children == [('child', DuckDBPyType('INTEGER[2]')), ('size', 3)]
        assert DuckDBPyType("ENUM('a', 'b', 'c')").children == [('values', ['a', 'b', 'c'])]
