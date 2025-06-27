import pytest

pa = pytest.importorskip("pyarrow")


class TestArrowFixedBinary(object):
    def test_arrow_fixed_binary(self, duckdb_cursor):
        ids = [
            None,
            b'\x66\x4d\xf4\xae\xb1\x5c\xb0\x4a\xdd\x5d\x1d\x54',
            b'\x66\x4d\xf4\xf0\xa3\xfc\xec\x5b\x26\x81\x4e\x1d',
        ]

        id_array = pa.array(ids, type=pa.binary(12))
        arrow_table = pa.Table.from_arrays([id_array], names=["id"])
        res = duckdb_cursor.sql(
            """
			SELECT lower(hex(id)) as id FROM arrow_table
		"""
        ).fetchall()
        assert res == [(None,), ('664df4aeb15cb04add5d1d54',), ('664df4f0a3fcec5b26814e1d',)]
