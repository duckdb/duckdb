import duckdb
import pytest
import pandas as pd
import duckdb

pa = pytest.importorskip("pyarrow", '14.0.0', reason="Needs pyarrow >= 14")
pc = pytest.importorskip("pyarrow.compute")


class TestArrowREE(object):
    @pytest.mark.parametrize(
        'query',
        [
            """
            select
                CASE WHEN (i % 2 == 0) THEN NULL ELSE (i // {})::{} END as ree
            from range({}) t(i);
        """,
            """
            select
                (i // {})::{} as ree
            from range({}) t(i);
        """,
        ],
    )
    @pytest.mark.parametrize('run_length', [4, 1, 10, 1000, 2048, 3000])
    @pytest.mark.parametrize('size', [100, 10000])
    @pytest.mark.parametrize(
        'value_type',
        ['UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT', 'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT'],
    )
    def test_arrow_run_end_encoding(self, duckdb_cursor, query, run_length, size, value_type):
        if value_type == 'UTINYINT':
            if size > 255:
                size = 255
        if value_type == 'TINYINT':
            if size > 127:
                size = 127
        query = query.format(run_length, value_type, size)
        rel = duckdb_cursor.sql(query)
        array = rel.arrow()['ree']
        expected = rel.fetchall()

        encoded_array = pc.run_end_encode(array)

        schema = pa.schema([("ree", encoded_array.type)])
        tbl = pa.Table.from_arrays([encoded_array], schema=schema)
        res = duckdb_cursor.sql("select * from tbl").fetchall()
        assert res == expected

    @pytest.mark.parametrize(
        ['val1', 'val2'],
        [
            ('(-128)::TINYINT', '127::TINYINT'),
            ('(-32768)::SMALLINT', '32767::SMALLINT'),
            ('(-2147483648)::INTEGER', '2147483647::INTEGER'),
            ('(-9223372036854775808)::BIGINT', '9223372036854775807::BIGINT'),
            ('0::UTINYINT', '255::UTINYINT'),
            ('0::USMALLINT', '65535::USMALLINT'),
            ('0::UINTEGER', '4294967295::UINTEGER'),
            ('0::UBIGINT', '18446744073709551615::UBIGINT'),
            ('true::BOOL', 'false::BOOL'),
            ("'test'::VARCHAR", "'this is a long string'::VARCHAR"),
            ("'\\xE0\\x9F\\x98\\x84'::BLOB", "'\\xF0\\x9F\\xA6\\x86'::BLOB"),
            ("'1992-03-27'::DATE", "'2204-11-01'::DATE"),
            ("'01:02:03'::TIME", "'23:41:35'::TIME"),
            ("'1992-03-22 01:02:03'::TIMESTAMP_S", "'2022-11-07 08:43:04.123456'::TIMESTAMP_S"),
            ("'1992-03-22 01:02:03'::TIMESTAMP", "'2022-11-07 08:43:04.123456'::TIMESTAMP"),
            ("'1992-03-22 01:02:03'::TIMESTAMP_MS", "'2022-11-07 08:43:04.123456'::TIMESTAMP_MS"),
            ("'1992-03-22 01:02:03'::TIMESTAMP_NS", "'2022-11-07 08:43:04.123456'::TIMESTAMP_NS"),
            ("'12.23'::DECIMAL(4,2)", "'99.99'::DECIMAL(4,2)"),
            ("'1.234234'::DECIMAL(7,6)", "'0.000001'::DECIMAL(7,6)"),
            ("'134523.234234'::DECIMAL(14,7)", "'999999.000001'::DECIMAL(14,7)"),
            ("'12345678910111234123456789.1'::DECIMAL(28,1)", "'999999999999999999999999999.9'::DECIMAL(28,1)"),
            # ("'10acd298-15d7-417c-8b59-eabb5a2bacab'::UUID", "'eeccb8c5-9943-b2bb-bb5e-222f4e14b687'::UUID"),
            # ("'01010101010000'::BIT", "'01010100010101010101010101111111111'::BIT"), # FIXME: BIT seems broken?
        ],
    )
    @pytest.mark.parametrize('size', [3000])
    def test_arrow_run_end_encoding_coverage(self, duckdb_cursor, val1, val2, size):
        query = """
            select
                case when ((i // 8) % 2 == 0)
                    then (
                        case when ((i // 4) % 2 == 0)
                            then {}
                            else {}
                        end
                    ) else
                        NULL
                end as ree
            from range({}) t(i);
        """
        query = query.format(val1, val2, size)
        rel = duckdb_cursor.sql(query)
        array = rel.arrow()['ree']
        expected = rel.fetchall()

        encoded_array = pc.run_end_encode(array)

        schema = pa.schema([("ree", encoded_array.type)])
        tbl = pa.Table.from_arrays([encoded_array], schema=schema)
        res = duckdb_cursor.sql("select * from tbl").fetchall()
        assert res == expected

    def test_arrow_ree_empty_table(self, duckdb_cursor):
        duckdb_cursor.query("create table tbl (ree integer)")
        rel = duckdb_cursor.table('tbl')
        array = rel.arrow()['ree']
        expected = rel.fetchall()

        encoded_array = pc.run_end_encode(array)

        schema = pa.schema([("ree", encoded_array.type)])
        pa_res = pa.Table.from_arrays([encoded_array], schema=schema)
        res = duckdb_cursor.sql("select * from pa_res").fetchall()
        assert res == expected

    @pytest.mark.parametrize(
        "filter",
        [
            "ree % 2 == 0",
            "ree == 1",
            "ree != 1",
            "ree == 20000",
            "true",
            "ree == 0 and ree == 1",
            "ree % 2 == 1",
            "ree % 2 == 2",
            "ree == 1250",
            "ree == 1249",
            "ree::VARCHAR == 'test'",
        ],
    )
    def test_arrow_run_end_encoding_filters(self, duckdb_cursor, filter):
        duckdb_cursor.query(
            """
            create table tbl as select (i // 8) as ree from range(10000) t(i)
        """
        )
        query = """
            select * from tbl where {}
        """
        query = query.format(filter)
        rel = duckdb_cursor.query(query)

        array = rel.arrow()['ree']
        expected = rel.fetchall()

        encoded_array = pc.run_end_encode(array)

        schema = pa.schema([("ree", encoded_array.type)])
        pa_res = pa.Table.from_arrays([encoded_array], schema=schema)
        res = duckdb_cursor.sql("select * from pa_res").fetchall()
        assert res == expected


# TODO: add tests with a WHERE clause
# TODO: add tests with projections
# TODO: add tests with lists
# TODO: add tests with structs
# TODO: add tests with maps
# TODO: add tests with ENUMs
