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
    def test_arrow_run_end_encoding_numerics(self, duckdb_cursor, query, run_length, size, value_type):
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
        ['dbtype', 'val1', 'val2'],
        [
            ('TINYINT', '(-128)', '127'),
            ('SMALLINT', '(-32768)', '32767'),
            ('INTEGER', '(-2147483648)', '2147483647'),
            ('BIGINT', '(-9223372036854775808)', '9223372036854775807'),
            ('UTINYINT', '0', '255'),
            ('USMALLINT', '0', '65535'),
            ('UINTEGER', '0', '4294967295'),
            ('UBIGINT', '0', '18446744073709551615'),
            ('BOOL', 'true', 'false'),
            ('VARCHAR', "'test'", "'this is a long string'"),
            ('BLOB', "'\\xE0\\x9F\\x98\\x84'", "'\\xF0\\x9F\\xA6\\x86'"),
            ('DATE', "'1992-03-27'", "'2204-11-01'"),
            ('TIME', "'01:02:03'", "'23:41:35'"),
            ('TIMESTAMP_S', "'1992-03-22 01:02:03'", "'2022-11-07 08:43:04.123456'"),
            ('TIMESTAMP', "'1992-03-22 01:02:03'", "'2022-11-07 08:43:04.123456'"),
            ('TIMESTAMP_MS', "'1992-03-22 01:02:03'", "'2022-11-07 08:43:04.123456'"),
            ('TIMESTAMP_NS', "'1992-03-22 01:02:03'", "'2022-11-07 08:43:04.123456'"),
            ('DECIMAL(4,2)', "'12.23'", "'99.99'"),
            ('DECIMAL(7,6)', "'1.234234'", "'0.000001'"),
            ('DECIMAL(14,7)', "'134523.234234'", "'999999.000001'"),
            ('DECIMAL(28,1)', "'12345678910111234123456789.1'", "'999999999999999999999999999.9'"),
            # ("'10acd298-15d7-417c-8b59-eabb5a2bacab'::UUID", "'eeccb8c5-9943-b2bb-bb5e-222f4e14b687'::UUID"),
            # ("'01010101010000'::BIT", "'01010100010101010101010101111111111'::BIT"), # FIXME: BIT seems broken?
        ],
    )
    @pytest.mark.parametrize(
        "filter",
        ["ree::VARCHAR == '5'"],
    )
    def test_arrow_run_end_encoding(self, duckdb_cursor, dbtype, val1, val2, filter):
        projection = "a, b, ree"
        query = """
            create table ree_tbl as select
                case when ((i // 8) % 2 == 0)
                    then (
                        case when ((i // 4) % 2 == 0)
                            then {}::{}
                            else {}::{}
                        end
                    ) else
                        NULL
                end as ree,
                i as a,
                i // 5 as b
            from range({}) t(i);
        """
        # Create the table
        query = query.format(val1, dbtype, val2, dbtype, 10000, filter)
        duckdb_cursor.execute(query)

        rel = duckdb_cursor.query("select * from ree_tbl")
        expected = duckdb_cursor.query("select {} from ree_tbl where {}".format(projection, filter)).fetchall()

        # Create an Arrow Table from the table
        arrow_conversion = rel.arrow()
        arrays = {
            'ree': arrow_conversion['ree'],
            'a': arrow_conversion['a'],
            'b': arrow_conversion['b'],
        }

        encoded_arrays = {
            'ree': pc.run_end_encode(arrays['ree']),
            'a': pc.run_end_encode(arrays['a']),
            'b': pc.run_end_encode(arrays['b']),
        }

        schema = pa.schema(
            [
                ("ree", encoded_arrays['ree'].type),
                ("a", encoded_arrays['a'].type),
                ("b", encoded_arrays['b'].type),
            ]
        )
        tbl = pa.Table.from_arrays([encoded_arrays['ree'], encoded_arrays['a'], encoded_arrays['b']], schema=schema)

        # Scan the Arrow Table and verify that the results are the same
        res = duckdb_cursor.sql("select {} from tbl where {}".format(projection, filter)).fetchall()
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

# TODO: add tests with a WHERE clause
# TODO: add tests with projections
# TODO: add tests with lists
# TODO: add tests with structs
# TODO: add tests with maps
# TODO: add tests with ENUMs
