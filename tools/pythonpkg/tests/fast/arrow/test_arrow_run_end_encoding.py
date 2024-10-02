import duckdb
import pytest
import pandas as pd
import duckdb

pa = pytest.importorskip("pyarrow", '14.0.0', reason="Needs pyarrow >= 14")
pc = pytest.importorskip("pyarrow.compute")


def create_list_view(offsets, values):
    sizes = []
    for i in range(1, len(offsets)):
        assert offsets[i - 1] <= offsets[i]
        size = offsets[i] - offsets[i - 1]
        sizes.append(size)
    # Cut off the last offset
    offsets = offsets[:-1]
    return pa.ListViewArray.from_arrays(offsets, sizes, values=values)


def create_list(offsets, values):
    return pa.ListArray.from_arrays(offsets, values=values)


def list_constructors():
    result = []
    result.append(create_list)
    if hasattr(pa, 'ListViewArray'):
        result.append(create_list_view)
    return result


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
            ('UUID', "'10acd298-15d7-417c-8b59-eabb5a2bacab'", "'eeccb8c5-9943-b2bb-bb5e-222f4e14b687'"),
            ('BIT', "'01010101010000'", "'01010100010101010101010101111111111'"),
        ],
    )
    @pytest.mark.parametrize(
        "filter",
        [
            # "ree::VARCHAR == '5'",
            "true"
        ],
    )
    def test_arrow_run_end_encoding(self, duckdb_cursor, dbtype, val1, val2, filter):
        if dbtype in ['BIT', 'UUID']:
            pytest.skip("BIT and UUID are currently broken (FIXME)")
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
        query = query.format(val1, dbtype, val2, dbtype, 10000)
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

    @pytest.mark.parametrize('projection', ['*', 'a, c, b', 'ree, a, b, c', 'c, b, a, ree', 'c', 'b, ree, c, a'])
    def test_arrow_ree_projections(self, duckdb_cursor, projection):
        # Create the schema
        duckdb_cursor.query(
            """
            create table tbl (
                ree integer,
                a int,
                b bool,
                c varchar
            )
        """
        )

        # Populate the table with data
        duckdb_cursor.query(
            """
            insert into tbl select
                i // 4,
                i,
                i % 2 == 0,
                i::VARCHAR
            from range(100000) t(i)
        """
        )

        # Fetch the result as an Arrow Table
        result = duckdb_cursor.table('tbl').arrow()

        # Turn 'ree' into a run-end-encoded array and reconstruct a table from it
        arrays = {
            'ree': pc.run_end_encode(result['ree']),
            'a': result['a'],
            'b': result['b'],
            'c': result['c'],
        }

        schema = pa.schema(
            [
                ("ree", arrays['ree'].type),
                ("a", arrays['a'].type),
                ("b", arrays['b'].type),
                ("c", arrays['c'].type),
            ]
        )
        arrow_tbl = pa.Table.from_arrays([arrays['ree'], arrays['a'], arrays['b'], arrays['c']], schema=schema)

        # Verify that the array is run end encoded
        ar_type = arrow_tbl['ree'].type
        assert pa.types.is_run_end_encoded(ar_type) == True

        # Scan the arrow table, making projections that don't cover the entire table
        # This should be pushed down into arrow to only provide us with the necessary columns

        res = duckdb_cursor.query(
            """
            select {} from arrow_tbl
        """.format(
                projection
            )
        ).arrow()

        # Verify correctness by fetching from the original table and the constructed result
        expected = duckdb_cursor.query("select {} from tbl".format(projection)).fetchall()
        actual = duckdb_cursor.query("select {} from res".format(projection)).fetchall()
        assert expected == actual

    @pytest.mark.parametrize('create_list', list_constructors())
    def test_arrow_ree_list(self, duckdb_cursor, create_list):
        size = 1000
        duckdb_cursor.query(
            """
            create table tbl
            as select
                i // 4 as ree,
            FROM range({}) t(i)
        """.format(
                size
            )
        )

        # Populate the table with data
        unstructured = duckdb_cursor.query(
            """
            select * from tbl
        """
        ).arrow()

        columns = unstructured.columns
        # Run-encode the first column ('ree')
        columns[0] = pc.run_end_encode(columns[0])

        # Create a (chunked) ListArray from the chunked arrays (columns) of the ArrowTable
        structured_chunks = []
        for chunk in columns[0].iterchunks():
            ree = chunk
            chunk_length = len(ree)
            offsets = []
            offset = 0
            while chunk_length > 10:
                offsets.append(offset)
                offset += 10
                chunk_length -= 10

            new_array = create_list(offsets, ree)
            structured_chunks.append(new_array)

        structured = pa.chunked_array(structured_chunks)

        arrow_tbl = pa.Table.from_arrays([structured], names=['ree'])
        result = duckdb_cursor.query("select * from arrow_tbl").arrow()
        assert arrow_tbl.to_pylist() == result.to_pylist()

    def test_arrow_ree_struct(self, duckdb_cursor):
        duckdb_cursor.query(
            """
            create table tbl
            as select
                i // 4 as ree,
                i as a,
                i % 2 == 0 as b,
                i::VARCHAR as c
            FROM range(1000) t(i)
        """
        )

        # Populate the table with data
        unstructured = duckdb_cursor.query(
            """
            select * from tbl
        """
        ).arrow()

        columns = unstructured.columns
        # Run-encode the first column ('ree')
        columns[0] = pc.run_end_encode(columns[0])

        # Create a (chunked) StructArray from the chunked arrays (columns) of the ArrowTable
        names = unstructured.column_names
        iterables = [x.iterchunks() for x in columns]
        zipped = zip(*iterables)

        structured_chunks = [pa.StructArray.from_arrays([y for y in x], names=names) for x in zipped]
        structured = pa.chunked_array(structured_chunks)

        arrow_tbl = pa.Table.from_arrays([structured], names=['ree'])
        result = duckdb_cursor.query("select * from arrow_tbl").arrow()

        expected = duckdb_cursor.query("select {'ree': ree, 'a': a, 'b': b, 'c': c} as s from tbl").fetchall()
        actual = duckdb_cursor.query("select * from result").fetchall()

        assert expected == actual

    def test_arrow_ree_union(self, duckdb_cursor):
        size = 1000

        duckdb_cursor.query(
            """
            create table tbl
            as select
                i // 4 as ree,
                i as a,
                i % 2 == 0 as b,
                i::VARCHAR as c
            FROM range({}) t(i)
        """.format(
                size
            )
        )

        # Populate the table with data
        unstructured = duckdb_cursor.query(
            """
            select * from tbl
        """
        ).arrow()

        columns = unstructured.columns
        # Run-encode the first column ('ree')
        columns[0] = pc.run_end_encode(columns[0])

        # Create a (chunked) UnionArray from the chunked arrays (columns) of the ArrowTable
        names = unstructured.column_names
        iterables = [x.iterchunks() for x in columns]
        zipped = zip(*iterables)

        structured_chunks = []
        for chunk in zipped:
            ree, a, b, c = chunk
            chunk_length = len(ree)
            new_array = pa.UnionArray.from_sparse(
                pa.array([i % len(names) for i in range(chunk_length)], type=pa.int8()), [ree, a, b, c]
            )
            structured_chunks.append(new_array)

        structured = pa.chunked_array(structured_chunks)
        arrow_tbl = pa.Table.from_arrays([structured], names=['ree'])
        result = duckdb_cursor.query("select * from arrow_tbl").arrow()

        # Recreate the same result set
        expected = []
        for i in range(size):
            if i % 4 == 0:
                expected.append((i // 4,))
            elif i % 4 == 1:
                expected.append((i,))
            elif i % 4 == 2:
                expected.append((i % 2 == 0,))
            elif i % 4 == 3:
                expected.append((str(i),))
        actual = duckdb_cursor.query("select * from result").fetchall()
        assert expected == actual

    def test_arrow_ree_map(self, duckdb_cursor):
        size = 1000

        duckdb_cursor.query(
            """
            create table tbl
            as select
                i // 4 as ree,
                i as a,
            FROM range({}) t(i)
        """.format(
                size
            )
        )

        # Populate the table with data
        unstructured = duckdb_cursor.query(
            """
            select * from tbl
        """
        ).arrow()

        columns = unstructured.columns
        # Run-encode the first column ('ree')
        columns[0] = pc.run_end_encode(columns[0])

        # Create a (chunked) MapArray from the chunked arrays (columns) of the ArrowTable
        names = unstructured.column_names
        iterables = [x.iterchunks() for x in columns]
        zipped = zip(*iterables)

        structured_chunks = []
        for chunk in zipped:
            ree, a = chunk
            chunk_length = len(ree)
            offsets = []
            offset = 0
            while chunk_length > 10:
                offsets.append(offset)
                offset += 10
                chunk_length -= 10

            new_array = pa.MapArray.from_arrays(offsets, a, ree)
            structured_chunks.append(new_array)

        structured = pa.chunked_array(structured_chunks)
        arrow_tbl = pa.Table.from_arrays([structured], names=['ree'])
        result = duckdb_cursor.query("select * from arrow_tbl").arrow()

        # Verify that the resulting scan is the same as the input
        assert result.to_pylist() == arrow_tbl.to_pylist()

    def test_arrow_ree_dictionary(self, duckdb_cursor):
        size = 1000

        duckdb_cursor.query(
            """
            create table tbl
            as select
                i // 4 as ree,
            FROM range({}) t(i)
        """.format(
                size
            )
        )

        # Populate the table with data
        unstructured = duckdb_cursor.query(
            """
            select * from tbl
        """
        ).arrow()

        columns = unstructured.columns
        # Run-encode the first column ('ree')
        columns[0] = pc.run_end_encode(columns[0])

        # Create a (chunked) MapArray from the chunked arrays (columns) of the ArrowTable
        structured_chunks = []
        for chunk in columns[0].iterchunks():
            ree = chunk
            chunk_length = len(ree)
            offsets = [i for i in reversed(range(chunk_length))]

            new_array = pa.DictionaryArray.from_arrays(indices=offsets, dictionary=ree)
            structured_chunks.append(new_array)

        structured = pa.chunked_array(structured_chunks)
        arrow_tbl = pa.Table.from_arrays([structured], names=['ree'])
        result = duckdb_cursor.query("select * from arrow_tbl").arrow()

        # Verify that the resulting scan is the same as the input
        assert result.to_pylist() == arrow_tbl.to_pylist()
